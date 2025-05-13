{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE QuasiQuotes       #-}
{-# LANGUAGE RecordWildCards   #-}
{-# LANGUAGE StrictData        #-}
{-# LANGUAGE TemplateHaskell   #-}

module Network.MisterWebhooks.MessagePayload where
import qualified Codec.CBOR.JSON
import qualified Codec.CBOR.Read    as CBOR
import           Data.Aeson         (FromJSON)
import qualified Data.Aeson         as Aeson
import qualified Data.Avro          as Avro
import           Data.Avro.Deriving (deriveAvroFromByteString, r)
import           Data.ByteString    (StrictByteString, fromStrict, unpack)
import           Data.Function      ((&))
import           Data.Functor       ((<&>))
import qualified Data.List          as List
import           Data.Map           (Map)
import           Data.Text          (Text)
import qualified Data.Text          as Text
import           Data.Text.Encoding (decodeUtf8)
import           Data.Word          (Word8)
import           Kafka.Consumer     (ConsumerRecord (..), Headers,
                                     headersToList)
import           Text.Printf        (printf)

deriveAvroFromByteString [r|
{
  "type": "record",
  "name": "KafkaMessageEnvelopeV1",
  "namespace": "com.mister_webhooks.data",
  "fields": [
    {
      "name": "method",
      "type": {
        "type": "enum",
        "name": "HTTPMethod",
        "symbols": ["GET", "HEAD", "POST", "PUT", "DELETE", "PATCH"]
      }
    },
    {
      "name": "headers",
      "type": {
        "type": "map",
        "values": {
          "type": "array",
          "items": "string"
        }
      }
    },
    {
      "name": "payload",
      "type": "bytes"
    },
    {
      "name": "encoding",
      "type": {
        "type": "enum",
        "name": "Encoding",
        "symbols": ["JSON", "CBOR"]
      }
    }
  ]
}
|]

data Webhook t = Webhook {
  whMethod  :: HTTPMethod,
  whHeaders :: Map Text [Text],
  whPayload :: t
} deriving (Eq, Show)

validateConsumerRecord :: ConsumerRecord (Maybe StrictByteString) (Maybe StrictByteString) -> Either Text (ConsumerRecord StrictByteString StrictByteString)
validateConsumerRecord ConsumerRecord{..} = do
    key <- check crKey "key is missing"
    val <- check crValue "value is missing"

    return ConsumerRecord{
      crTopic = crTopic,
      crPartition = crPartition,
      crOffset = crOffset,
      crTimestamp = crTimestamp,
      crHeaders = crHeaders,
      crKey = key,
      crValue = val
    }

check :: Maybe a -> String -> Either Text a
check (Just a) _      = Right a
check Nothing message = Left (Text.pack message)

data MessagePayload t = SystemEvent Word8 StrictByteString | WebhookPayload (Webhook t) deriving (Eq, Show)

decodeConsumerRecord :: forall a. FromJSON a => ConsumerRecord StrictByteString StrictByteString -> Either Text (ConsumerRecord StrictByteString (MessagePayload a))
decodeConsumerRecord ConsumerRecord{..} = do
  envelopeTypeHeader <- requireHeader crHeaders "envelope"

  envelopeType <- case Data.ByteString.unpack envelopeTypeHeader of
    [byte]  -> return byte
    unknown -> Left $ Text.pack $ printf "expected 'envelope' header to be single byte, got %s instead" (show unknown)

  if envelopeType < 0x80 then
    return ConsumerRecord{
        crTopic = crTopic,
        crPartition = crPartition,
        crOffset = crOffset,
        crTimestamp = crTimestamp,
        crHeaders = crHeaders,
        crKey = crKey,
        crValue = SystemEvent envelopeType crValue
    }
  else do
    decoded <- case envelopeType of
      0x80 -> decodeKafkaMessageEnvelopeV1 crValue
      unknown -> Left $ Text.pack $ printf "envelope type 0x%2x is unsupported, please upgrade mister-webhooks-client" unknown

    return ConsumerRecord{
        crTopic = crTopic,
        crPartition = crPartition,
        crOffset = crOffset,
        crTimestamp = crTimestamp,
        crHeaders = crHeaders,
        crKey = crKey,
        crValue = WebhookPayload decoded
    }

  where
    decodeKafkaMessageEnvelopeV1 :: StrictByteString -> Either Text (Webhook a)
    decodeKafkaMessageEnvelopeV1 bytes =
      case Avro.decodeValue . fromStrict $ bytes of
        Left errmsg    -> Left (Text.pack errmsg)
        Right KafkaMessageEnvelopeV1{..} -> do
          payload <- case kafkaMessageEnvelopeV1Encoding of
            EncodingCBOR    ->
              case CBOR.deserialiseFromBytes (Codec.CBOR.JSON.decodeValue True) $ fromStrict kafkaMessageEnvelopeV1Payload of
                Left (CBOR.DeserialiseFailure offset message) ->
                  Left (Text.pack $ printf "error deserializing CBOR at %d: %s" offset message)
                Right (_, value) -> case Aeson.fromJSON value of
                  Aeson.Error err -> Left (Text.pack err)
                  Aeson.Success x -> Right x
            EncodingJSON    ->  case Aeson.eitherDecode . fromStrict $ kafkaMessageEnvelopeV1Payload of
                  Left msg -> Left (Text.pack msg)
                  Right x  -> Right x

          return $ Webhook{
            whMethod = kafkaMessageEnvelopeV1Method,
            whHeaders = kafkaMessageEnvelopeV1Headers,
            whPayload = payload
          }

    requireHeader :: Headers -> StrictByteString -> Either Text StrictByteString
    requireHeader headers name =
      case headersToList headers & List.find (\(k, _) -> k == name) <&> snd of
        Nothing -> Left $ Text.pack $ printf "'%s' header is missing" (decodeUtf8 name)
        Just v  -> return v
