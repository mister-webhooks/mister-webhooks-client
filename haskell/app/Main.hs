{-# LANGUAGE BlockArguments      #-}
{-# LANGUAGE LambdaCase          #-}
{-# LANGUAGE OverloadedLists     #-}
{-# LANGUAGE OverloadedStrings   #-}
{-# LANGUAGE QuasiQuotes         #-}
{-# LANGUAGE RecordWildCards     #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE StrictData          #-}
{-# LANGUAGE TemplateHaskell     #-}

module Main where

import qualified Codec.CBOR.JSON
import qualified Codec.CBOR.Read        as CBOR
import           Control.Monad          (forM, forM_, void)
import           Control.Monad.Catch    (Exception, MonadMask, bracket, throwM)
import           Control.Monad.IO.Class (MonadIO (liftIO))
import           Data.Aeson             (FromJSON)
import qualified Data.Aeson             as Aeson
import qualified Data.Avro              as Avro
import           Data.Avro.Deriving     (deriveAvroFromByteString, r)
import           Data.ByteString        (StrictByteString, unpack)
import           Data.ByteString.Lazy   (fromStrict)
import           Data.Function          ((&))
import           Data.Functor           ((<&>))
import qualified Data.List              as List
import           Data.Map               (Map)
import           Data.Text              (Text)
import qualified Data.Text              as Text
import           Data.Word              (Word8)
import qualified Kafka.Consumer         as KC
import           Text.Printf            (printf)
import           Text.Show.Pretty       (pPrint)


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

main :: IO ()
main = do
  runWebhookConsumer
    connectionProfile
    "incoming.jessekempf.github.mister-kafka"
    (WebhookHandler @Aeson.Value \cr@KC.ConsumerRecord{} -> do
      liftIO $ pPrint cr
    )

  where
    connectionProfile = ConnectionProfile {
      cpConsumerName  = "jessekempf.herpy",
      cpAuthMechanism = SASLPlain,
      cpAuthSecret    = "derpy",
      cpBrokers       = ["localhost:9092"]
    }

data SASLMechanism = SASLPlain deriving Show

data ConnectionProfile = ConnectionProfile {
  cpConsumerName  :: Text,
  cpAuthMechanism :: SASLMechanism,
  cpAuthSecret    :: Text,
  cpBrokers       :: [KC.BrokerAddress]
} deriving Show

toConsumerProperties :: ConnectionProfile -> KC.ConsumerProperties
toConsumerProperties ConnectionProfile{..} =
  KC.brokersList cpBrokers
  <> KC.groupId (KC.ConsumerGroupId cpConsumerName)
  <> KC.extraProp "security.protocol" "SASL_PLAINTEXT"
  <> KC.extraProp "sasl.mechanism" case cpAuthMechanism of
                                      SASLPlain -> "PLAIN"
  <> KC.extraProp "sasl.username" cpConsumerName
  <> KC.extraProp "sasl.password" cpAuthSecret
  <> KC.noAutoCommit
  <> KC.logLevel KC.KafkaLogDebug

newtype WebhookConsumer t = WebhookConsumer KC.KafkaConsumer

data Webhook t = Webhook {
  whMethod  :: HTTPMethod,
  wbHeaders :: Map Text [Text],
  wbPayload :: t
} deriving Show

newtype WebhookHandler t = WebhookHandler (forall io. MonadIO io => KC.ConsumerRecord StrictByteString (Webhook t) -> io ())

validateConsumerRecord :: KC.ConsumerRecord (Maybe StrictByteString) (Maybe StrictByteString) -> Either Text (KC.ConsumerRecord StrictByteString StrictByteString)
validateConsumerRecord KC.ConsumerRecord{..} = do
    key <- check crKey "key is missing"
    val <- check crValue "value is missing"

    return KC.ConsumerRecord{
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

data MessagePayload t = SystemEvent Word8 | WebhookPayload (Webhook t)

decodeConsumerRecord :: forall a. FromJSON a => KC.ConsumerRecord StrictByteString StrictByteString -> Either Text (KC.ConsumerRecord StrictByteString (MessagePayload a))
decodeConsumerRecord KC.ConsumerRecord{..} = do
  envelopeTypeHeader <- requireHeader crHeaders "envelope"

  envelopeType <- case Data.ByteString.unpack envelopeTypeHeader of
    [byte]  -> return byte
    unknown -> Left $ Text.pack $ printf "expected 'envelope' header to be single byte, got %s instead" (show unknown)

  if envelopeType < 0x80 then
    return KC.ConsumerRecord{
        crTopic = crTopic,
        crPartition = crPartition,
        crOffset = crOffset,
        crTimestamp = crTimestamp,
        crHeaders = crHeaders,
        crKey = crKey,
        crValue = SystemEvent envelopeType
    }
  else do
    decoded <- case envelopeType of
      0x80 -> decodeKafkaMessageEnvelopeV1 crValue
      unknown -> Left $ Text.pack $ printf "envelope type 0x%2x is unsupported, please upgrade mister-webhooks-client" unknown

    return KC.ConsumerRecord{
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
            wbHeaders = kafkaMessageEnvelopeV1Headers,
            wbPayload = payload
          }

    requireHeader :: KC.Headers -> StrictByteString -> Either Text StrictByteString
    requireHeader headers name =
      case KC.headersToList headers & List.find (\(k, _) -> k == name) <&> snd of
        Nothing -> Left $ Text.pack $ printf "'%s' header is missing" (show name)
        Just v  -> return v

runWebhookConsumer :: forall m t. (MonadIO m, FromJSON t, MonadMask m) => ConnectionProfile -> KC.TopicName -> WebhookHandler t -> m ()
runWebhookConsumer profile topicName (WebhookHandler handler) = do
  bracket
    (KC.newConsumer (toConsumerProperties profile) (KC.topics [topicName]) >>= \case
      Left kerr -> throwM kerr
      Right consumer -> return consumer
    )
    (void . KC.closeConsumer)
    loop

  where
    throwReceiveError :: Either KC.KafkaError a -> m a
    throwReceiveError (Left kerr) = throwM $ ReceiveError kerr
    throwReceiveError (Right a)   = return a

    loop :: KC.KafkaConsumer -> m ()
    loop consumer = do
      batch <- mapM throwReceiveError =<< KC.pollMessageBatch consumer (KC.Timeout 10_000) (KC.BatchSize 100)

      records <- forM batch $ \msg ->
        case validateConsumerRecord msg >>= decodeConsumerRecord of
          Left err ->
            throwM (DecodeError (KC.crTopic msg) (KC.crPartition msg) (KC.crOffset msg) err)
          Right decoded ->
            return decoded

      forM_ records $ \KC.ConsumerRecord{..} ->
        case crValue of
          SystemEvent _    -> return ()
          WebhookPayload p -> handler KC.ConsumerRecord{
            crTopic = crTopic,
            crPartition = crPartition,
            crOffset = crOffset,
            crTimestamp = crTimestamp,
            crHeaders = crHeaders,
            crKey = crKey,
            crValue = p
          }

      loop consumer

data ConsumerError =
     ReceiveError KC.KafkaError
  |  DecodeError KC.TopicName KC.PartitionId KC.Offset Text
  deriving Show

instance Exception ConsumerError
