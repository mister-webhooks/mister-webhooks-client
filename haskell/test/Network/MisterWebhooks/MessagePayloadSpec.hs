{-# LANGUAGE OverloadedStrings #-}

module Network.MisterWebhooks.MessagePayloadSpec (spec) where
import           Data.Aeson                            (Value)
import qualified Data.Aeson                            as Aeson
import qualified Data.Aeson.KeyMap                     as KeyMap
import qualified Data.Avro                             as Avro
import           Data.ByteString                       (toStrict)
import qualified Data.ByteString                       as ByteString
import qualified Data.Map                              as Map
import qualified Data.Text                             as Text
import           Hedgehog
import qualified Hedgehog.Gen                          as Gen
import qualified Hedgehog.Range                        as Range
import           Kafka.Consumer                        (ConsumerRecord (..),
                                                        headersFromList)
import qualified Kafka.Consumer                        as Kafka
import           Network.MisterWebhooks.MessagePayload (Encoding (..),
                                                        HTTPMethod (..),
                                                        KafkaMessageEnvelopeV1 (..),
                                                        MessagePayload (..),
                                                        Webhook (..),
                                                        decodeConsumerRecord,
                                                        validateConsumerRecord)
import           Test.Hspec
import           Test.Hspec.Hedgehog
import qualified Text.Printf                           as Text

spec :: Spec
spec = do
  describe "validateConsumerRecord" $ do
    it "rejects missing keys" $
      validateConsumerRecord ConsumerRecord {
        crTopic = "test",
        crPartition = Kafka.PartitionId 3,
        crOffset = Kafka.Offset 7,
        crTimestamp = Kafka.CreateTime 12,
        crHeaders = headersFromList [],
        crKey = Nothing,
        crValue = Just "foobar"
      } `shouldBe` Left "key is missing"

    it "rejects missing values" $
      validateConsumerRecord ConsumerRecord {
        crTopic = "test",
        crPartition = Kafka.PartitionId 3,
        crOffset = Kafka.Offset 7,
        crTimestamp = Kafka.CreateTime 12,
        crHeaders = headersFromList [],
        crKey = Just "foobar",
        crValue = Nothing
      } `shouldBe` Left "value is missing"

  describe "decodeConsumerRecord" $ do
    it "requires the `envelope` header" $
      decodeConsumerRecord @Value ConsumerRecord {
        crTopic = "test",
        crPartition = Kafka.PartitionId 3,
        crOffset = Kafka.Offset 7,
        crTimestamp = Kafka.CreateTime 12,
        crHeaders = headersFromList [],
        crKey = "foobar",
        crValue = "baz"
      } `shouldBe` Left "'envelope' header is missing"

    it "requires the `envelope` header to contain a single byte" $
      decodeConsumerRecord @Value ConsumerRecord {
        crTopic = "test",
        crPartition = Kafka.PartitionId 3,
        crOffset = Kafka.Offset 7,
        crTimestamp = Kafka.CreateTime 12,
        crHeaders = headersFromList [("envelope", ByteString.pack [0x01, 0x02])],
        crKey = "foobar",
        crValue = "baz"
      } `shouldBe` Left "expected 'envelope' header to be single byte, got [1,2] instead"

    it "treats envelopeType in the 0x00 to 0x7F range as system events" $ hedgehog $ do
      envelopeType <- forAll $ Gen.filter (< 0x80) $ Gen.word8 (Range.linear 0 100)

      decodeConsumerRecord @Value ConsumerRecord {
        crTopic = "test",
        crPartition = Kafka.PartitionId 3,
        crOffset = Kafka.Offset 7,
        crTimestamp = Kafka.CreateTime 12,
        crHeaders = headersFromList [("envelope", ByteString.pack [envelopeType])],
        crKey = "foobar",
        crValue = "baz"
      } === Right ConsumerRecord {
        crTopic = "test",
        crPartition = Kafka.PartitionId 3,
        crOffset = Kafka.Offset 7,
        crTimestamp = Kafka.CreateTime 12,
        crHeaders = headersFromList [("envelope", ByteString.pack [envelopeType])],
        crKey = "foobar",
        crValue = SystemEvent envelopeType "baz"
      }

    it "treats envelopeType 0x80 as KafkaMessageEnvelopeV1 Avro" $ do
      let payload = Avro.encodeValue KafkaMessageEnvelopeV1 {
        kafkaMessageEnvelopeV1Headers = Map.fromList [("hello", ["there", "everywhere"])],
        kafkaMessageEnvelopeV1Method = HTTPMethodDELETE,
        kafkaMessageEnvelopeV1Payload = "{\"hello\":\"world\"}",
        kafkaMessageEnvelopeV1Encoding = EncodingJSON
      }

      decodeConsumerRecord @Value ConsumerRecord {
        crTopic = "test",
        crPartition = Kafka.PartitionId 3,
        crOffset = Kafka.Offset 7,
        crTimestamp = Kafka.CreateTime 12,
        crHeaders = headersFromList [("envelope", ByteString.pack [0x80])],
        crKey = "foobar",
        crValue = toStrict payload
      } `shouldBe` Right ConsumerRecord {
        crTopic = "test",
        crPartition = Kafka.PartitionId 3,
        crOffset = Kafka.Offset 7,
        crTimestamp = Kafka.CreateTime 12,
        crHeaders = headersFromList [("envelope", ByteString.pack [0x80])],
        crKey = "foobar",
        crValue = WebhookPayload (Webhook {
          whMethod = HTTPMethodDELETE,
          whHeaders = Map.fromList [("hello", ["there", "everywhere"])],
          whPayload = Aeson.Object $ KeyMap.fromList [("hello", "world")]
        })
      }

    it "rejects envelope type > 0x80 with an upgrade message" $ hedgehog $ do
      envelopeType <- forAll $ Gen.filter (> 0x80) $ Gen.word8 (Range.linear 0 255)

      decodeConsumerRecord @Value ConsumerRecord {
        crTopic = "test",
        crPartition = Kafka.PartitionId 3,
        crOffset = Kafka.Offset 7,
        crTimestamp = Kafka.CreateTime 12,
        crHeaders = headersFromList [("envelope", ByteString.pack [envelopeType])],
        crKey = "foobar",
        crValue = "baz"
      } === Left (Text.pack $ Text.printf "envelope type 0x%2x is unsupported, please upgrade mister-webhooks-client" envelopeType)
