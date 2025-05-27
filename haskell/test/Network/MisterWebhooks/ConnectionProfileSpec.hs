{-# LANGUAGE LambdaCase        #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE QuasiQuotes       #-}

module Network.MisterWebhooks.ConnectionProfileSpec(spec) where

import           Data.Aeson.Decoding                      (decode, eitherDecode)
import           Data.ByteString                          (fromStrict)
import           Data.Either                              (isLeft)
import           Data.Function                            ((&))
import qualified Data.Map                                 as Map
import           Data.String                              (IsString)
import           Data.Text                                (Text)
import           Hedgehog
import qualified Hedgehog.Gen                             as Gen
import qualified Hedgehog.Range                           as Range
import           Kafka.Consumer                           (KafkaLogLevel (..))
import qualified Kafka.Consumer                           as ConsumerProperties
import           Network.MisterWebhooks.ConnectionProfile (ConnectionProfile (..),
                                                           SASLMechanism (..),
                                                           toConsumerProperties,
                                                           (#>))
import           Test.Hspec
import           Test.Hspec.Hedgehog                      (hedgehog)
import           Text.RawString.QQ                        (r)


goodJSON :: IsString a => a
goodJSON = [r|{
  "consumer_name": "myproject.foobar",
  "auth": {
    "mechanism": "plain",
    "secret": "12345-abcde"
  },
  "kafka": {
    "bootstrap": "b0.mister-webhooks.net.example.com:9092"
  }
}|]

missingServersBadJSON :: IsString a => a
missingServersBadJSON = [r|{
  "consumer_name": "myproject.foobar",
  "auth": {
    "mechanism": "plain",
    "secret": "12345-abcde"
  },
  "kafka": {
  }
}|]

badTypeBadJSON :: IsString a => a
badTypeBadJSON = [r|{
  "consumer_name": "myproject.foobar",
  "auth": {
    "mechanism": "plain",
    "secret": 123
  },
  "kafka": {
    "bootstrap": "b0.mister-webhooks.net.example.com:9092"
  }
}|]

spec :: Spec
spec = do
  describe "SASLMechanism" $ do
    describe "FromJSON" $ do
      it "parses PLAIN" $ do
        eitherDecode "\"PLAIN\"" `shouldBe` Right SASLPlain
        eitherDecode "\"pLaIn\"" `shouldBe` Right SASLPlain

      it "rejects everything else" $ hedgehog $ do
        str <- forAll $ Gen.utf8 (Range.linear 0 100) Gen.ascii
        decode ("\"" <> fromStrict str <> "\"") === Nothing @SASLMechanism

  describe "ConnectionProfile" $ do
    describe "FromJSON" $ do
      it "parses from correct JSON" $ do
        decode goodJSON `shouldBe` Just ConnectionProfile {
                              cpConsumerName = "myproject.foobar",
                              cpAuthMechanism = SASLPlain,
                              cpAuthSecret = "12345-abcde",
                              cpBootstrap = "b0.mister-webhooks.net.example.com:9092",
                              cpLogLevel = Nothing,
                              cpProperties = Map.empty
                            }

      it "provides an error for broken JSON" $ do
        eitherDecode @ConnectionProfile missingServersBadJSON `shouldSatisfy` isLeft
        eitherDecode @ConnectionProfile badTypeBadJSON        `shouldSatisfy` isLeft

    describe "#>" $ do
      let decoded = eitherDecode @ConnectionProfile goodJSON

      case decoded of
        Left err      -> do
          it "requires decodable JSON" $ do
            expectationFailure err
        Right profile -> do
          it "defaults to logging at INFO" $ do
            (profile & toConsumerProperties & ConsumerProperties.cpLogLevel) `shouldBe` Just KafkaLogInfo
          it "sets the logging level" $ do
            ((profile #> KafkaLogErr) & toConsumerProperties & ConsumerProperties.cpLogLevel) `shouldBe` Just KafkaLogErr
          it "allows setting arbitrary parameters" $ do
            ((profile & toConsumerProperties & ConsumerProperties.cpProps) Map.!? ("debug" :: Text)) `shouldBe` Nothing
            ((profile #> ("debug" :: String, "all" :: String) & toConsumerProperties & ConsumerProperties.cpProps) Map.!? ("debug" :: Text)) `shouldBe` Just "all"
