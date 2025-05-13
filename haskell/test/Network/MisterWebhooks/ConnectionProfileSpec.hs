{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE QuasiQuotes       #-}

module Network.MisterWebhooks.ConnectionProfileSpec(spec) where

import           Data.Aeson.Decoding                      (decode, eitherDecode)
import           Data.ByteString                          (fromStrict)
import           Data.Either                              (isLeft)
import           Data.String                              (IsString)
import           Hedgehog
import qualified Hedgehog.Gen                             as Gen
import qualified Hedgehog.Range                           as Range
import           Network.MisterWebhooks.ConnectionProfile (ConnectionProfile (..),
                                                           SASLMechanism (..))
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
    "servers": [
      "b0.mister-webhooks.net.example.com:9092"
    ]
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
    "servers": [
      "b0.mister-webhooks.net.example.com:9092"
    ]
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
                              cpBrokers = ["b0.mister-webhooks.net.example.com:9092"]
                            }

      it "provides an error for broken JSON" $ do
        eitherDecode @ConnectionProfile missingServersBadJSON `shouldSatisfy` isLeft
        eitherDecode @ConnectionProfile badTypeBadJSON        `shouldSatisfy` isLeft
