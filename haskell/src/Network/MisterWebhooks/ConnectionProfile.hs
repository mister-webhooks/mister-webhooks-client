{-# LANGUAGE BlockArguments    #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RecordWildCards   #-}

module Network.MisterWebhooks.ConnectionProfile where
import           Data.Aeson     (FromJSON (..), withObject, withText, (.:))
import           Data.Text      (Text)
import qualified Data.Text      as Text
import           Kafka.Consumer (BrokerAddress (..),
                                 ConsumerGroupId (ConsumerGroupId),
                                 ConsumerProperties,
                                 KafkaLogLevel (KafkaLogDebug), brokersList,
                                 extraProp, groupId, logLevel, noAutoCommit)
import           Text.Printf    (printf)

data SASLMechanism = SASLPlain deriving (Eq, Show)

instance FromJSON SASLMechanism where
  parseJSON = withText "SASL Mechanism" $ \t ->
                case Text.toUpper t of
                  "PLAIN" -> pure SASLPlain
                  other   -> fail $ printf "'%s' is not a recognized mechanism" other

data ConnectionProfile = ConnectionProfile {
  cpConsumerName  :: Text,
  cpAuthMechanism :: SASLMechanism,
  cpAuthSecret    :: Text,
  cpBrokers       :: [BrokerAddress]
} deriving (Eq, Show)

instance FromJSON ConnectionProfile where
  parseJSON = withObject "Connection Profile" $ \v -> do
    auth <- v .: "auth"
    kafka <- v .: "kafka"

    ConnectionProfile
      <$> v .: "consumer_name"
      <*> (auth .: "mechanism")
      <*> (auth .: "secret")
      <*> (map BrokerAddress <$> kafka .: "servers")

toConsumerProperties :: ConnectionProfile -> ConsumerProperties
toConsumerProperties ConnectionProfile{..} =
  brokersList cpBrokers
  <> groupId (ConsumerGroupId cpConsumerName)
  <> extraProp "security.protocol" "SASL_PLAINTEXT"
  <> extraProp "sasl.mechanism" case cpAuthMechanism of
                                      SASLPlain -> "PLAIN"
  <> extraProp "sasl.username" cpConsumerName
  <> extraProp "sasl.password" cpAuthSecret
  <> noAutoCommit
  <> logLevel KafkaLogDebug
