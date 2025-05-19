{-# LANGUAGE BlockArguments    #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RecordWildCards   #-}
{-# LANGUAGE StrictData        #-}

module Network.MisterWebhooks.ConnectionProfile where
import           Data.Aeson     (FromJSON (..), withObject, withText, (.:))
import           Data.Map       (Map)
import qualified Data.Map       as Map
import           Data.Maybe     (fromMaybe)
import           Data.String    (IsString (fromString))
import           Data.Text      (Text)
import qualified Data.Text      as Text
import           Kafka.Consumer (BrokerAddress (..),
                                 ConsumerGroupId (ConsumerGroupId),
                                 ConsumerProperties,
                                 KafkaLogLevel (KafkaLogDebug, KafkaLogInfo),
                                 brokersList, extraProp, groupId, logLevel,
                                 noAutoCommit)
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
  cpBrokers       :: [BrokerAddress],
  cpLogLevel      :: Maybe KafkaLogLevel,
  cpProperties    :: Map Text Text
} deriving (Eq, Show)

class ConnectionProfileModifier a where
  (#>) :: ConnectionProfile -> a -> ConnectionProfile

instance ConnectionProfileModifier KafkaLogLevel where
  ConnectionProfile{..} #> level =
    ConnectionProfile{
      cpLogLevel = Just level,
      ..
    }

instance ConnectionProfileModifier (String, String) where
  ConnectionProfile{..} #> (k, v) =
    ConnectionProfile{
      cpProperties = Map.insert (Text.pack k) (Text.pack v) cpProperties,
      ..
    }

instance FromJSON ConnectionProfile where
  parseJSON = withObject "Connection Profile" $ \v -> do
    auth <- v .: "auth"
    kafka <- v .: "kafka"

    ConnectionProfile
      <$> v .: "consumer_name"
      <*> (auth .: "mechanism")
      <*> (auth .: "secret")
      <*> (map BrokerAddress <$> kafka .: "servers")
      <*> pure Nothing
      <*> pure Map.empty

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
  <> logLevel (fromMaybe KafkaLogInfo cpLogLevel)
  <> foldr ((<>) . uncurry extraProp) mempty (Map.toList cpProperties)
