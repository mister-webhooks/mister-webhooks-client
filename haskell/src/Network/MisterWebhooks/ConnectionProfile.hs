{-# LANGUAGE BlockArguments    #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE QuasiQuotes       #-}
{-# LANGUAGE RecordWildCards   #-}
{-# LANGUAGE StrictData        #-}

module Network.MisterWebhooks.ConnectionProfile where
import           Data.Aeson        (FromJSON (..), withObject, withText, (.:))
import           Data.Map          (Map)
import qualified Data.Map          as Map
import           Data.Maybe        (fromMaybe)
import           Data.Text         (Text)
import qualified Data.Text         as Text
import           Kafka.Consumer    (BrokerAddress (..),
                                    ConsumerGroupId (ConsumerGroupId),
                                    ConsumerProperties,
                                    KafkaLogLevel (KafkaLogInfo), brokersList,
                                    extraProp, groupId, logLevel, noAutoCommit)
import           Text.Printf       (printf)
import           Text.RawString.QQ (r)

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
  cpBootstrap     :: BrokerAddress,
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
      <*> (BrokerAddress <$> kafka .: "bootstrap")
      <*> pure Nothing
      <*> pure Map.empty

toConsumerProperties :: ConnectionProfile -> ConsumerProperties
toConsumerProperties ConnectionProfile{..} =
  brokersList [cpBootstrap]
  <> groupId (ConsumerGroupId cpConsumerName)
  <> extraProp "security.protocol" "SSL"
  <> extraProp "sasl.mechanism" case cpAuthMechanism of
                                      SASLPlain -> "PLAIN"
  <> extraProp "sasl.username" cpConsumerName
  <> extraProp "sasl.password" cpAuthSecret
  <> noAutoCommit
  <> logLevel (fromMaybe KafkaLogInfo cpLogLevel)
  <> foldr ((<>) . uncurry extraProp) mempty (Map.toList cpProperties)
  <> extraProp "ssl.ca.pem" [r|
-----BEGIN CERTIFICATE-----
MIICuDCCAmqgAwIBAgIURKmZE5o9LPqEQpU6yahiP+TLwpAwBQYDK2VwMIHHMQsw
CQYDVQQGEwJVUzETMBEGA1UECAwKQ2FsaWZvcm5pYTEWMBQGA1UEBwwNU2FuIEZy
YW5jaXNjbzEYMBYGA1UECgwPTWlzdGVyIFdlYmhvb2tzMRQwEgYDVQQLDAtFbmdp
bmVlcmluZzErMCkGA1UEAwwiS2Fma2EgQnJva2VyIENlcnRpZmljYXRlIEF1dGhv
cml0eTEuMCwGCSqGSIb3DQEJARYfZW5naW5lZXJpbmdAbWlzdGVyLXdlYmhvb2tz
LmNvbTAeFw0yNTA1MjIwNDA0NTZaFw0zNTA1MjAwNDA0NTZaMIHHMQswCQYDVQQG
EwJVUzETMBEGA1UECAwKQ2FsaWZvcm5pYTEWMBQGA1UEBwwNU2FuIEZyYW5jaXNj
bzEYMBYGA1UECgwPTWlzdGVyIFdlYmhvb2tzMRQwEgYDVQQLDAtFbmdpbmVlcmlu
ZzErMCkGA1UEAwwiS2Fma2EgQnJva2VyIENlcnRpZmljYXRlIEF1dGhvcml0eTEu
MCwGCSqGSIb3DQEJARYfZW5naW5lZXJpbmdAbWlzdGVyLXdlYmhvb2tzLmNvbTAq
MAUGAytlcAMhAE4/M7Qj1+KNtqGdGF7DgAtO+elzPGDHlyCLz1VCvwi+o2YwZDAd
BgNVHQ4EFgQUVVOr9w+0L3obSHwAx/3DKG+iKOMwHwYDVR0jBBgwFoAUVVOr9w+0
L3obSHwAx/3DKG+iKOMwEgYDVR0TAQH/BAgwBgEB/wIBATAOBgNVHQ8BAf8EBAMC
AQYwBQYDK2VwA0EAZlSOhxGZrIK/gUwB6tOKK3S0gvD7a+SoEEkAYVF44AnwvMe0
5qzICSe+0sFaqLT0CNf2JQo/PSK06e9Lb7zNCw==
-----END CERTIFICATE-----
  |]
