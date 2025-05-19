{-# LANGUAGE BlockArguments      #-}
{-# LANGUAGE LambdaCase          #-}
{-# LANGUAGE OverloadedStrings   #-}
{-# LANGUAGE RecordWildCards     #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE StrictData          #-}

module Main where

import           Control.Monad                            (void)
import           Control.Monad.IO.Class                   (MonadIO (liftIO))
import qualified Data.Aeson                               as Aeson
import           Data.Text                                (Text)
import qualified Data.Text                                as Text
import           Kafka.Consumer                           (KafkaLogLevel (..))
import           Network.MisterWebhooks.ConnectionProfile (ConnectionProfileModifier ((#>)))
import           Network.MisterWebhooks.Consumer          (ConsumerCommand (..),
                                                           ConsumerRecord (..),
                                                           TopicName (..),
                                                           WebhookHandler (WebhookHandler),
                                                           newWebhookConsumer,
                                                           runWebhookHandler,
                                                           signalConsumer)
import           Options.Applicative
import           System.Exit                              (exitFailure,
                                                           exitSuccess)
import           System.IO                                (hPrint, stderr)
import           System.Posix                             (Handler (Catch),
                                                           installHandler,
                                                           sigINT)
import           Text.Show.Pretty                         (pPrint)

data Arguments = Arguments {
  topic             :: TopicName,
  connectionProfile :: String
} deriving (Eq, Show)

kafkaTopicReader :: ReadM TopicName
kafkaTopicReader = eitherReader $ \topic -> Right (TopicName $ Text.pack topic)

argumentsParser :: Parser Arguments
argumentsParser =
  Arguments <$> argument kafkaTopicReader (metavar "TOPIC_NAME")
            <*> argument str (metavar "PROFILE_PATH")

main :: IO ()
main = do
  Arguments{..} <- execParser $ info (argumentsParser <**> helper) (
    fullDesc
     <> progDesc "Print to the console messages from TOPIC_NAME using the connection profile at PROFILE_PATH"
     <> header "mister-webhooks-client -- a simple client for Mister Webhooks"
    )

  Aeson.eitherDecodeFileStrict connectionProfile >>= \case
    Left err -> do
      hPrint stderr err
      exitFailure
    Right profile ->
      newWebhookConsumer (profile #> KafkaLogDebug #> ("debug" :: Text, "all" :: Text)) topic >>= \case
        Left err -> do
          hPrint stderr err
          exitFailure

        Right consumer -> do
          void $ installHandler sigINT (Catch $ signalConsumer consumer ConsumerCommandShutdown) Nothing

          mbErr <- runWebhookHandler consumer (
            WebhookHandler @Aeson.Value \cr@ConsumerRecord{} -> do
              liftIO $ pPrint cr
            )

          case mbErr of
            Nothing  ->
              exitSuccess
            Just err -> do
              hPrint stderr err
              exitFailure
