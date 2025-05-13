{-# LANGUAGE BlockArguments      #-}
{-# LANGUAGE LambdaCase          #-}
{-# LANGUAGE OverloadedStrings   #-}
{-# LANGUAGE RecordWildCards     #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE StrictData          #-}

module Main where

import           Control.Monad.IO.Class          (MonadIO (liftIO))
import qualified Data.Aeson                      as Aeson
import qualified Data.Text                       as Text
import           Network.MisterWebhooks.Consumer (ConsumerRecord (..),
                                                  TopicName (..),
                                                  WebhookHandler (WebhookHandler),
                                                  runWebhookConsumer)
import           Options.Applicative
import           System.Exit                     (exitFailure)
import           System.IO                       (hPrint, stderr)
import           Text.Show.Pretty                (pPrint)

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
    Right profile -> do
      runWebhookConsumer
        profile
        id
        topic
        (WebhookHandler @Aeson.Value \cr@ConsumerRecord{} -> do
          liftIO $ pPrint cr
        )
