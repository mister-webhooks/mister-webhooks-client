{-# LANGUAGE LambdaCase      #-}
{-# LANGUAGE RecordWildCards #-}
{-# LANGUAGE StrictData      #-}

module Network.MisterWebhooks.Consumer (
  newWebhookConsumer,
  signalConsumer,
  ConsumerCommand(..),
  runWebhookHandler,
  WebhookHandler(..),
  ConsumerError,
  ConsumerRecord(..),
  TopicName(..),
) where
import           Control.Concurrent.STM                   (TQueue, atomically,
                                                           newTQueueIO,
                                                           tryReadTQueue,
                                                           writeTQueue)
import           Control.Monad                            (forM, forM_)
import           Control.Monad.Catch                      (Exception, MonadMask,
                                                           MonadThrow,
                                                           onException, throwM)
import           Control.Monad.IO.Class                   (MonadIO (..))
import           Data.Aeson                               (FromJSON)
import           Data.ByteString                          (StrictByteString)
import           Data.Text                                (Text)
import           Kafka.Consumer                           (BatchSize (BatchSize),
                                                           ConsumerProperties,
                                                           ConsumerRecord (..),
                                                           KafkaConsumer,
                                                           KafkaError, Offset,
                                                           OffsetCommit (OffsetCommit),
                                                           PartitionId,
                                                           Timeout (Timeout),
                                                           TopicName (..),
                                                           closeConsumer,
                                                           commitOffsetMessage,
                                                           newConsumer,
                                                           pollMessageBatch,
                                                           topics)
import           Network.MisterWebhooks.ConnectionProfile (ConnectionProfile (..),
                                                           toConsumerProperties)
import           Network.MisterWebhooks.MessagePayload    (MessagePayload (SystemEvent, WebhookPayload),
                                                           Webhook,
                                                           decodeConsumerRecord,
                                                           validateConsumerRecord)

newtype WebhookHandler t = WebhookHandler (forall io. (MonadIO io, MonadThrow io) => ConsumerRecord StrictByteString (Webhook t) -> io ())

data ConsumerError = ReceiveError KafkaError | DecodeError TopicName PartitionId Offset Text
  deriving Show

instance Exception ConsumerError

data ConsumerCommand = ConsumerCommandShutdown deriving Show
data WebhookConsumer = WebhookConsumer {
  kafkaConsumer :: KafkaConsumer,
  commandQueue  :: TQueue ConsumerCommand
}

newWebhookConsumer :: MonadIO io => ConnectionProfile -> (ConsumerProperties -> ConsumerProperties) -> TopicName -> io (Either KafkaError WebhookConsumer)
newWebhookConsumer profile tweakProperties topicName = do
  newConsumer (tweakProperties $ toConsumerProperties profile) (topics [topicName]) >>= \case
    Left kerr -> return (Left kerr)
    Right kafkaConsumer -> do
      commandQueue  <- liftIO newTQueueIO

      return $ Right WebhookConsumer{..}

signalConsumer :: MonadIO io => WebhookConsumer -> ConsumerCommand -> io ()
signalConsumer WebhookConsumer{..} = liftIO . atomically . writeTQueue commandQueue

runWebhookHandler :: forall io t. (MonadIO io, MonadMask io, FromJSON t) => WebhookConsumer -> WebhookHandler t -> io (Maybe KafkaError)
runWebhookHandler WebhookConsumer{..} (WebhookHandler handler) = do
  liftIO (atomically (tryReadTQueue commandQueue)) >>= \case
    Nothing -> do
      onException (work kafkaConsumer) (closeConsumer kafkaConsumer)
      runWebhookHandler WebhookConsumer{..} (WebhookHandler handler)
    Just ConsumerCommandShutdown ->
      closeConsumer kafkaConsumer

  where
    throwReceiveError :: Either KafkaError a -> io a
    throwReceiveError (Left kerr) = throwM $ ReceiveError kerr
    throwReceiveError (Right a)   = return a

    work :: KafkaConsumer -> io ()
    work consumer = do
      batch <- mapM throwReceiveError =<< pollMessageBatch consumer (Timeout 10_000) (BatchSize 100)

      records <- forM batch $ \msg ->
        case validateConsumerRecord msg >>= decodeConsumerRecord of
          Left err ->
            throwM (DecodeError (crTopic msg) (crPartition msg) (crOffset msg) err)
          Right decoded ->
            return decoded

      forM_ records $ \ConsumerRecord{..} -> do
        case crValue of
          SystemEvent  _ _ -> return ()
          WebhookPayload p -> handler ConsumerRecord{
            crTopic = crTopic,
            crPartition = crPartition,
            crOffset = crOffset,
            crTimestamp = crTimestamp,
            crHeaders = crHeaders,
            crKey = crKey,
            crValue = p
          }

        commitOffsetMessage OffsetCommit consumer ConsumerRecord{..}
