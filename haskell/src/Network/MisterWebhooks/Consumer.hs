{-# LANGUAGE LambdaCase      #-}
{-# LANGUAGE RecordWildCards #-}
{-# LANGUAGE StrictData      #-}

module Network.MisterWebhooks.Consumer (
  runWebhookConsumer,
  WebhookHandler(..),
  ConsumerError,
  ConsumerRecord(..),
  TopicName(..),
) where
import           Control.Monad                            (forM, forM_, void)
import           Control.Monad.Catch                      (Exception, MonadMask,
                                                           MonadThrow, bracket,
                                                           throwM)
import           Control.Monad.IO.Class                   (MonadIO)
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
                                                           commitAllOffsets,
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

runWebhookConsumer :: forall m t. (MonadIO m, FromJSON t, MonadMask m) => ConnectionProfile -> (ConsumerProperties -> ConsumerProperties) -> TopicName -> WebhookHandler t -> m ()
runWebhookConsumer profile tweakProperties topicName (WebhookHandler handler) = do
  bracket
    (newConsumer (tweakProperties $ toConsumerProperties profile) (topics [topicName]) >>= \case
      Left kerr -> throwM kerr
      Right consumer -> return consumer
    )
    (void . closeConsumer)
    loop

  where
    throwReceiveError :: Either KafkaError a -> m a
    throwReceiveError (Left kerr) = throwM $ ReceiveError kerr
    throwReceiveError (Right a)   = return a

    loop :: KafkaConsumer -> m ()
    loop consumer = do
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

      loop consumer
