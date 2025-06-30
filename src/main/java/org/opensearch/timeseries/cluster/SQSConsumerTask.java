/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.timeseries.cluster;

import java.io.IOException;
import java.math.BigInteger;
import java.time.Clock;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.concurrent.Semaphore;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.action.FailedNodeException;
import org.opensearch.ad.ADJobProcessor;
import org.opensearch.ad.settings.AnomalyDetectorSettings;
import org.opensearch.client.Request;
import org.opensearch.client.Response;
import org.opensearch.client.ResponseListener;
import org.opensearch.client.RestClient;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.lifecycle.LifecycleListener;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.common.xcontent.LoggingDeprecationHandler;
import org.opensearch.common.xcontent.json.JsonXContent;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.common.ParsingException;
import org.opensearch.core.common.Strings;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.jobscheduler.spi.JobDocVersion;
import org.opensearch.jobscheduler.spi.JobExecutionContext;
import org.opensearch.jobscheduler.spi.LockModel;
import org.opensearch.jobscheduler.spi.utils.LockService;
import org.opensearch.threadpool.Scheduler.Cancellable;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.timeseries.SkipOnLockUnavailable;
import org.opensearch.timeseries.StateManager;
import org.opensearch.timeseries.TimeSeriesAnalyticsPlugin;
import org.opensearch.timeseries.client.RestClientProvider;
import org.opensearch.timeseries.ml.CheckpointDaoInterface;
import org.opensearch.timeseries.constant.CommonName;
import org.opensearch.timeseries.model.Job;
import org.opensearch.timeseries.settings.TimeSeriesSettings;
import org.opensearch.timeseries.sqs.SQSMessageHandler;
import org.opensearch.timeseries.sqs.SQSService;
import org.opensearch.timeseries.util.DateUtils;
import org.opensearch.timeseries.util.DiscoveryNodeSelector;
import org.opensearch.timeseries.util.SDKNodeFilter;

import software.amazon.awssdk.services.sqs.model.Message;
import software.amazon.awssdk.services.sqs.model.MessageSystemAttributeName;
import software.amazon.awssdk.services.sqs.model.ReceiveMessageRequest;

/**
 * SQS Consumer Task that polls SQS for job messages and processes them.
 * Similar to CloudMapWatcherTask but for SQS queue consumption.
 */
public abstract class SQSConsumerTask implements ClusterManagerTask {

    private static final Logger logger = LogManager.getLogger(SQSConsumerTask.class);

    protected ThreadPool pool;
    protected Clock clock;
    protected ClusterService cs;
    protected Settings settings;
    protected TimeValue pollingInterval;
    private Cancellable cron;
    private SQSService sqsService;
    protected SQSMessageHandler messageHandler;
    private Executor messageProcessorExecutor;
    protected Semaphore concurrencySemaphore;
    private final ConcurrentHashMap<String, Semaphore> groupSemaphores = new ConcurrentHashMap<>();
    private static final long MIN_ERROR_BACKOFF_MILLIS = TimeUnit.SECONDS.toMillis(5);
    private static final long MAX_ERROR_BACKOFF_MILLIS = TimeUnit.MINUTES.toMillis(1);
    private static final int MAX_BACKOFF_EXPONENT = 6;
    private static final int EMPTY_RECEIVE_BACKOFF_THRESHOLD = 3;
    private static final long EMPTY_BACKOFF_MIN_MILLIS = 200L;
    private static final long EMPTY_BACKOFF_MAX_MILLIS = 1500L;
    private static final int MAX_PROCESSING_ATTEMPTS = 3; // 1 initial attempt + 2 retries
    private int consecutivePollErrors = 0;
    private long backoffUntilMillis = 0L;
    private int consecutiveEmptyReceives = 0;
    private final AtomicBoolean dlqConfigChecked = new AtomicBoolean(false);
    private volatile String dlqConfigFailureMessage = null;

    protected int maxMessages;
    protected int waitTime;
    protected int visibilityTimeout;
    protected final StateManager nodeStateManager;
    protected final CheckpointDaoInterface<?> checkpointStore;

    protected SQSConsumerTask(StateManager nodeStateManager, CheckpointDaoInterface<?> checkpointStore) {
        this.nodeStateManager = nodeStateManager;
        this.checkpointStore = Objects.requireNonNull(checkpointStore, "checkpointStore must not be null");
    }

    protected void initWithSQSConfig(
        ClusterService cs,
        ThreadPool pool,
        Clock clock,
        DiscoveryNodeSelector f,
        Settings settings,
        TimeValue pollingInterval,
        int maxConcurrentProcessors,
        String threadPoolName,
        SQSService sqsService
    ) {
        List<String> roles = TimeSeriesSettings.NODE_ROLE.get(settings);
        if (!roles.contains(TimeSeriesSettings.COORDINATOR_ROLE)) {
            return;
        }

        logger.info("Initializing SQS Consumer Task");

        this.cs = cs;
        this.clock = clock;
        this.pool = pool;
        this.settings = settings;

        // Initialize SQS service
        this.sqsService = sqsService;

        // Initialize message handler
        this.messageHandler = new DefaultSQSMessageHandler(this.nodeStateManager, groupSemaphores, settings, clock, checkpointStore);

        // Get configuration settings
        this.pollingInterval = pollingInterval;

        // Initialize concurrency controls
        this.concurrencySemaphore = new Semaphore(maxConcurrentProcessors);
        this.messageProcessorExecutor = pool.executor(threadPoolName);

        // Important: register as cluster manager listener
        cs.addLocalNodeClusterManagerListener(this);
    }

    @Override
    public void onClusterManager() {
        logger.info("Node became cluster manager, starting SQS consumer");
        restart();
    }

    @Override
    public void offClusterManager() {
        logger.info("Node stopped being cluster manager, stopping SQS consumer");
        cancel(cron);
        cron = null;
    }

    protected void restart() {
        // Only restart if node has SQS_CONSUMER_ROLE
        List<String> roles = TimeSeriesSettings.NODE_ROLE.get(settings);
        if (!roles.contains(TimeSeriesSettings.COORDINATOR_ROLE)) {
            return;
        }

        cancel(cron);
        cron = pool.scheduleWithFixedDelay(this::pollSQS, pollingInterval, CommonName.SQS_CONSUMER_THREAD_POOL_NAME);

        cs.addLifecycleListener(new LifecycleListener() {
            @Override
            public void beforeStop() {
                logger.info("Cluster is stopping, cleaning up SQS consumer");
                cancel(cron);
                cron = null;
            }
        });
    }

    private void pollSQS() {
        long now = clock.millis();
        // backoff until time is up. This is to avoid polling SQS too frequently after an error or empty receive.
        // We don't Thread.sleep() here because it blocks the scheduler thread and that's a smell in production.
        if (now < backoffUntilMillis) {
            logger.debug("Skipping SQS poll while backing off for {} ms", backoffUntilMillis - now);
            return;
        }

        if (!isReadyForPolling()) {
            return;
        }

        checkDlqConfigurationOnce();

        boolean successfulPoll = false;
        try {
            int availableSlots = concurrencySemaphore.availablePermits();
            if (availableSlots == 0) {
                logger.debug("No available concurrency slots for SQS message processing");
                successfulPoll = true;
                return;
            }

            // Get SQS settings
            int maxMessages = Math.min(availableSlots, this.maxMessages);

            // With WaitTimeSeconds=20, a ReceiveMessage call waits on the server for up to 20s and returns
            // as soon as a message arrives.
            // Long polling gets us lower latency and higher throughput.
            int waitTime = this.waitTime;

            // Create receive message request
            ReceiveMessageRequest request = ReceiveMessageRequest
                .builder()
                .queueUrl(sqsService.getQueueUrl())
                .waitTimeSeconds(waitTime)
                .maxNumberOfMessages(maxMessages)
                .messageSystemAttributeNames(
                    MessageSystemAttributeName.MESSAGE_GROUP_ID,
                    MessageSystemAttributeName.SEQUENCE_NUMBER,
                    MessageSystemAttributeName.APPROXIMATE_RECEIVE_COUNT
                )
                // We return early before all distributed requests are processed because we don't want to block later intervals.
                // Should the ecs task disappears before the inference/cold start completes, the unfinished entity interval is simply
                // retried in a later interval by whichever task inherits that slice of the ring. The code is in RealTimeInferencer.process.
                // RealTimeInferencer also handles out of order samples and retries to ensure idempotent processing by ignoring duplicates.
                .visibilityTimeout(this.visibilityTimeout)
                .build();

            // Poll SQS
            List<Message> messages = sqsService.receiveMessages(request);

            if (messages.isEmpty()) {
                logger.debug("No messages received from SQS");
                consecutiveEmptyReceives++;
                if (consecutiveEmptyReceives >= EMPTY_RECEIVE_BACKOFF_THRESHOLD) {
                    // Backoff with jitter on empties. e.g., after N empty receives, sleep a randomized 200–1500 ms, then resume long
                    // polling.
                    long sleepMillis = ThreadLocalRandom.current().nextLong(EMPTY_BACKOFF_MIN_MILLIS, EMPTY_BACKOFF_MAX_MILLIS + 1);
                    int empties = consecutiveEmptyReceives;
                    consecutiveEmptyReceives = 0;
                    backoffUntilMillis = clock.millis() + sleepMillis;
                    logger.debug("Backing off after {} consecutive empty polls for {}", empties, TimeValue.timeValueMillis(sleepMillis));
                }
                successfulPoll = true;
                return;
            }

            logger.info("Received {} messages from SQS", messages.size());
            consecutiveEmptyReceives = 0;

            // Ordering per config id:
            // - FIFO queue uses MessageGroupId = tenantId + configId, so SQS delivers in order per group.
            // - A single poll can still return multiple messages from the same group; concurrent processing would reorder them.
            // Example: fetch 5 messages, process the 1st, reset visibility for 3, then the 1st finishes and you process the 5th;
            // the older 2nd-4th messages are redelivered later, so processing order is no longer sequential.
            // - We keep only the latest (highest sequence number) per group and delete older ones; their intervals are
            // recovered by gap-filling in RealTimeInferencer.getFeatures() when the latest message is processed.
            // - Per-group concurrency is enforced at execution time via the lock service; in-flight groups are skipped.
            Map<String, Message> latestByGroup = new LinkedHashMap<>();
            List<Message> toDelete = new ArrayList<>();

            for (Message msg : messages) {
                String groupId = msg.attributes().get(MessageSystemAttributeName.MESSAGE_GROUP_ID);
                String seqNum = msg.attributes().get(MessageSystemAttributeName.SEQUENCE_NUMBER);

                logger.debug("Received SQS message: id={}, groupId={}, sequenceNumber={}", msg.messageId(), groupId, seqNum);

                if (groupId == null) {
                    // No group - use message ID as key to process normally
                    latestByGroup.put(msg.messageId(), msg);
                    continue;
                }

                Message existing = latestByGroup.get(groupId);
                if (existing == null) {
                    latestByGroup.put(groupId, msg);
                } else {
                    String existingSeq = existing.attributes().get(MessageSystemAttributeName.SEQUENCE_NUMBER);
                    // Compare sequence numbers (they're numeric strings)
                    if (compareSequenceNumbers(seqNum, existingSeq) > 0) {
                        // Current message is newer - keep it, delete existing
                        toDelete.add(existing);
                        latestByGroup.put(groupId, msg);
                        logger
                            .debug(
                                "Keeping newer message {} (seq={}), will delete older message {} (seq={}) from group {}",
                                msg.messageId(),
                                seqNum,
                                existing.messageId(),
                                existingSeq,
                                groupId
                            );
                    } else {
                        // Existing is newer - delete current
                        toDelete.add(msg);
                        logger
                            .debug(
                                "Keeping existing message {} (seq={}), will delete older message {} (seq={}) from group {}",
                                existing.messageId(),
                                existingSeq,
                                msg.messageId(),
                                seqNum,
                                groupId
                            );
                    }
                }
            }

            // Delete older messages immediately - their data will be fetched via gap-filling
            for (Message msg : toDelete) {
                try {
                    sqsService.deleteMessage(msg);
                    logger
                        .debug(
                            "Deleted older message {} from group {} (gap-filling will fetch its data)",
                            msg.messageId(),
                            msg.attributes().get(MessageSystemAttributeName.MESSAGE_GROUP_ID)
                        );
                } catch (Exception e) {
                    logger.warn("Failed to delete older message {}, it will be redelivered", msg.messageId(), e);
                }
            }

            // Process only the latest messages per group
            List<Message> messagesToProcess = new ArrayList<>(latestByGroup.values());
            logger
                .debug(
                    "After deduplication: {} messages to process (deleted {} older messages)",
                    messagesToProcess.size(),
                    toDelete.size()
                );

            for (Message message : messagesToProcess) {
                final Semaphore semaphore = concurrencySemaphore;
                String groupId = message.attributes().get(MessageSystemAttributeName.MESSAGE_GROUP_ID);
                String sequenceNumber = message.attributes().get(MessageSystemAttributeName.SEQUENCE_NUMBER);

                if (!semaphore.tryAcquire()) {
                    logger.warn("Max concurrency reached, resetting visibility for message {}", message.messageId());
                    sqsService.resetMessageVisibility(message);
                    continue;
                }

                logger.info("Processing SQS message: id={}, groupId={}, sequenceNumber={}", message.messageId(), groupId, sequenceNumber);
                messageProcessorExecutor.execute(() -> {
                    try {
                        processMessage(message);
                    } finally {
                        // semaphore is for the entire queue (by default, 5 messages can be processed concurrently),
                        // while group concurrency is enforced at job execution time via the lock service.
                        semaphore.release();
                    }
                });
            }
            successfulPoll = true;
        } catch (Exception e) {
            consecutivePollErrors++;
            long backoffMillis = computeBackoffMillis();
            backoffUntilMillis = clock.millis() + backoffMillis;
            logger
                .error(
                    "Error polling SQS ({} consecutive failures). Backing off for {}",
                    consecutivePollErrors,
                    TimeValue.timeValueMillis(backoffMillis),
                    e
                );
        } finally {
            if (successfulPoll) {
                consecutivePollErrors = 0;
                if (clock.millis() >= backoffUntilMillis) {
                    backoffUntilMillis = 0L;
                }
            }
        }
    }

    /**
     * Hook for subclasses to block polling until prerequisites are ready.
     */
    protected boolean isReadyForPolling() {
        return true;
    }

    private void checkDlqConfigurationOnce() {
        // fast‑path skips synchronized when the check already ran
        if (dlqConfigChecked.get()) {
            throwIfDlqCheckFailed();
            return;
        }

        synchronized (this) {
            // protects against a race where another thread completed
            // the check between the first read and acquiring the lock
            if (dlqConfigChecked.get()) {
                throwIfDlqCheckFailed();
                return;
            }

            String failureMessage = validateDlqConfiguration();
            if (failureMessage != null) {
                logger.warn(failureMessage);
                dlqConfigFailureMessage = failureMessage;
            }
            dlqConfigChecked.set(true);
            if (failureMessage != null) {
                throw new IllegalStateException(failureMessage);
            }
        }
    }

    private void throwIfDlqCheckFailed() {
        if (dlqConfigFailureMessage != null) {
            throw new IllegalStateException(dlqConfigFailureMessage);
        }
    }

    private String validateDlqConfiguration() {
        if (sqsService == null) {
            return "SQS service is not initialized; cannot validate DLQ configuration";
        }

        String queueUrl = sqsService.getQueueUrl();
        SQSService.RedrivePolicy redrivePolicy = sqsService.getRedrivePolicy();
        if (redrivePolicy == null) {
            return "SQS queue " + queueUrl + " has no redrive policy configured; DLQ is missing";
        }

        String deadLetterTargetArn = redrivePolicy.getDeadLetterTargetArn();
        if (deadLetterTargetArn == null || deadLetterTargetArn.isBlank()) {
            return "SQS queue " + queueUrl + " redrive policy is missing deadLetterTargetArn (DLQ)";
        }

        int maxReceiveCount = redrivePolicy.getMaxReceiveCount();
        if (maxReceiveCount <= 0) {
            return "SQS queue " + queueUrl + " redrive policy has invalid maxReceiveCount '" + maxReceiveCount + "'";
        }
        if (maxReceiveCount != MAX_PROCESSING_ATTEMPTS) {
            return "SQS queue "
                + queueUrl
                + " maxReceiveCount ("
                + maxReceiveCount
                + ") does not match MAX_PROCESSING_ATTEMPTS ("
                + MAX_PROCESSING_ATTEMPTS
                + ")";
        }

        return null;
    }

    private void processMessage(Message message) {
        LockReleaseContext releaseContext = new LockReleaseContext(sqsService, message);
        try {
            logger.debug("Processing SQS message: {}", message.messageId());
            DefaultSQSMessageHandler.setCurrentLockReleaseContext(releaseContext);

            messageHandler.processMessage(message.body(), ActionListener.wrap(success -> {
                if (!success) {
                    handleProcessingFailure(message, releaseContext);
                } else if (!releaseContext.isDeletionPlanned()) {
                    // Deletion was not planned: we never acquired a lock, so there is no later
                    // hook that will delete the message for us. If we simply fall through,
                    // the message keeps its visibility timeout and will eventually become visible again,
                    // letting SQS redeliver it even though we already processed it successfully.
                    // To avoid the duplicate run, we delete it right away.
                    try {
                        releaseContext.deleteImmediately();
                    } catch (RuntimeException ex) {
                        releaseContext.cancelDeletion();
                        releaseContext.resetVisibility();
                        logger.error("Failed to delete SQS message {} after processing", message.messageId(), ex);
                    }
                }
                // ele branch: Deletion was planned: we successfully acquired a lock. In that flow the message must be
                // deleted when the lock is released (the lock release happens a bit later, after task cleanup),
                // so we skip the immediate delete and let releaseContext.deleteIfNeeded() do it.
            }, e -> {
                logger.error("Exception processing SQS message: " + message.messageId(), e);
                handleProcessingFailure(message, releaseContext);
            }));
        } catch (Exception e) {
            logger.error("Exception processing SQS message: " + message.messageId(), e);
            handleProcessingFailure(message, releaseContext);
        } finally {
            // Once the LockService instance is created inside messageHandler.processMessage, it has its own direct reference to
            // the context. It no longer needs or uses the ThreadLocal. So we can clear it here.
            DefaultSQSMessageHandler.clearCurrentLockReleaseContext();
        }
    }

    /**
     * Tracks whether an SQS message should be deleted when the associated lock is released.
     */
    private static class LockReleaseContext {
        private final SQSService sqsService;
        private final Message message;
        private final AtomicBoolean shouldDelete = new AtomicBoolean(false);
        private final AtomicBoolean deleted = new AtomicBoolean(false);

        LockReleaseContext(SQSService sqsService, Message message) {
            this.sqsService = sqsService;
            this.message = message;
        }

        void markLockAcquired() {
            shouldDelete.set(true);
        }

        String getGroupId() {
            return message.attributes().get(MessageSystemAttributeName.MESSAGE_GROUP_ID);
        }

        void cancelDeletion() {
            shouldDelete.set(false);
        }

        boolean isDeletionPlanned() {
            return shouldDelete.get();
        }

        boolean isDeleted() {
            return deleted.get();
        }

        void deleteIfNeeded() {
            if (!shouldDelete.get()) {
                return;
            }
            deleteImmediately();
        }

        void deleteImmediately() {
            if (deleted.compareAndSet(false, true)) {
                try {
                    sqsService.deleteMessage(message);
                } catch (RuntimeException e) {
                    deleted.set(false);
                    throw e;
                } finally {
                    shouldDelete.set(false);
                }
            }
        }

        void resetVisibility() {
            if (deleted.get()) {
                return;
            }
            sqsService.resetMessageVisibility(message);
        }
    }

    private long computeBackoffMillis() {
        long configuredInterval = pollingInterval != null ? pollingInterval.getMillis() : MIN_ERROR_BACKOFF_MILLIS;
        long baseBackoff = Math.max(MIN_ERROR_BACKOFF_MILLIS, configuredInterval);
        int exponent = Math.min(consecutivePollErrors - 1, MAX_BACKOFF_EXPONENT);
        long scaledBackoff = baseBackoff << exponent;
        if (scaledBackoff < 0) {
            return MAX_ERROR_BACKOFF_MILLIS;
        }
        return Math.min(scaledBackoff, MAX_ERROR_BACKOFF_MILLIS);
    }

    /**
     * Compare SQS FIFO sequence numbers.
     * Sequence numbers are 128-bit integers represented as strings.
     *
     * @param seq1 first sequence number
     * @param seq2 second sequence number
     * @return negative if seq1 < seq2, zero if equal, positive if seq1 > seq2
     */
    private int compareSequenceNumbers(String seq1, String seq2) {
        if (seq1 == null && seq2 == null) {
            return 0;
        }
        if (seq1 == null) {
            return -1;
        }
        if (seq2 == null) {
            return 1;
        }
        // SQS sequence numbers are 128-bit integers, use BigInteger for safe comparison
        return new BigInteger(seq1).compareTo(new BigInteger(seq2));
    }

    private void handleProcessingFailure(Message message, LockReleaseContext releaseContext) {
        int receiveCount = getApproximateReceiveCount(message);
        releaseContext.cancelDeletion();
        if (receiveCount >= MAX_PROCESSING_ATTEMPTS) {
            logger.warn("SQS message {} failed {} times.", message.messageId(), receiveCount, MAX_PROCESSING_ATTEMPTS);
        }
        // when max attemps is reached, the message is left for DLQ/redrive.
        releaseContext.resetVisibility();
    }

    private int getApproximateReceiveCount(Message message) {
        String value = message.attributes().get(MessageSystemAttributeName.APPROXIMATE_RECEIVE_COUNT);
        if (value == null) {
            return 1;
        }
        try {
            return Integer.parseInt(value);
        } catch (NumberFormatException e) {
            logger.debug("Invalid ApproximateReceiveCount '{}' for message {}", value, message.messageId());
            return 1;
        }
    }

    private static void cancel(Cancellable c) {
        if (c != null) {
            c.cancel();
        }
    }

    public Cancellable getSQSPollingCron() {
        return cron;
    }

    /**
     * Default implementation of SQSMessageHandler for basic job processing.
     */
    public static class DefaultSQSMessageHandler implements SQSMessageHandler {
        private static final ThreadLocal<LockReleaseContext> CURRENT_LOCK_CONTEXT = new ThreadLocal<>();
        private final StateManager nodeStateManager;
        private final ConcurrentHashMap<String, Semaphore> groupSemaphores;
        private final Settings settings;
        private final Clock clock;
        private final CheckpointDaoInterface<?> checkpointStore;

        public DefaultSQSMessageHandler(
            StateManager nodeStateManager,
            ConcurrentHashMap<String, Semaphore> groupSemaphores,
            Settings settings,
            Clock clock,
            CheckpointDaoInterface<?> checkpointStore
        ) {
            this.nodeStateManager = Objects.requireNonNull(nodeStateManager, "nodeStateManager must not be null");
            this.groupSemaphores = Objects.requireNonNull(groupSemaphores, "groupSemaphores must not be null");
            this.settings = Objects.requireNonNull(settings, "settings must not be null");
            this.clock = Objects.requireNonNull(clock, "clock must not be null");
            this.checkpointStore = Objects.requireNonNull(checkpointStore, "checkpointStore must not be null");
        }

        static void setCurrentLockReleaseContext(LockReleaseContext context) {
            CURRENT_LOCK_CONTEXT.set(context);
        }

        static void clearCurrentLockReleaseContext() {
            CURRENT_LOCK_CONTEXT.remove();
        }

        static LockReleaseContext getCurrentLockReleaseContext() {
            return CURRENT_LOCK_CONTEXT.get();
        }

        protected Job parseJob(String messageBody) throws IOException {
            logger
                .debug(
                    "Parsing job payload, length={}, preview='{}'",
                    messageBody == null ? 0 : messageBody.length(),
                    messageBody == null ? "" : messageBody.substring(0, Math.min(128, messageBody.length()))
                );

            XContentParser parser = JsonXContent.jsonXContent
                .createParser(NamedXContentRegistry.EMPTY, LoggingDeprecationHandler.INSTANCE, messageBody);
            if (parser.nextToken() == null) {
                logger.debug("Parser returned null token for payload: '{}'", messageBody);
                throw new ParsingException(parser.getTokenLocation(), "Empty job payload");
            }
            return Job.parse(parser);
        }

        /**
         * Parse JobExecutionContext from message body.
         * expectedExecutionTime is extracted from messageBody's CommonName.EB_SCHEDULED_TIME_FIELD field.
         * JobDocVersion is created with version (1, 1, 1).
         * LockService is injected by caller.
         *
         * @param messageBody the message body containing job execution information
         * @param jobId the job ID
         * @param lockService the lock service for job execution
         * @return JobExecutionContext instance
         * @throws IOException if parsing fails
         */
        protected JobExecutionContext parseJobExecutionContext(String messageBody, String jobId, LockService lockService)
            throws IOException {
            XContentParser parser = JsonXContent.jsonXContent
                .createParser(NamedXContentRegistry.EMPTY, LoggingDeprecationHandler.INSTANCE, messageBody);

            Instant expectedExecutionTime = null;
            parser.nextToken(); // Move to START_OBJECT
            while (parser.nextToken() != XContentParser.Token.END_OBJECT) {
                String fieldName = parser.currentName();
                parser.nextToken();
                if (CommonName.EB_SCHEDULED_TIME_FIELD.equals(fieldName)) {
                    if (parser.currentToken() == XContentParser.Token.VALUE_STRING) {
                        expectedExecutionTime = Instant.parse(parser.text());
                    } else {
                        logger
                            .error(
                                "Expected " + CommonName.EB_SCHEDULED_TIME_FIELD + " field to be a string, got: " + parser.currentToken()
                            );
                        throw new IOException(
                            "Expected " + CommonName.EB_SCHEDULED_TIME_FIELD + " field to be a string, got: " + parser.currentToken()
                        );
                    }
                } else {
                    parser.skipChildren();
                }
            }

            if (expectedExecutionTime == null) {
                throw new IOException("Missing " + CommonName.EB_SCHEDULED_TIME_FIELD + " field in message body");
            }

            JobDocVersion jobVersion = new JobDocVersion(1, 1, 1);

            // job index name is not used for SQS, so we use an empty string
            return new JobExecutionContext(expectedExecutionTime, jobVersion, lockService, "", jobId);
        }

        @Override
        public void processMessage(String messageBody, ActionListener<Boolean> listener) throws Exception {
            Job job = parseJob(messageBody);
            LockReleaseContext releaseContext = getCurrentLockReleaseContext();
            String groupId = releaseContext != null ? releaseContext.getGroupId() : null;
            LockService lockService = new GroupSemaphoreLockService(releaseContext, groupSemaphores, groupId);
            JobExecutionContext context = parseJobExecutionContext(messageBody, job.getName(), lockService);

            nodeStateManager.getJob(job.getName(), job.getTenantId(), true, ActionListener.wrap(jobOptional -> {
                if (!jobOptional.isPresent() || !jobOptional.get().isEnabled()) {
                    logger.info("Job {} is disabled, deleted, or does not exist. Skipping execution.", job.getName());
                    listener.onResponse(Boolean.TRUE);
                    return;
                }
                executeJob(job, context, listener, messageBody);
            }, e -> {
                logger.warn("Failed to determine latest state for job {}. Proceeding with execution.", job.getName(), e);
                executeJob(job, context, listener, messageBody);
            }));
        }

        @Override
        public String getHandlerName() {
            return "DefaultSQSMessageHandler";
        }

        private void executeJob(Job job, JobExecutionContext context, ActionListener<Boolean> listener, String messageBody) {
            try {
                logger
                    .info(
                        "Successfully deserialized Job object: name={}, enabled={}, analysisType={}",
                        job.getName(),
                        job.isEnabled(),
                        job.getAnalysisType()
                    );
                switch (job.getAnalysisType()) {
                    case AD:
                        logger.info("Processing AD job: {}", job.getName());
                        ADJobProcessor.getInstance().process(job, context, job.getTenantId());
                        break;
                    case HOURLY_MAINTENANCE:
                        logger.info("Processing hourly maintenance job");
                        executeCron(listener);
                        return; // Don't call listener.onResponse below since executeCron handles it
                    case DAILY_S3_CHECKPOINT_CLEANUP:
                        logger.info("Processing daily S3 checkpoint cleanup job");
                        executeCheckpointCleanup(listener);
                        return; // Don't call listener.onResponse below since executeCheckpointCleanup handles it
                    default:
                        throw new IllegalArgumentException("Analysis type is not supported, type: : " + job.getAnalysisType());
                }
                listener.onResponse(Boolean.TRUE);
            } catch (Exception e) {
                logger.error("Error processing AD job message: {}", messageBody, e);
                listener.onFailure(e);
            }
        }

        /**
         * Execute cron maintenance by sending HTTP requests to all eligible data nodes.
         * Each node will execute CronTransportAction to perform local maintenance tasks.
         */
        private void executeCron(ActionListener<Boolean> listener) {
            SDKNodeFilter nodeFilter = new SDKNodeFilter(settings);
            DiscoveryNode[] nodes = nodeFilter.getEligibleDataNodes();
            if (nodes == null || nodes.length == 0) {
                logger.info("No eligible data nodes for hourly cron");
                listener.onResponse(Boolean.TRUE);
                return;
            }

            AtomicInteger pending = new AtomicInteger(nodes.length);
            List<FailedNodeException> failures = Collections.synchronizedList(new ArrayList<>());

            for (DiscoveryNode node : nodes) {
                String endpoint = getNodeEndpoint(node);
                if (endpoint == null) {
                    failures.add(new FailedNodeException(node.getId(), "Missing node endpoint", null));
                    if (pending.decrementAndGet() == 0) {
                        logCronResults(failures);
                        listener.onResponse(Boolean.TRUE);
                    }
                    continue;
                }

                RestClient restClient = RestClientProvider.getRestClient(endpoint);
                Request request = new Request("POST", TimeSeriesAnalyticsPlugin.TIMESERIES_BASE_URI + "/_cron");

                restClient.performRequestAsync(request, new ResponseListener() {
                    @Override
                    public void onSuccess(Response response) {
                        logger.debug("Successfully executed cron on node {}", node.getId());
                        if (pending.decrementAndGet() == 0) {
                            logCronResults(failures);
                            listener.onResponse(Boolean.TRUE);
                        }
                    }

                    @Override
                    public void onFailure(Exception e) {
                        logger.warn("Failed to execute cron on node {}", node.getId(), e);
                        failures.add(new FailedNodeException(node.getId(), "Cron execution failed", e));
                        if (pending.decrementAndGet() == 0) {
                            logCronResults(failures);
                            listener.onResponse(Boolean.TRUE);
                        }
                    }
                });
            }
        }

        private String getNodeEndpoint(DiscoveryNode node) {
            if (node == null || node.getAddress() == null) {
                return null;
            }
            String address = node.getAddress().toString();
            if (Strings.isNullOrEmpty(address)) {
                return null;
            }
            if (address.startsWith("/")) {
                address = address.substring(1);
            }
            int lastSlash = address.lastIndexOf('/');
            if (lastSlash >= 0) {
                address = address.substring(lastSlash + 1);
            }
            return address;
        }

        private void logCronResults(List<FailedNodeException> failures) {
            if (failures.isEmpty()) {
                logger.info("Hourly cron completed successfully on all nodes");
            } else {
                logger
                    .warn(
                        "Hourly cron completed with {} failures: {}",
                        failures.size(),
                        failures.stream().map(FailedNodeException::nodeId).collect(Collectors.joining(", "))
                    );
            }
        }

        /**
         * Execute checkpoint cleanup using the store implementation selected by
         * {@code CHECKPOINT_STORE_FACTORY_CLASS}.
         */
        private void executeCheckpointCleanup(ActionListener<Boolean> listener) {
            try {
                checkpointStore.createRetentionTask(DateUtils.toDuration(AnomalyDetectorSettings.AD_CHECKPOINT_TTL.get(settings)), clock).run();
                logger.info("Checkpoint cleanup completed");
                listener.onResponse(Boolean.TRUE);
            } catch (Exception e) {
                logger.error("Checkpoint cleanup failed", e);
                listener.onFailure(e);
            }
        }

        /**
         * LockService backed by per-group semaphores. If a group is already in-flight, lock acquisition
         * returns null so the caller can skip without error.
         */
        private static class GroupSemaphoreLockService implements LockService, SkipOnLockUnavailable {
            private final LockReleaseContext releaseContext;
            private final ConcurrentHashMap<String, Semaphore> groupSemaphores;
            private final String groupId;
            private final AtomicBoolean lockAcquired = new AtomicBoolean(false);
            private final AtomicBoolean groupSemaphoreAcquired = new AtomicBoolean(false);
            private volatile Semaphore groupSemaphore;

            private GroupSemaphoreLockService(
                LockReleaseContext releaseContext,
                ConcurrentHashMap<String, Semaphore> groupSemaphores,
                String groupId
            ) {
                this.releaseContext = releaseContext;
                this.groupSemaphores = groupSemaphores;
                this.groupId = groupId;
            }

            private boolean tryAcquireGroupSemaphore() {
                if (groupId == null) {
                    return true;
                }
                if (groupSemaphoreAcquired.get()) {
                    return true;
                }
                Semaphore candidate = groupSemaphores.computeIfAbsent(groupId, key -> new Semaphore(1));
                if (!candidate.tryAcquire()) {
                    return false;
                }
                groupSemaphore = candidate;
                groupSemaphoreAcquired.set(true);
                return true;
            }

            private void releaseGroupSemaphore() {
                if (groupSemaphoreAcquired.compareAndSet(true, false) && groupSemaphore != null) {
                    groupSemaphore.release();
                }
            }

            /**
             * Acquires a lock for the job.
             * <p>
             * Group semaphore ensures only one message per group is processed concurrently. If the semaphore
             * is unavailable, the message is skipped (no retry).
             * <p>
             * Race condition: a newer message (e.g., M5) can be skipped while a previous one is in-flight and then
             * deleted by the processing logic; the next poll may return M6, so M5 is never explicitly processed.
             * This is acceptable because gap-filling guarantees data completeness: when M6 is processed,
             * RealTimeInferencer detects the gap and getFeatures() fetches all missing intervals, including M5's range.
             * Assumption: a future message arrives (e.g., scheduled job) to trigger gap-filling. If no future messages
             * arrive (e.g., detector stopped), the last skipped interval is not fetched, which is acceptable for stopped detectors.
             *
             * @param jobParameter the job parameter
             * @param context the job execution context
             * @param listener the listener to call when the lock is acquired
             */
            @Override
            public void acquireLock(
                final org.opensearch.jobscheduler.spi.ScheduledJobParameter jobParameter,
                final JobExecutionContext context,
                ActionListener<LockModel> listener
            ) {
                if (!tryAcquireGroupSemaphore()) {
                    listener.onResponse(null);
                    return;
                }
                // Create a dummy lock model once we own the group semaphore.
                LockModel lockModel = new LockModel(context.getJobIndexName(), context.getJobId(), Instant.now(), 3600L, false); // 1 hour
                                                                                                                                 // duration
                lockAcquired.set(true);
                if (releaseContext != null) {
                    releaseContext.markLockAcquired();
                }
                listener.onResponse(lockModel);
            }

            @Override
            public void acquireLockWithId(
                final String jobIndexName,
                final Long lockDurationSeconds,
                final String lockId,
                ActionListener<LockModel> listener
            ) {
                if (!tryAcquireGroupSemaphore()) {
                    listener.onResponse(null);
                    return;
                }
                // Create a dummy lock model once we own the group semaphore.
                LockModel lockModel = new LockModel(
                    jobIndexName,
                    lockId,
                    Instant.now(),
                    lockDurationSeconds != null ? lockDurationSeconds : 3600L,
                    false
                );
                lockAcquired.set(true);
                if (releaseContext != null) {
                    releaseContext.markLockAcquired();
                }
                listener.onResponse(lockModel);
            }

            @Override
            public void findLock(final String lockId, ActionListener<LockModel> listener) {
                // Return null to indicate lock not found
                listener.onResponse(new LockModel(null, lockId, Instant.now(), 3600L, false));
            }

            @Override
            public void release(final LockModel lock, ActionListener<Boolean> listener) {
                try {
                    if (lockAcquired.get() && releaseContext != null) {
                        releaseContext.deleteIfNeeded();
                    }
                    listener.onResponse(Boolean.TRUE);
                } catch (Exception e) {
                    if (releaseContext != null) {
                        releaseContext.cancelDeletion();
                        releaseContext.resetVisibility();
                    }
                    listener.onFailure(e);
                } finally {
                    releaseGroupSemaphore();
                }
            }

            @Override
            public void deleteLock(final String lockId, ActionListener<Boolean> listener) {
                // Always succeed
                listener.onResponse(true);
            }

            @Override
            public void renewLock(final LockModel lock, ActionListener<LockModel> listener) {
                // Always succeed - return the same lock
                listener.onResponse(lock);
            }
        }
    }
}
