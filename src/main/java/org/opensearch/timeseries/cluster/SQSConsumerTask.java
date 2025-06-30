/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.timeseries.cluster;

import java.io.IOException;
import java.math.BigInteger;
import java.time.Clock;
import java.time.Duration;
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
import java.util.function.Supplier;
import java.util.stream.Collectors;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.action.FailedNodeException;
import org.opensearch.ad.ADJobProcessor;
import org.opensearch.ad.settings.AnomalyDetectorSettings;
import org.opensearch.client.Request;
import org.opensearch.client.RequestOptions;
import org.opensearch.client.Response;
import org.opensearch.client.ResponseListener;
import org.opensearch.client.RestClient;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.lifecycle.LifecycleListener;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.common.util.concurrent.ThreadContext;
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
import org.opensearch.jobscheduler.spi.ScheduledJobParameter;
import org.opensearch.jobscheduler.spi.utils.LockService;
import org.opensearch.threadpool.Scheduler.Cancellable;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.timeseries.SkipOnLockUnavailable;
import org.opensearch.timeseries.StateManager;
import org.opensearch.timeseries.client.DataPlaneClientFactory;
import org.opensearch.timeseries.client.DataPlaneClientFactoryContext;
import org.opensearch.timeseries.client.RestClientProvider;
import org.opensearch.timeseries.constant.CommonName;
import org.opensearch.timeseries.ml.CheckpointDaoInterface;
import org.opensearch.timeseries.model.Job;
import org.opensearch.timeseries.rest.RestCronAction;
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
    protected volatile Duration checkpointTtl;
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
    private final AtomicBoolean lifecycleListenerRegistered = new AtomicBoolean(false);
    private volatile boolean clusterManager = false;

    protected int maxMessages;
    protected int waitTime;
    protected int visibilityTimeout;
    protected final StateManager nodeStateManager;
    protected final CheckpointDaoInterface<?> checkpointStore;
    protected DataPlaneClientFactory sqsSigningFactory;

    protected SQSConsumerTask(StateManager nodeStateManager, CheckpointDaoInterface<?> checkpointStore) {
        this.nodeStateManager = nodeStateManager;
        this.checkpointStore = Objects.requireNonNull(checkpointStore, "checkpointStore must not be null");
    }

    protected void initWithSQSConfig(
        ClusterService cs,
        ThreadPool pool,
        Clock clock,
        DiscoveryNodeSelector filterer,
        Settings settings,
        TimeValue pollingInterval,
        int maxConcurrentProcessors,
        String threadPoolName
    ) {
        initWithSQSConfig(cs, pool, clock, filterer, settings, pollingInterval, maxConcurrentProcessors, threadPoolName, null);
    }

    protected void initWithSQSConfig(
        ClusterService cs,
        ThreadPool pool,
        Clock clock,
        DiscoveryNodeSelector filterer,
        Settings settings,
        TimeValue pollingInterval,
        int maxConcurrentProcessors,
        String threadPoolName,
        DataPlaneClientFactory sqsSigningFactory
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
        this.checkpointTtl = DateUtils.toDuration(AnomalyDetectorSettings.AD_CHECKPOINT_TTL.get(settings));
        this.sqsSigningFactory = sqsSigningFactory;

        this.messageHandler = new DefaultSQSMessageHandler(
            this.nodeStateManager,
            groupSemaphores,
            settings,
            clock,
            checkpointStore,
            () -> checkpointTtl,
            pool
        );
        cs.getClusterSettings().addSettingsUpdateConsumer(AnomalyDetectorSettings.AD_CHECKPOINT_TTL, ttl -> {
            this.checkpointTtl = DateUtils.toDuration(ttl);
        });

        this.pollingInterval = pollingInterval;
        this.concurrencySemaphore = new Semaphore(maxConcurrentProcessors);
        this.messageProcessorExecutor = pool.executor(threadPoolName);

        cs.addLocalNodeClusterManagerListener(this);
        registerLifecycleListenerOnce();
    }

    @Override
    public void onClusterManager() {
        clusterManager = true;
        logger.info("Node became cluster manager, starting SQS consumer");
        restart();
    }

    @Override
    public void offClusterManager() {
        clusterManager = false;
        logger.info("Node stopped being cluster manager, stopping SQS consumer");
        stopPolling();
    }

    protected boolean isClusterManagerNode() {
        return clusterManager;
    }

    protected abstract void restart();

    protected abstract void stopPolling();

    protected QueuePollingState createQueuePollingState() {
        return new QueuePollingState();
    }

    protected Cancellable scheduleQueuePoller(SQSService sqsService, QueuePollingState state, String threadPoolName) {
        return pool.scheduleWithFixedDelay(() -> pollSQS(sqsService, state), pollingInterval, threadPoolName);
    }

    protected void pollSQS(SQSService sqsService, QueuePollingState state) {
        long now = clock.millis();
        if (now < state.backoffUntilMillis) {
            logger.debug("Skipping SQS poll for {} while backing off for {} ms", sqsService.getQueueUrl(), state.backoffUntilMillis - now);
            return;
        }

        if (!isReadyForPolling()) {
            return;
        }

        checkDlqConfigurationOnce(sqsService, state);

        boolean successfulPoll = false;
        try {
            int availableSlots = concurrencySemaphore.availablePermits();
            if (availableSlots == 0) {
                logger.debug("No available concurrency slots for SQS message processing");
                successfulPoll = true;
                return;
            }

            int maxMessages = Math.min(availableSlots, this.maxMessages);
            int waitTime = this.waitTime;

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
                .visibilityTimeout(this.visibilityTimeout)
                .build();

            List<Message> messages = sqsService.receiveMessages(request);

            if (messages.isEmpty()) {
                logger.debug("No messages received from SQS queue {}", sqsService.getQueueUrl());
                state.consecutiveEmptyReceives++;
                if (state.consecutiveEmptyReceives >= EMPTY_RECEIVE_BACKOFF_THRESHOLD) {
                    long sleepMillis = ThreadLocalRandom.current().nextLong(EMPTY_BACKOFF_MIN_MILLIS, EMPTY_BACKOFF_MAX_MILLIS + 1);
                    int empties = state.consecutiveEmptyReceives;
                    state.consecutiveEmptyReceives = 0;
                    state.backoffUntilMillis = clock.millis() + sleepMillis;
                    logger
                        .debug(
                            "Backing off queue {} after {} consecutive empty polls for {}",
                            sqsService.getQueueUrl(),
                            empties,
                            TimeValue.timeValueMillis(sleepMillis)
                        );
                }
                successfulPoll = true;
                return;
            }

            logger.info("Received {} messages from SQS queue {}", messages.size(), sqsService.getQueueUrl());
            state.consecutiveEmptyReceives = 0;

            Map<String, Message> latestByGroup = new LinkedHashMap<>();
            List<Message> toDelete = new ArrayList<>();

            for (Message msg : messages) {
                String groupId = msg.attributes().get(MessageSystemAttributeName.MESSAGE_GROUP_ID);
                String seqNum = msg.attributes().get(MessageSystemAttributeName.SEQUENCE_NUMBER);

                logger.debug("Received SQS message: id={}, groupId={}, sequenceNumber={}", msg.messageId(), groupId, seqNum);

                if (groupId == null) {
                    latestByGroup.put(msg.messageId(), msg);
                    continue;
                }

                Message existing = latestByGroup.get(groupId);
                if (existing == null) {
                    latestByGroup.put(groupId, msg);
                } else {
                    String existingSeq = existing.attributes().get(MessageSystemAttributeName.SEQUENCE_NUMBER);
                    if (compareSequenceNumbers(seqNum, existingSeq) > 0) {
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

            List<Message> messagesToProcess = new ArrayList<>(latestByGroup.values());
            logger
                .debug(
                    "After deduplication on queue {}: {} messages to process (deleted {} older messages)",
                    sqsService.getQueueUrl(),
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
                        processMessage(sqsService, message);
                    } finally {
                        semaphore.release();
                    }
                });
            }
            successfulPoll = true;
        } catch (Exception e) {
            state.consecutivePollErrors++;
            long backoffMillis = computeBackoffMillis(state.consecutivePollErrors);
            state.backoffUntilMillis = clock.millis() + backoffMillis;
            logger
                .error(
                    "Error polling SQS queue {} ({} consecutive failures). Backing off for {}",
                    sqsService.getQueueUrl(),
                    state.consecutivePollErrors,
                    TimeValue.timeValueMillis(backoffMillis),
                    e
                );
        } finally {
            if (successfulPoll) {
                state.consecutivePollErrors = 0;
                if (clock.millis() >= state.backoffUntilMillis) {
                    state.backoffUntilMillis = 0L;
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

    private void registerLifecycleListenerOnce() {
        if (lifecycleListenerRegistered.compareAndSet(false, true)) {
            cs.addLifecycleListener(new LifecycleListener() {
                @Override
                public void beforeStop() {
                    logger.info("Cluster is stopping, cleaning up SQS consumer");
                    stopPolling();
                }
            });
        }
    }

    private void checkDlqConfigurationOnce(SQSService sqsService, QueuePollingState state) {
        if (state.dlqConfigChecked.get()) {
            throwIfDlqCheckFailed(state);
            return;
        }

        synchronized (state) {
            if (state.dlqConfigChecked.get()) {
                throwIfDlqCheckFailed(state);
                return;
            }

            String failureMessage = validateDlqConfiguration(sqsService);
            if (failureMessage != null) {
                logger.warn(failureMessage);
                state.dlqConfigFailureMessage = failureMessage;
            }
            state.dlqConfigChecked.set(true);
            if (failureMessage != null) {
                throw new IllegalStateException(failureMessage);
            }
        }
    }

    private void throwIfDlqCheckFailed(QueuePollingState state) {
        if (state.dlqConfigFailureMessage != null) {
            throw new IllegalStateException(state.dlqConfigFailureMessage);
        }
    }

    private String validateDlqConfiguration(SQSService sqsService) {
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

    private void processMessage(SQSService sqsService, Message message) {
        LockReleaseContext releaseContext = new LockReleaseContext(sqsService, message);
        ThreadContext.StoredContext storedContext = null;
        try {
            if (sqsSigningFactory != null) {
                storedContext = pool.getThreadContext().stashContext();
                DataPlaneClientFactoryContext.setCurrentFactory(pool.getThreadContext(), sqsSigningFactory);
            }
            logger.debug("Processing SQS message: {}", message.messageId());
            DefaultSQSMessageHandler.setCurrentLockReleaseContext(releaseContext);

            messageHandler.processMessage(message.body(), ActionListener.wrap(success -> {
                if (!success) {
                    handleProcessingFailure(message, releaseContext);
                } else if (!releaseContext.isDeletionPlanned()) {
                    try {
                        releaseContext.deleteImmediately();
                    } catch (RuntimeException ex) {
                        releaseContext.cancelDeletion();
                        releaseContext.resetVisibility();
                        logger.error("Failed to delete SQS message {} after processing", message.messageId(), ex);
                    }
                }
            }, e -> {
                logger.error("Exception processing SQS message: " + message.messageId(), e);
                handleProcessingFailure(message, releaseContext);
            }));
        } catch (Exception e) {
            logger.error("Exception processing SQS message: " + message.messageId(), e);
            handleProcessingFailure(message, releaseContext);
        } finally {
            DefaultSQSMessageHandler.clearCurrentLockReleaseContext();
            if (storedContext != null) {
                storedContext.close();
            }
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

    private long computeBackoffMillis(int consecutivePollErrors) {
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
        return new BigInteger(seq1).compareTo(new BigInteger(seq2));
    }

    private void handleProcessingFailure(Message message, LockReleaseContext releaseContext) {
        int receiveCount = getApproximateReceiveCount(message);
        releaseContext.cancelDeletion();
        if (receiveCount >= MAX_PROCESSING_ATTEMPTS) {
            logger.warn("SQS message {} failed {} times.", message.messageId(), receiveCount, MAX_PROCESSING_ATTEMPTS);
        }
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

    protected static void cancel(Cancellable cancellable) {
        if (cancellable != null) {
            cancellable.cancel();
        }
    }

    public Cancellable getSQSPollingCron() {
        return null;
    }

    protected static class QueuePollingState {
        private int consecutivePollErrors = 0;
        private long backoffUntilMillis = 0L;
        private int consecutiveEmptyReceives = 0;
        private final AtomicBoolean dlqConfigChecked = new AtomicBoolean(false);
        private volatile String dlqConfigFailureMessage = null;
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
        private final Supplier<Duration> checkpointTtlSupplier;
        private final ThreadPool threadPool;

        public DefaultSQSMessageHandler(
            StateManager nodeStateManager,
            ConcurrentHashMap<String, Semaphore> groupSemaphores,
            Settings settings,
            Clock clock,
            CheckpointDaoInterface<?> checkpointStore,
            Supplier<Duration> checkpointTtlSupplier
        ) {
            this(nodeStateManager, groupSemaphores, settings, clock, checkpointStore, checkpointTtlSupplier, null);
        }

        public DefaultSQSMessageHandler(
            StateManager nodeStateManager,
            ConcurrentHashMap<String, Semaphore> groupSemaphores,
            Settings settings,
            Clock clock,
            CheckpointDaoInterface<?> checkpointStore,
            Supplier<Duration> checkpointTtlSupplier,
            ThreadPool threadPool
        ) {
            this.nodeStateManager = Objects.requireNonNull(nodeStateManager, "nodeStateManager must not be null");
            this.groupSemaphores = Objects.requireNonNull(groupSemaphores, "groupSemaphores must not be null");
            this.settings = Objects.requireNonNull(settings, "settings must not be null");
            this.clock = Objects.requireNonNull(clock, "clock must not be null");
            this.checkpointStore = Objects.requireNonNull(checkpointStore, "checkpointStore must not be null");
            this.checkpointTtlSupplier = Objects.requireNonNull(checkpointTtlSupplier, "checkpointTtlSupplier must not be null");
            this.threadPool = threadPool;
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
         */
        protected JobExecutionContext parseJobExecutionContext(String messageBody, String jobId, LockService lockService)
            throws IOException {
            XContentParser parser = JsonXContent.jsonXContent
                .createParser(NamedXContentRegistry.EMPTY, LoggingDeprecationHandler.INSTANCE, messageBody);

            Instant expectedExecutionTime = null;
            parser.nextToken();
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
            return new JobExecutionContext(expectedExecutionTime, jobVersion, lockService, "", jobId);
        }

        @Override
        public void processMessage(String messageBody, ActionListener<Boolean> listener) throws Exception {
            Job job = parseJob(messageBody);
            LockReleaseContext releaseContext = getCurrentLockReleaseContext();
            String groupId = releaseContext != null ? releaseContext.getGroupId() : null;
            LockService lockService = new GroupSemaphoreLockService(releaseContext, groupSemaphores, groupId, threadPool);
            JobExecutionContext context = parseJobExecutionContext(messageBody, job.getName(), lockService);
            Supplier<ThreadContext.StoredContext> restorableContext = threadPool == null
                ? null
                : threadPool.getThreadContext().newRestorableContext(false);

            // SQS messages can outlive stop/delete operations, so do not execute from cached job state.
            // The false argument on getJob(..., false, ...) is what makes this safe: it bypasses the
            // cached job state, so a recently-disabled or recently-deleted config will be observed
            // correctly even if a stale cached entry still says "enabled".
            nodeStateManager.getJob(job.getName(), job.getTenantId(), false, ActionListener.wrap(jobOptional -> {
                DataPlaneClientFactoryContext.runWithRestoredContext(restorableContext, () -> {
                    if (!jobOptional.isPresent() || !jobOptional.get().isEnabled()) {
                        logger.info("Job {} is disabled, deleted, or does not exist. Skipping execution.", job.getName());
                        listener.onResponse(Boolean.TRUE);
                        return;
                    }
                    executeJob(job, context, listener, messageBody);
                });
            }, e -> {
                // every failure just resets visibility and lets SQS redeliver.
                // SQS itself enforces the 3-strike rule: after the 3rd unsuccessful receive,
                // SQS redrives the message to the DLQ instead of returning it to the consumer.
                DataPlaneClientFactoryContext.runWithRestoredContext(restorableContext, () -> {
                    logger.warn("Failed to determine latest state for job {}. Leaving SQS message for retry.", job.getName(), e);
                    listener.onResponse(Boolean.FALSE);
                });
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
                        return;
                    case DAILY_S3_CHECKPOINT_CLEANUP:
                        logger.info("Processing daily S3 checkpoint cleanup job");
                        executeCheckpointCleanup(listener);
                        return;
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
                Request request = createCronRequest();

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

        private Request createCronRequest() {
            Request request = new Request("POST", RestCronAction.CRON_URI);

            // Authenticate this inter-node internal call with the shared secret, mirroring
            // HttpNodeCommunicator#applyInternalRequestHeaders for the inference path.
            String internalToken = TimeSeriesSettings.INTERNAL_API_SHARED_SECRET.get(settings);
            if (internalToken != null && !internalToken.isBlank()) {
                RequestOptions.Builder options = RequestOptions.DEFAULT.toBuilder();
                options.addHeader(CommonName.INTERNAL_API_TOKEN_HEADER, internalToken);
                request.setOptions(options);
            }

            return request;
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
                Duration checkpointTtl = Objects.requireNonNull(checkpointTtlSupplier.get(), "checkpointTtl must not be null");
                checkpointStore.createRetentionTask(checkpointTtl, clock).run();
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
            private final ThreadPool threadPool;
            private final AtomicBoolean lockAcquired = new AtomicBoolean(false);
            private final AtomicBoolean groupSemaphoreAcquired = new AtomicBoolean(false);
            private volatile Cancellable scheduledSemaphoreRelease;
            private volatile Semaphore groupSemaphore;

            private GroupSemaphoreLockService(
                LockReleaseContext releaseContext,
                ConcurrentHashMap<String, Semaphore> groupSemaphores,
                String groupId,
                ThreadPool threadPool
            ) {
                this.releaseContext = releaseContext;
                this.groupSemaphores = groupSemaphores;
                this.groupId = groupId;
                this.threadPool = threadPool;
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
                cancelScheduledSemaphoreRelease();
                releaseGroupSemaphorePermit();
            }

            private void releaseGroupSemaphoreOnTimeout() {
                if (groupSemaphoreAcquired.get()) {
                    logger.warn("Auto-releasing SQS group semaphore for group {} after lock duration elapsed", groupId);
                }
                scheduledSemaphoreRelease = null;
                releaseGroupSemaphorePermit();
            }

            private void releaseGroupSemaphorePermit() {
                if (groupSemaphoreAcquired.compareAndSet(true, false) && groupSemaphore != null) {
                    groupSemaphore.release();
                }
            }

            private void scheduleSemaphoreRelease(long lockDurationSeconds) {
                if (threadPool == null || groupId == null) {
                    return;
                }
                try {
                    scheduledSemaphoreRelease = threadPool
                        .schedule(
                            this::releaseGroupSemaphoreOnTimeout,
                            TimeValue.timeValueSeconds(lockDurationSeconds),
                            CommonName.SQS_CONSUMER_THREAD_POOL_NAME
                        );
                } catch (RuntimeException e) {
                    logger.warn("Failed to schedule SQS group semaphore safety release for group {}", groupId, e);
                }
            }

            private void cancelScheduledSemaphoreRelease() {
                Cancellable cancellable = scheduledSemaphoreRelease;
                if (cancellable != null) {
                    cancellable.cancel();
                    scheduledSemaphoreRelease = null;
                }
            }

            @Override
            public void acquireLock(
                final ScheduledJobParameter jobParameter,
                final JobExecutionContext context,
                ActionListener<LockModel> listener
            ) {
                if (!tryAcquireGroupSemaphore()) {
                    listener.onResponse(null);
                    return;
                }
                Long configuredLockDurationSeconds = jobParameter.getLockDurationSeconds();
                long lockDurationSeconds = configuredLockDurationSeconds != null ? configuredLockDurationSeconds : 3600L;
                LockModel lockModel = new LockModel(
                    context.getJobIndexName(),
                    context.getJobId(),
                    Instant.now(),
                    lockDurationSeconds,
                    false
                );
                lockAcquired.set(true);
                if (releaseContext != null) {
                    releaseContext.markLockAcquired();
                }
                scheduleSemaphoreRelease(lockDurationSeconds);
                try {
                    listener.onResponse(lockModel);
                } catch (RuntimeException e) {
                    releaseGroupSemaphore();
                    throw e;
                }
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
                scheduleSemaphoreRelease(lockModel.getLockDurationSeconds());
                try {
                    listener.onResponse(lockModel);
                } catch (RuntimeException e) {
                    releaseGroupSemaphore();
                    throw e;
                }
            }

            @Override
            public void findLock(final String lockId, ActionListener<LockModel> listener) {
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
                listener.onResponse(true);
            }

            @Override
            public void renewLock(final LockModel lock, ActionListener<LockModel> listener) {
                listener.onResponse(lock);
            }
        }
    }
}
