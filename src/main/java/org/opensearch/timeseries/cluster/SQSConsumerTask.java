/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.timeseries.cluster;

import java.time.Clock;
import java.time.Instant;
import java.util.List;
import java.util.concurrent.Executor;
import java.util.concurrent.Semaphore;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.ad.ADJobProcessor;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.lifecycle.LifecycleListener;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.core.action.ActionListener;
import java.io.IOException;

import org.opensearch.common.xcontent.LoggingDeprecationHandler;
import org.opensearch.common.xcontent.json.JsonXContent;
import org.opensearch.core.common.ParsingException;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.threadpool.Scheduler.Cancellable;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.timeseries.constant.CommonName;
import org.opensearch.timeseries.model.Job;
import org.opensearch.timeseries.settings.TimeSeriesSettings;
import org.opensearch.timeseries.sqs.SQSMessageHandler;
import org.opensearch.timeseries.sqs.SQSService;
import org.opensearch.timeseries.util.ClientUtil;
import org.opensearch.timeseries.util.DiscoveryNodeFilterer;
import org.opensearch.transport.client.Client;
import org.opensearch.timeseries.NodeStateManager;

import org.opensearch.jobscheduler.spi.JobExecutionContext;
import org.opensearch.jobscheduler.spi.JobDocVersion;
import org.opensearch.jobscheduler.spi.LockModel;
import org.opensearch.jobscheduler.spi.utils.LockService;

import software.amazon.awssdk.services.sqs.model.Message;
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
    private static final long MIN_ERROR_BACKOFF_MILLIS = TimeUnit.SECONDS.toMillis(5);
    private static final long MAX_ERROR_BACKOFF_MILLIS = TimeUnit.MINUTES.toMillis(1);
    private static final int MAX_BACKOFF_EXPONENT = 6;
    private static final int EMPTY_RECEIVE_BACKOFF_THRESHOLD = 3;
    private static final long EMPTY_BACKOFF_MIN_MILLIS = 200L;
    private static final long EMPTY_BACKOFF_MAX_MILLIS = 1500L;
    private int consecutivePollErrors = 0;
    private long backoffUntilMillis = 0L;
    private int consecutiveEmptyReceives = 0;

    protected int maxMessages;
    protected int waitTime;
    protected int visibilityTimeout;
    protected final NodeStateManager nodeStateManager;

    protected SQSConsumerTask(NodeStateManager nodeStateManager) {
        this.nodeStateManager = nodeStateManager;
    }

    @Override
    public void init(
        ClusterService cs,
        ThreadPool pool,
        Client client,
        Clock clock,
        ClientUtil util,
        DiscoveryNodeFilterer f,
        Settings settings
    ) {
        throw new UnsupportedOperationException("Subclasses must implement this method and call initWithSQSConfig");
    }

    protected void initWithSQSConfig(
        ClusterService cs,
        ThreadPool pool,
        Client client,
        Clock clock,
        ClientUtil util,
        DiscoveryNodeFilterer f,
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
        this.messageHandler = new DefaultSQSMessageHandler(this.nodeStateManager);

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
                    // Backoff with jitter on empties. e.g., after N empty receives, sleep a randomized 200–1500 ms, then resume long polling.
                    long sleepMillis = ThreadLocalRandom.current()
                        .nextLong(EMPTY_BACKOFF_MIN_MILLIS, EMPTY_BACKOFF_MAX_MILLIS + 1);
                    int empties = consecutiveEmptyReceives;
                    consecutiveEmptyReceives = 0;
                    backoffUntilMillis = clock.millis() + sleepMillis;
                    logger.debug(
                        "Backing off after {} consecutive empty polls for {}",
                        empties,
                        TimeValue.timeValueMillis(sleepMillis)
                    );
                }
                successfulPoll = true;
                return;
            }

            logger.info("Received {} messages from SQS", messages.size());
            consecutiveEmptyReceives = 0;

            // Process messages concurrently
            for (Message message : messages) {
                final Semaphore semaphore = concurrencySemaphore;
                if (!semaphore.tryAcquire()) {
                    logger.warn("Max concurrency reached, resetting visibility for message {}", message.messageId());
                    sqsService.resetMessageVisibility(message);
                    continue;
                }

                messageProcessorExecutor.execute(() -> {
                    try {
                        processMessage(message);
                    } finally {
                        semaphore.release();
                    }
                });
            }
            successfulPoll = true;
        } catch (Exception e) {
            consecutivePollErrors++;
            long backoffMillis = computeBackoffMillis();
            backoffUntilMillis = clock.millis() + backoffMillis;
            logger.error(
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

    private void processMessage(Message message) {
        LockReleaseContext releaseContext = new LockReleaseContext(sqsService, message);
        try {
            logger.debug("Processing SQS message: {}", message.messageId());
            DefaultSQSMessageHandler.setCurrentLockReleaseContext(releaseContext);

            messageHandler.processMessage(message.body(), ActionListener.wrap(success -> {
                if (!success) {
                    releaseContext.cancelDeletion();
                    releaseContext.resetVisibility();
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
                releaseContext.cancelDeletion();
                releaseContext.resetVisibility();
                logger.error("Exception processing SQS message: " + message.messageId(), e);
            }));
        } catch (Exception e) {
            releaseContext.cancelDeletion();
            releaseContext.resetVisibility();
            logger.error("Exception processing SQS message: " + message.messageId(), e);
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
     * In a real implementation, this would contain the actual job processing logic.
     */
    public static class DefaultSQSMessageHandler implements SQSMessageHandler {
        private static final ThreadLocal<LockReleaseContext> CURRENT_LOCK_CONTEXT = new ThreadLocal<>();
        private final NodeStateManager nodeStateManager;

        public DefaultSQSMessageHandler(NodeStateManager nodeStateManager) {
            this.nodeStateManager = nodeStateManager;
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
            logger.debug("Parsing job payload, length={}, preview='{}'",
                messageBody == null ? 0 : messageBody.length(),
                messageBody == null ? "" : messageBody.substring(0, Math.min(128, messageBody.length())));
            
            XContentParser parser = JsonXContent.jsonXContent.createParser(
                NamedXContentRegistry.EMPTY,
                LoggingDeprecationHandler.INSTANCE,
                messageBody
            );
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
         * LockService implementation always succeeds for all operations.
         *
         * @param messageBody the message body containing job execution information
         * @param jobId the job ID
         * @return JobExecutionContext instance
         * @throws IOException if parsing fails
         */
        protected JobExecutionContext parseJobExecutionContext(String messageBody, String jobId) throws IOException {
            XContentParser parser = JsonXContent.jsonXContent.createParser(
                NamedXContentRegistry.EMPTY,
                LoggingDeprecationHandler.INSTANCE,
                messageBody
            );

            Instant expectedExecutionTime = null;
            parser.nextToken(); // Move to START_OBJECT
            while (parser.nextToken() != XContentParser.Token.END_OBJECT) {
                String fieldName = parser.currentName();
                parser.nextToken();
                if (CommonName.EB_SCHEDULED_TIME_FIELD.equals(fieldName)) {
                    if (parser.currentToken() == XContentParser.Token.VALUE_STRING) {
                        expectedExecutionTime = Instant.parse(parser.text());
                    } else {
                        logger.error("Expected " + CommonName.EB_SCHEDULED_TIME_FIELD + " field to be a string, got: " + parser.currentToken());
                        throw new IOException("Expected " + CommonName.EB_SCHEDULED_TIME_FIELD + " field to be a string, got: " + parser.currentToken());
                    }
                } else {
                    parser.skipChildren();
                }
            }

            if (expectedExecutionTime == null) {
                throw new IOException("Missing " + CommonName.EB_SCHEDULED_TIME_FIELD + " field in message body");
            }

            JobDocVersion jobVersion = new JobDocVersion(1, 1, 1);
            LockService lockService = new AlwaysSucceedLockService(getCurrentLockReleaseContext());

            // job index name is not used for SQS, so we use an empty string
            return new JobExecutionContext(expectedExecutionTime, jobVersion, lockService, "", jobId);
        }

        @Override
        public void processMessage(String messageBody, ActionListener<Boolean> listener) throws Exception {
            Job job = parseJob(messageBody);
            JobExecutionContext context = parseJobExecutionContext(messageBody, job.getName());
            if (nodeStateManager == null) {
                executeJob(job, context, listener, messageBody);
                return;
            }

            nodeStateManager.getJob(job.getName(), ActionListener.wrap(jobOptional -> {
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
                logger.info("Successfully deserialized Job object: name={}, enabled={}, analysisType={}",
                    job.getName(), job.isEnabled(), job.getAnalysisType());
                switch (job.getAnalysisType()) {
                    case AD:
                        logger.info("Processing AD job: {}", job.getName());
                        ADJobProcessor.getInstance().process(job, context, job.getTenantId());
                        break;
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
         * Simple LockService implementation that always succeeds for all operations.
         * SQS FIFO queue works like a lock service, so we can just return a dummy lock model
         * to reuse existing code.
         */
        private static class AlwaysSucceedLockService implements LockService {
            private final LockReleaseContext releaseContext;
            private final AtomicBoolean lockAcquired = new AtomicBoolean(false);

            private AlwaysSucceedLockService(LockReleaseContext releaseContext) {
                this.releaseContext = releaseContext;
            }

            @Override
            public void acquireLock(final org.opensearch.jobscheduler.spi.ScheduledJobParameter jobParameter,
                                  final JobExecutionContext context,
                                  ActionListener<LockModel> listener) {
                // Always succeed - create a dummy lock model
                LockModel lockModel = new LockModel(context.getJobIndexName(), context.getJobId(),
                    Instant.now(), 3600L, false); // 1 hour duration
                lockAcquired.set(true);
                if (releaseContext != null) {
                    releaseContext.markLockAcquired();
                }
                listener.onResponse(lockModel);
            }

            @Override
            public void acquireLockWithId(final String jobIndexName,
                                        final Long lockDurationSeconds,
                                        final String lockId,
                                        ActionListener<LockModel> listener) {
                // Always succeed - create a dummy lock model
                LockModel lockModel = new LockModel(jobIndexName, lockId,
                    Instant.now(), lockDurationSeconds != null ? lockDurationSeconds : 3600L, false);
                lockAcquired.set(true);
                if (releaseContext != null) {
                    releaseContext.markLockAcquired();
                }
                listener.onResponse(lockModel);
            }

            @Override
            public void findLock(final String lockId, ActionListener<LockModel> listener) {
                // Return null to indicate lock not found
                listener.onResponse(new LockModel(null, lockId,
                        Instant.now(), 3600L, false));
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
