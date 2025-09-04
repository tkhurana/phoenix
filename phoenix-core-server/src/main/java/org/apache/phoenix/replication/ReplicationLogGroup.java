/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.phoenix.replication;

import static org.apache.phoenix.replication.ReplicationLogGroup.LogEvent.EVENT_TYPE_DATA;
import static org.apache.phoenix.replication.ReplicationLogGroup.LogEvent.EVENT_TYPE_SYNC;

import java.io.IOException;
import java.io.InterruptedIOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.DoNotRetryIOException;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hbase.thirdparty.com.google.common.collect.ImmutableMap;
import org.apache.hbase.thirdparty.com.google.common.collect.Maps;
import org.apache.phoenix.jdbc.HAGroupStoreManager;
import org.apache.phoenix.replication.metrics.MetricsReplicationLogGroupSource;
import org.apache.phoenix.replication.metrics.MetricsReplicationLogGroupSourceImpl;
import org.apache.phoenix.thirdparty.com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.lmax.disruptor.EventFactory;
import com.lmax.disruptor.EventHandler;
import com.lmax.disruptor.ExceptionHandler;
import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.YieldingWaitStrategy;
import com.lmax.disruptor.dsl.Disruptor;
import com.lmax.disruptor.dsl.ProducerType;

/**
 * ReplicationLogGroup manages a group of replication logs for a given HA Group.
 * <p>
 * This class provides an API for replication operations and delegates to either synchronous
 * replication (StandbyLogGroupWriter) or store-and-forward replication
 * (StoreAndForwardLogGroupWriter) based on the current replication mode.
 * <p>
 * Key features:
 * <ul>
 *   <li>Manages multiple replication logs for an HA Group</li>
 *   <li>Provides append() and sync() API for higher layers</li>
 *   <li>Delegates to appropriate writer implementation based on replication mode</li>
 *   <li>Thread-safe operations</li>
 * </ul>
 * <p>
 * The class delegates actual replication work to implementations of ReplicationLogGroupWriter:
 * <ul>
 *   <li>StandbyLogGroupWriter: Synchronous replication to standby cluster</li>
 *   <li>StoreAndForwardLogGroupWriter: Local storage with forwarding when available</li>
 * </ul>
 */
public class ReplicationLogGroup {

    private static final Logger LOG = LoggerFactory.getLogger(ReplicationLogGroup.class);

    // Configuration constants from original ReplicationLog
    public static final String REPLICATION_REMOTE_HDFS_URL_KEY =
        "phoenix.replication.log.standby.hdfs.url";
    public static final String REPLICATION_LOCAL_HDFS_URL_KEY =
        "phoenix.replication.log.fallback.hdfs.url";
    public static final String REPLICATION_LOG_ROTATION_TIME_MS_KEY =
        "phoenix.replication.log.rotation.time.ms";
    public static final long DEFAULT_REPLICATION_LOG_ROTATION_TIME_MS = 60 * 1000L;
    public static final String REPLICATION_LOG_ROTATION_SIZE_BYTES_KEY =
        "phoenix.replication.log.rotation.size.bytes";
    public static final long DEFAULT_REPLICATION_LOG_ROTATION_SIZE_BYTES = 256 * 1024 * 1024L;
    public static final String REPLICATION_LOG_ROTATION_SIZE_PERCENTAGE_KEY =
        "phoenix.replication.log.rotation.size.percentage";
    public static final double DEFAULT_REPLICATION_LOG_ROTATION_SIZE_PERCENTAGE = 0.95;
    public static final String REPLICATION_LOG_COMPRESSION_ALGORITHM_KEY =
        "phoenix.replication.log.compression";
    public static final String DEFAULT_REPLICATION_LOG_COMPRESSION_ALGORITHM = "NONE";
    public static final String REPLICATION_LOG_RINGBUFFER_SIZE_KEY =
        "phoenix.replication.log.ringbuffer.size";
    public static final int DEFAULT_REPLICATION_LOG_RINGBUFFER_SIZE = 1024 * 32;
    public static final String REPLICATION_LOG_SYNC_TIMEOUT_KEY =
        "phoenix.replication.log.sync.timeout.ms";
    public static final long DEFAULT_REPLICATION_LOG_SYNC_TIMEOUT = 1000 * 30;
    public static final String REPLICATION_LOG_SYNC_RETRIES_KEY =
        "phoenix.replication.log.sync.retries";
    public static final int DEFAULT_REPLICATION_LOG_SYNC_RETRIES = 5;
    public static final String REPLICATION_LOG_ROTATION_RETRIES_KEY =
        "phoenix.replication.log.rotation.retries";
    public static final int DEFAULT_REPLICATION_LOG_ROTATION_RETRIES = 5;
    public static final String REPLICATION_LOG_RETRY_DELAY_MS_KEY =
        "phoenix.replication.log.retry.delay.ms";
    public static final long DEFAULT_REPLICATION_LOG_RETRY_DELAY_MS = 100L;

    public static final String FILE_NAME_FORMAT = "%d_%s.plog";

    /** Cache of ReplicationLogGroup instances by HA Group ID */
    protected static final ConcurrentHashMap<String, ReplicationLogGroup> INSTANCES =
        new ConcurrentHashMap<>();

    protected final Configuration conf;
    protected final ServerName serverName;
    protected final String haGroupName;
    protected final MetricsReplicationLogGroupSource metrics;
    protected long syncTimeoutMs;
    protected ReplicationLog remoteLog;
    protected ReplicationLog localLog;
    protected ReplicationMode mode;
    protected volatile boolean closed = false;

    protected static class Record {
        public String tableName;
        public long commitId;
        public Mutation mutation;

        public Record(String tableName, long commitId, Mutation mutation) {
            this.tableName = tableName;
            this.commitId = commitId;
            this.mutation = mutation;
        }
    }

    /** Event structure for the Disruptor ring buffer containing data and sync operations. */
    protected static class LogEvent {
        protected static final EventFactory<LogEvent> EVENT_FACTORY = LogEvent::new;

        protected int type;
        protected Record record;
        protected CompletableFuture<Void> syncFuture; // Used only for SYNC events
        protected long timestampNs; // Timestamp when event was created

        public static final byte EVENT_TYPE_DATA = 0;
        public static final byte EVENT_TYPE_SYNC = 1;

        public void setValues(int type, Record record, CompletableFuture<Void> syncFuture) {
            this.type = type;
            this.record = record;
            this.syncFuture = syncFuture;
            this.timestampNs = System.nanoTime();
        }
    }

    protected Disruptor<LogEvent> disruptor;
    protected RingBuffer<LogEvent> ringBuffer;

    /**
     * Tracks the current replication mode of the ReplicationLog.
     * <p>
     * The replication mode determines how mutations are handled:
     * <ul>
     *   <li>SYNC: Normal operation where mutations are written directly to the standby cluster's
     *   HDFS.
     *   This is the default and primary mode of operation.</li>
     *   <li>STORE_AND_FORWARD: Fallback mode when the standby cluster's HDFS is unavailable.
     *   Mutations are stored locally and will be forwarded when connectivity is restored.</li>
     *   <li>SYNC_AND_FORWARD: Transitional mode where new mutations are written directly to the
     *   standby cluster while concurrently draining the local queue of previously stored
     *   mutations.</li>
     * </ul>
     * <p>
     * Mode transitions occur automatically based on the availability of the standby cluster's HDFS
     * and the state of the local mutation queue.
     */
    protected enum ReplicationMode {
        /**
         * Normal operation where mutations are written directly to the standby cluster's HDFS.
         * This is the default and primary mode of operation.
         */
        SYNC,

        /**
         * Fallback mode when the standby cluster's HDFS is unavailable. Mutations are stored
         * locally and will be forwarded when connectivity is restored.
         */
        STORE_AND_FORWARD,

        /**
         * Transitional mode where new mutations are written directly to the standby cluster
         * while concurrently draining the local queue of previously stored mutations. This mode
         * is entered when connectivity to the standby cluster is restored and there are still
         * mutations in the local queue.
         */
        SYNC_AND_FORWARD
    }

    private static final ImmutableMap<ReplicationMode,
            EnumSet<ReplicationMode>> allowedTransition = Maps.immutableEnumMap(ImmutableMap.of(
                    ReplicationMode.SYNC,
                    EnumSet.of(ReplicationMode.STORE_AND_FORWARD),
                    ReplicationMode.STORE_AND_FORWARD,
                    EnumSet.of(ReplicationMode.SYNC_AND_FORWARD),
                    ReplicationMode.SYNC_AND_FORWARD,
                    EnumSet.of(ReplicationMode.SYNC, ReplicationMode.STORE_AND_FORWARD)));

    /**
     * Get or create a ReplicationLogGroup instance for the given HA Group.
     *
     * @param conf Configuration object
     * @param serverName The server name
     * @param haGroupName The HA Group name
     * @return ReplicationLogGroup instance
     * @throws RuntimeException if initialization fails
     */
    public static ReplicationLogGroup get(Configuration conf, ServerName serverName,
            String haGroupName) {
        return INSTANCES.computeIfAbsent(haGroupName, k -> {
            try {
                ReplicationLogGroup group = new ReplicationLogGroup(conf, serverName, haGroupName);
                group.init();
                return group;
            } catch (IOException e) {
                LOG.error("Failed to create ReplicationLogGroup for HA Group: {}", haGroupName, e);
                throw new RuntimeException(e);
            }
        });
    }

    /**
     * Protected constructor for ReplicationLogGroup.
     *
     * @param conf Configuration object
     * @param serverName The server name
     * @param haGroupName The HA Group name
     */
    protected ReplicationLogGroup(Configuration conf, ServerName serverName, String haGroupName) {
        this.conf = conf;
        this.serverName = serverName;
        this.haGroupName = haGroupName;
        this.metrics = createMetricsSource();
    }

    /**
     * Initialize the ReplicationLogGroup by creating the appropriate writer implementation.
     *
     * @throws IOException if initialization fails
     */
    protected void init() throws IOException {
        // Create the replication logs before we initialize the Disruptor.
        // We need the local writer created first if we intend to fall back to it should the init
        // of the remote writer fail.
        localLog = createLocalLog();
        // Initialize the remote writer and set the mode to SYNC. TODO: switch instead of set
        mode = ReplicationMode.SYNC;
        remoteLog = createRemoteLog();
        // TODO: Switch the initial mode to STORE_AND_FORWARD if the remote writer fails to
        // initialize.
        this.syncTimeoutMs = conf.getLong(ReplicationLogGroup.REPLICATION_LOG_SYNC_TIMEOUT_KEY,
                ReplicationLogGroup.DEFAULT_REPLICATION_LOG_SYNC_TIMEOUT);
        initializeDisruptor();
        LOG.info("Started ReplicationLogGroup for HA Group: {}", this);
    }

    /** Initialize the Disruptor. */
    @SuppressWarnings("unchecked")
    protected void initializeDisruptor() throws IOException {
        int ringBufferSize = conf.getInt(REPLICATION_LOG_RINGBUFFER_SIZE_KEY,
                DEFAULT_REPLICATION_LOG_RINGBUFFER_SIZE);
        disruptor = new Disruptor<>(LogEvent.EVENT_FACTORY, ringBufferSize,
                new ThreadFactoryBuilder()
                        .setNameFormat("ReplicationLogGroup-" + getHaGroupName() + "-%d")
                        .setDaemon(true).build(),
                ProducerType.MULTI, new YieldingWaitStrategy());
        LogEventHandler eventHandler = new LogEventHandler();
        disruptor.handleEventsWith(eventHandler);
        LogExceptionHandler exceptionHandler = new LogExceptionHandler();
        disruptor.setDefaultExceptionHandler(exceptionHandler);
        ringBuffer = disruptor.start();
    }

    /**
     * Handles events from the Disruptor, managing batching, writer rotation, and error handling.
     */
    protected class LogEventHandler implements EventHandler<LogEvent> {
        private final List<Record> currentBatch = new ArrayList<>();
        private final List<CompletableFuture<Void>> pendingSyncFutures = new ArrayList<>();
        private long generation;

        protected LogEventHandler() {
            this.generation = getActiveLog().getGeneration();
        }

        /**
         * Processes all pending sync operations by syncing the current writer and completing
         * their associated futures. This method is called when we are ready to process a set of
         * consolidated sync requests and performs the following steps:
         * <ol>
         *   <li>Syncs the current writer to ensure all data is durably written.</li>
         *   <li>Completes all pending sync futures successfully.</li>
         *   <li>Clears the list of pending sync futures.</li>
         *   <li>Clears the current batch of records since they have been successfully synced.</li>
         * </ol>
         * @param log The which should process the sync event
         * @param sequence The sequence number of the last processed event
         * @throws IOException if the sync operation fails
         */
        private void processPendingSyncs(ReplicationLog log, long sequence) throws IOException {
            if (pendingSyncFutures.isEmpty()) {
                return;
            }
            log.sync();
            // Complete all pending sync futures
            for (CompletableFuture<Void> future : pendingSyncFutures) {
                future.complete(null);
            }
            pendingSyncFutures.clear();
            // Sync completed, clear the list of in-flight appends.
            currentBatch.clear();
            LOG.trace("Sync operation completed successfully up to sequence {}", sequence);
        }

        /**
         * Fails all pending sync operations with the given exception. This method is called when
         * we encounter an unrecoverable error during the sync of the inner writer. It completes
         * all pending sync futures that were consolidated exceptionally.
         * <p>
         * Note: This method does not clear the currentBatch list. The currentBatch must be
         * preserved as it contains records that may need to be replayed if we successfully
         * rotate to a new writer.
         *
         * @param sequence The sequence number of the last processed event
         * @param e The IOException that caused the failure
         */
        private void failPendingSyncs(long sequence, IOException e) {
            if (pendingSyncFutures.isEmpty()) {
                return;
            }
            for (CompletableFuture<Void> future : pendingSyncFutures) {
                future.completeExceptionally(e);
            }
            pendingSyncFutures.clear();
            LOG.warn("Failed to process syncs at sequence {}", sequence, e);
        }

        /**
         *
         * @param e
         */
        private void onFailure(long sequence, IOException e) throws IOException {
            switch (mode) {
                case SYNC:
                case SYNC_AND_FORWARD:
                    switchMode(ReplicationMode.STORE_AND_FORWARD, e);
                    // We have switched the mode, replay the batch
                    replayBatch(sequence);
                case STORE_AND_FORWARD:
                    // can't recover from IOException in STORE_AND_FORWARD mode
                    throw e;
            }
        }

        private void replayBatch(long sequence) throws IOException {
            ReplicationLog log = getActiveLog();
            for (Record r : currentBatch) {
                log.append(r.tableName, r.commitId, r.mutation);
            }
            processPendingSyncs(log, sequence);
        }

        /**
         * Processes a single event from the Disruptor ring buffer. This method handles both data
         * and sync events, with retry logic for handling IO failures.
         * <p>
         * For data events, it:
         * <ol>
         *   <li>Checks if the writer has been rotated and replays any in-flight records.</li>
         *   <li>Appends the record to the current writer.</li>
         *   <li>Adds the record to the current batch for potential replay.</li>
         *   <li>Processes any pending syncs if this is the end of a batch.</li>
         * </ol>
         * <p>
         * For sync events, it:
         * <ol>
         *   <li>Adds the sync future to the pending list.</li>
         *   <li>Processes any pending syncs if this is the end of a batch.</li>
         * </ol>
         * If an IOException occurs, the method will attempt to rotate the writer and retry the
         * operation up to the configured maximum number of retries. If all retries fail, it will
         * fail all pending syncs and throw the exception.
         * <p>
         * The retry logic includes a configurable delay between attempts to prevent tight loops
         * when there are persistent HDFS issues. This delay helps mitigate the risk of rapid
         * cycling through writers when the underlying storage system is experiencing problems.
         *
         * @param event The event to process
         * @param sequence The sequence number of the event
         * @param endOfBatch Whether this is the last event in the current batch
         * @throws Exception if the operation fails after all retries
         */
        @Override
        public void onEvent(LogEvent event, long sequence, boolean endOfBatch) throws Exception {
            // Calculate time spent in ring buffer
            long currentTimeNs = System.nanoTime();
            long ringBufferTimeNs = currentTimeNs - event.timestampNs;
            metrics.updateRingBufferTime(ringBufferTimeNs);

            // find the current active log to which the event needs to be sent to
            ReplicationLog log = getActiveLog();
            try {
                if (log.getGeneration() > generation) {
                    generation = log.getGeneration();
                    // If the writer has been rotated, we need to replay the current batch of
                    // in-flight appends into the new writer.
                    if (!currentBatch.isEmpty()) {
                        LOG.trace("Writer has been rotated, replaying in-flight batch");
                        for (Record r: currentBatch) {
                            log.append(r.tableName,  r.commitId,  r.mutation);
                        }
                    }
                }
                switch (event.type) {
                    case EVENT_TYPE_DATA:
                        currentBatch.add(event.record);
                        log.append(event.record.tableName, event.record.commitId,
                                event.record.mutation);
                        // Process any pending syncs at the end of batch.
                        if (endOfBatch) {
                            processPendingSyncs(log, sequence);
                        }
                        return;
                    case EVENT_TYPE_SYNC:
                        // Add this sync future to the pending list
                        // OK, to add the same future multiple times when we rewind the batch
                        // as completing an already completed future is a no-op
                        pendingSyncFutures.add(event.syncFuture);
                        // Process any pending syncs at the end of batch.
                        if (endOfBatch) {
                            processPendingSyncs(log, sequence);
                        }
                        return;
                    default:
                        throw new UnsupportedOperationException("Unknown event type: "
                                + event.type);
                }
            } catch (IOException e) {
                try {
                    onFailure(sequence, e);
                } catch (IOException e1) {
                    // Either we failed to switch the mode or we are in STORE_AND_FORWARD mode
                    // and got an exception
                    failPendingSyncs(sequence, e);
                    // don't throw the exception and halt the disruptor
                    // we only halt the disruptor on fatal exceptions
                }
            }
        }
    }

    /**
     * Handler for critical errors during the Disruptor lifecycle that closes the writer to prevent
     * data loss.
     */
    protected class LogExceptionHandler implements ExceptionHandler<LogEvent> {
        @Override
        public void handleEventException(Throwable e, long sequence, LogEvent event) {
            String message = "Exception processing sequence " + sequence + "  for event " + event;
            LOG.error(message, e);
            closeOnError();
        }

        @Override
        public void handleOnStartException(Throwable e) {
            LOG.error("Exception during Disruptor startup", e);
            closeOnError();
        }

        @Override
        public void handleOnShutdownException(Throwable e) {
            // Should not happen, but if it does, the regionserver is aborting or shutting down.
            LOG.error("Exception during Disruptor shutdown", e);
            closeOnError();
        }
    }

    /**
     * Get the name for this HA Group.
     *
     * @return The name for this HA Group
     */
    public String getHaGroupName() {
        return haGroupName;
    }

    protected Configuration getConfiguration() {
        return conf;
    }

    protected ServerName getServerName() {
        return serverName;
    }

    @Override
    public String toString() {
        return getHaGroupName();
    }

    /**
     * Append a mutation to the log. This method is non-blocking and returns quickly, unless the
     * ring buffer is full. The actual write happens asynchronously. We expect multiple append()
     * calls followed by a sync(). The appends will be batched by the Disruptor. Should the ring
     * buffer become full, which is not expected under normal operation but could (and should)
     * happen if the log file writer is unable to make progress, due to a HDFS level disruption.
     * Should we enter that condition this method will block until the append can be inserted.
     * <p>
     * An internal error may trigger fail-stop behavior. Subsequent to fail-stop, this method will
     * throw an IOException("Closed"). No further appends are allowed.
     *
     * @param tableName The name of the HBase table the mutation applies to.
     * @param commitId  The commit identifier (e.g., SCN) associated with the mutation.
     * @param mutation  The HBase Mutation (Put or Delete) to be logged.
     * @throws IOException If the writer is closed or if the ring buffer is full.
     */
    public void append(String tableName, long commitId, Mutation mutation) throws IOException {
        if (LOG.isTraceEnabled()) {
            LOG.trace("Append: table={}, commitId={}, mutation={}", tableName, commitId, mutation);
        }
        if (closed) {
            throw new IOException("Closed");
        }
        long startTime = System.nanoTime();
        try {
            // ringBuffer.next() claims the next sequence number. Because we initialize the Disruptor
            // with ProducerType.MULTI and the blocking YieldingWaitStrategy this call WILL BLOCK if
            // the ring buffer is full, thus providing backpressure to the callers.
            long sequence = ringBuffer.next();
            try {
                LogEvent event = ringBuffer.get(sequence);
                event.setValues(EVENT_TYPE_DATA, new Record(tableName, commitId, mutation), null);
            } finally {
                // Update ring buffer events metric
                ringBuffer.publish(sequence);
            }
        } finally {
            metrics.updateAppendTime(System.nanoTime() - startTime);
        }
    }

    /**
     * Ensures all previously appended records are durably persisted. This method blocks until the
     * sync operation completes or fails, potentially after internal retries. All in flight appends
     * are batched and provided to the underlying LogWriter, which will then be synced. If there is
     * a problem syncing the LogWriter we will retry, up to the retry limit, rolling the writer for
     * each retry.
     * <p>
     * An internal error may trigger fail-stop behavior. Subsequent to fail-stop, this method will
     * throw an IOException("Closed"). No further syncs are allowed.
     * <p>
     * NOTE: When the ReplicationLogManager is capable of switching between synchronous and
     * fallback (store-and-forward) writers, then this will be pretty bullet proof. Right now we
     * will still try to roll the synchronous writer a few times before giving up.
     * @throws IOException If the sync operation fails after retries, or if interrupted.
     */
    public void sync() throws IOException {
        if (LOG.isTraceEnabled()) {
            LOG.trace("Sync");
        }
        if (closed) {
            throw new IOException("Closed");
        }
        long startTime = System.nanoTime();
        try {
            syncInternal();
        } finally {
            metrics.updateSyncTime(System.nanoTime() - startTime);
        }
    }

    /**
     * Internal implementation of sync that publishes a sync event to the ring buffer and waits
     * for completion.
     */
    protected void syncInternal() throws IOException {
        CompletableFuture<Void> syncFuture = new CompletableFuture<>();
        long sequence = ringBuffer.next();
        try {
            LogEvent event = ringBuffer.get(sequence);
            event.setValues(EVENT_TYPE_SYNC, null, syncFuture);
        } finally {
            ringBuffer.publish(sequence);
        }
        LOG.trace("Published EVENT_TYPE_SYNC at sequence {}", sequence);
        try {
            // Wait for the event handler to process up to and including this sync event
            syncFuture.get(syncTimeoutMs, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new InterruptedIOException("Interrupted while waiting for sync");
        } catch (ExecutionException e) {
            LOG.error("Sync operation failed", e.getCause());
            if (e.getCause() instanceof IOException) {
                throw (IOException) e.getCause();
            } else {
                throw new IOException("Sync operation failed", e.getCause());
            }
        } catch (TimeoutException e) {
            String message = "Sync operation timed out";
            LOG.error(message);
            throw new IOException(message, e);
        }
    }

    /**
     * Check if this ReplicationLogGroup is closed.
     *
     * @return true if closed, false otherwise
     */
    public boolean isClosed() {
        return closed;
    }

    /**
     * Force closes the log group upon an unrecoverable internal error.
     * This is a fail-stop behavior: once called, the log group is marked as closed,
     * the Disruptor is halted, and all subsequent append() and sync() calls will
     * throw an IOException("Closed"). This ensures that no further operations are attempted on a
     * log group that has encountered a critical error.
     */
    protected void closeOnError() {
        if (closed) {
            return;
        }
        synchronized (this) {
            if (closed) {
                return;
            }
            closed = true;
        }
        // Directly halt the disruptor. shutdown() would wait for events to drain. We are expecting
        // that will not work.
        disruptor.halt();
        closeLog(remoteLog);
        closeLog(localLog);
        metrics.close();
        LOG.info("Closed on error replicationLogGroup for HA Group: {}", haGroupName);
    }

    /**
     * Close the ReplicationLogGroup and all associated resources. This method is thread-safe and
     * can be called multiple times.
     */
    public void close() {
        if (closed) {
            return;
        }
        synchronized (this) {
            if (closed) {
                return;
            }
            closed = true;
            // Remove from instances cache
            INSTANCES.remove(haGroupName);
            // Sync before shutting down to flush all pending appends.
            try {
                syncInternal();
                disruptor.shutdown(); // Wait for a clean shutdown.
            } catch (IOException e) {
                LOG.warn("Error during final sync on close", e);
                disruptor.halt(); // Go directly to halt.
            }
            // TODO revisit close logic and the below comment
            // We must wait for the disruptor before closing the writers.
            // Close the writers, remote first. If there are any problems closing the remote writer
            // the pending writes will be sent to the local writer instead, during the appropriate
            // mode switch.
            closeLog(remoteLog);
            closeLog(localLog);
            metrics.close();
            LOG.info("Closed ReplicationLogGroup for HA Group: {}", haGroupName);
        }
    }

    /**
     * Switch the replication mode.
     *
     * @param newMode The new replication mode
     * @param reason The reason for the mode switch
     * @throws IOException If the mode switch fails
     */
    public void switchMode(ReplicationMode newMode, Throwable reason) throws IOException {
        if (mode.equals(newMode)) {
            LOG.info("HA group {} is already in new mode {}", this, newMode);
            return;
        }
        EnumSet<ReplicationMode> allowedToStates = allowedTransition.get(this.mode);
        if (allowedToStates == null || !allowedToStates.contains(newMode)) {
            throw new DoNotRetryIOException("Can not transit HA Group " + haGroupName +
                    " mode from " + this.mode + " to " + newMode);
        }

        LOG.info("Attempting to switch replication mode for HA Group: {} from {} to {} because {}",
                this, this.mode, newMode, reason);

        HAGroupStoreManager haGroupStoreManager = HAGroupStoreManager.getInstance(conf);

        switch (mode) {
            case SYNC:
                // SYNC -> STORE_AND_FORWARD
                try {
                    haGroupStoreManager.setHAGroupStatusToStoreAndForward(haGroupName);
                } catch (IOException e) {
                    throw e;
                }
                catch (Exception e) {
                    throw new IOException(e);
                }
                break;
        }

        LOG.info("Switched replication mode for HA Group: {} from {} to {}",
                this, this.mode, newMode);
    }

    /** Get the current metrics source for monitoring operations. */
    public MetricsReplicationLogGroupSource getMetrics() {
        return metrics;
    }

    /** Create a new metrics source for monitoring operations. */
    protected MetricsReplicationLogGroupSource createMetricsSource() {
        return new MetricsReplicationLogGroupSourceImpl(haGroupName);
    }

    /** Close the given writer. */
    protected void closeLog(ReplicationLog log) {
        if (log != null) {
            log.close();
        }
    }


    private URI getLogURI(String urlKey) throws IOException {
        String urlString = conf.get(urlKey);
        if (urlString == null || urlString.trim().isEmpty()) {
            throw new IOException("HDFS URL not configured: " + urlKey);
        }
        try {
            return new URI(urlString);
        } catch (URISyntaxException e) {
            throw new IOException("Invalid HDFS URL: " + urlString, e);
        }
    }

    /** Create the remote (synchronous) writer. Mainly for tests. */
    protected ReplicationLog createRemoteLog() throws IOException {
        URI remoteURI = getLogURI(ReplicationLogGroup.REPLICATION_REMOTE_HDFS_URL_KEY);
        ReplicationLog log = new ReplicationLog(this, remoteURI);
        log.init();
        return log;
    }

    /** Create the local (store and forward) writer. Mainly for tests. */
    protected ReplicationLog createLocalLog() throws IOException {
        URI localURI = getLogURI(ReplicationLogGroup.REPLICATION_LOCAL_HDFS_URL_KEY);
        ReplicationLog log = new ReplicationLog(this, localURI);
        log.init();
        return log;
    }

    /** Returns the currently active writer. Mainly for tests. */
    protected ReplicationLog getActiveLog() {
        switch (mode) {
        case SYNC:
            return remoteLog;
        case SYNC_AND_FORWARD:
            return remoteLog;
        case STORE_AND_FORWARD:
            return localLog;
        default:
            throw new IllegalStateException("Invalid replication mode: " + mode);
        }
    }
}
