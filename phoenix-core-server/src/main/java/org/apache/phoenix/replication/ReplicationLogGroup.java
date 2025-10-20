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

import static org.apache.hadoop.hbase.HConstants.DEFAULT_ZK_SESSION_TIMEOUT;
import static org.apache.hadoop.hbase.HConstants.ZK_SESSION_TIMEOUT;
import static org.apache.phoenix.replication.ReplicationLogGroup.LogEvent.EVENT_TYPE_DATA;
import static org.apache.phoenix.replication.ReplicationLogGroup.LogEvent.EVENT_TYPE_SYNC;
import static org.apache.phoenix.replication.ReplicationLogGroup.ReplicationMode.State.INIT;
import static org.apache.phoenix.replication.ReplicationLogGroup.ReplicationMode.State.SYNC;
import static org.apache.phoenix.replication.ReplicationLogGroup.ReplicationMode.State.STORE_AND_FORWARD;
import static org.apache.phoenix.replication.ReplicationLogGroup.ReplicationMode.State.SYNC_AND_FORWARD;

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
import java.util.concurrent.atomic.AtomicReference;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.DoNotRetryIOException;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hdfs.DFSConfigKeys;
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
    public static final int DEFAULT_REPLICATION_LOG_SYNC_RETRIES = 4;
    public static final String REPLICATION_LOG_ROTATION_RETRIES_KEY =
        "phoenix.replication.log.rotation.retries";
    public static final int DEFAULT_REPLICATION_LOG_ROTATION_RETRIES = 5;
    public static final String REPLICATION_LOG_RETRY_DELAY_MS_KEY =
        "phoenix.replication.log.retry.delay.ms";
    public static final long DEFAULT_REPLICATION_LOG_RETRY_DELAY_MS = 100L;
    private static final long DEFAULT_HDFS_WRITE_RPC_TIMEOUT_MS = 30*1000;

    public static final String FILE_NAME_FORMAT = "%d_%s.plog";
    public static final String REMOTE_DIR = "in";
    public static final String LOCAL_DIR = "out";

    /** Cache of ReplicationLogGroup instances by HA Group ID */
    protected static final ConcurrentHashMap<String, ReplicationLogGroup> INSTANCES =
        new ConcurrentHashMap<>();

    protected final Configuration conf;
    protected final ServerName serverName;
    protected final String haGroupName;
    protected final HAGroupStoreManager haGroupStoreManager;
    protected final MetricsReplicationLogGroupSource metrics;
    protected long syncTimeoutMs;
    private AtomicReference<ReplicationMode> mode = new AtomicReference<>();
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
    protected abstract static class ReplicationMode {
        enum State {
            /**
             *
             */
            INIT,

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

        private final State state;

        // The mode manages the underlying log to which the append and sync events will be sent
        protected ReplicationLog log;

        protected ReplicationMode(State state) {
            this.state = state;
        }

        /**
         *
         * @throws IOException
         */
        abstract void onEnter() throws IOException;

        /**
         *
         */
        abstract void onExit();

        /**
         *
         * @param e
         * @throws IOException
         */
        abstract void onFailure(Throwable e) throws IOException;

        /**
         *
         * @return
         */
        ReplicationLog getReplicationLog() {
            return log;
        }

        void append(Record r) throws IOException {
            getReplicationLog().append(r);
        }

        void sync() throws IOException {
            getReplicationLog().sync();
        }

        void close() {
            if (log != null) {
                log.close();
            }
        }

        void closeOnError() {
            if (log != null) {
                log.closeOnError();
            }
        }

        State getState() {
            return state;
        }

        @Override
        public String toString() {
            return getState().name();
        }
    }

    protected class Init extends ReplicationMode {
        Init() {
            super(INIT);
        }

        @Override
        void onEnter() throws IOException {}

        @Override
        void onExit() {}

        @Override
        void onFailure(Throwable e) throws IOException {
            throw new UnsupportedOperationException("Not supported for " + this);
        }

        @Override
        void append(Record r) throws IOException {
            throw new UnsupportedOperationException("Not supported for " + this);
        }

        @Override
        void sync() throws IOException {
            throw new UnsupportedOperationException("Not supported for " + this);
        }

    }

    protected class Sync extends ReplicationMode {
        Sync() {
            super(SYNC);
        }

        @Override
        public void onEnter() throws IOException {
            log = createRemoteLog();
            log.init();
        }

        @Override
        public void onExit() {
            close();
        }

        @Override
        public void onFailure(Throwable e) throws IOException {
            try {
                LOG.info("{} mode={} got error", haGroupName, this, e);
                haGroupStoreManager.setHAGroupStatusToStoreAndForward(haGroupName);
                switchMode(new StoreAndForward());
            }
            catch (IOException ex) {
                // TODO logging
                throw ex;
            }
            catch (Exception ex) {
                // TODO logging
                throw new IOException(ex);
            }
        }
    }

    protected class StoreAndForward extends ReplicationMode {
        StoreAndForward() {
            super(STORE_AND_FORWARD);
        }

        @Override
        public void onEnter() throws IOException {
            log = createLocalLog();
            log.init();
        }

        @Override
        public void onExit() {
            close();
        }

        @Override
        public void onFailure(Throwable e) throws IOException {
            // TODO logging
            if (e.getCause() instanceof IOException) {
                throw (IOException) e.getCause();
            } else {
                throw new IOException(e.getCause());
            }
        }
    }

    protected class SyncAndForward extends ReplicationMode {
        SyncAndForward() {
            super(SYNC_AND_FORWARD);
        }

        @Override
        public void onEnter() throws IOException {
            log = createRemoteLog();
            log.init();
        }

        @Override
        public void onExit() {
            close();
        }

        @Override
        public void onFailure(Throwable e) throws IOException {
            try {
                LOG.info("{} mode={} got error", haGroupName, this, e);
                haGroupStoreManager.setHAGroupStatusToStoreAndForward(haGroupName);
                switchMode(new StoreAndForward());
            }
            catch (IOException ex) {
                // TODO logging
                throw ex;
            }
            catch (Exception ex) {
                // TODO logging
                throw new IOException(ex);
            }
        }
    }

    private static final ImmutableMap<ReplicationMode.State, EnumSet<ReplicationMode.State>> allowedTransition =
            Maps.immutableEnumMap(ImmutableMap.of(
                    INIT, EnumSet.of(SYNC, STORE_AND_FORWARD),
                    SYNC, EnumSet.of(STORE_AND_FORWARD),
                    STORE_AND_FORWARD, EnumSet.of(SYNC_AND_FORWARD),
                    SYNC_AND_FORWARD, EnumSet.of(SYNC, STORE_AND_FORWARD))
            );

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
        this(conf, serverName, haGroupName, HAGroupStoreManager.getInstance(conf));
    }

    /**
     * Protected constructor for ReplicationLogGroup.
     *
     * @param conf Configuration object
     * @param serverName The server name
     * @param haGroupName The HA Group name
     * @param haGroupStoreManager HA Group Store Manager instance
     */
    protected ReplicationLogGroup(Configuration conf,
                                  ServerName serverName,
                                  String haGroupName,
                                  HAGroupStoreManager haGroupStoreManager) {
        this.conf = conf;
        this.serverName = serverName;
        this.haGroupName = haGroupName;
        this.haGroupStoreManager = haGroupStoreManager;
        this.metrics = createMetricsSource();
        this.mode.set(new Init());
    }

    /**
     * Initialize the ReplicationLogGroup by creating the appropriate writer implementation.
     *
     * @throws IOException if initialization fails
     */
    protected void init() throws IOException {
        initializeReplicationMode();
        // if provided in the config use that else use a derived value
        this.syncTimeoutMs = conf.getLong(ReplicationLogGroup.REPLICATION_LOG_SYNC_TIMEOUT_KEY,
                calculateSyncTimeout());
        initializeDisruptor();
        LOG.info("Started ReplicationLogGroup for HA Group: {}", this);
    }

    /*
     Return the sync timeout in ms
    */
    protected long calculateSyncTimeout() {
        int maxAttempts = conf.getInt(REPLICATION_LOG_SYNC_RETRIES_KEY,
                DEFAULT_REPLICATION_LOG_SYNC_RETRIES) + 1;
        long retryDelayMs = conf.getLong(REPLICATION_LOG_RETRY_DELAY_MS_KEY,
                DEFAULT_REPLICATION_LOG_RETRY_DELAY_MS);
        long wrtiteRpcTimeout = conf.getLong(DFSConfigKeys.DFS_DATANODE_SOCKET_WRITE_TIMEOUT_KEY,
                DEFAULT_HDFS_WRITE_RPC_TIMEOUT_MS);
        long zkTimeoutMs = conf.getLong(ZK_SESSION_TIMEOUT, DEFAULT_ZK_SESSION_TIMEOUT);
        long totalRpcTimeout =  maxAttempts*wrtiteRpcTimeout + (maxAttempts - 1)*retryDelayMs;
        return 2*totalRpcTimeout + zkTimeoutMs;
    }

    protected void initializeReplicationMode() throws IOException {
        //TODO Do we need to read from HA Group store to determine if we should transition from
        // INIT -> SYNC or INIT -> STORE_AND_FORWARD
        switchMode(new Sync());
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
        LogEventHandler eventHandler = new LogEventHandler(getMode());
        disruptor.handleEventsWith(eventHandler);
        LogExceptionHandler exceptionHandler = new LogExceptionHandler();
        disruptor.setDefaultExceptionHandler(exceptionHandler);
        ringBuffer = disruptor.start();
    }

    /**
     * Handles events from the Disruptor, managing batching, writer rotation, and error handling.
     */
    protected class LogEventHandler implements EventHandler<LogEvent> {
        private final List<CompletableFuture<Void>> pendingSyncFutures = new ArrayList<>();
        private ReplicationMode lastMode;

        public LogEventHandler(ReplicationMode mode) {
            this.lastMode = mode;
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
         * @param mode The mode which should process the sync event
         * @param sequence The sequence number of the last processed event
         * @throws IOException if the sync operation fails
         */
        private void processPendingSyncs(ReplicationMode mode, long sequence) throws IOException {
            if (pendingSyncFutures.isEmpty()) {
                return;
            }
            mode.sync();
            // Complete all pending sync futures
            for (CompletableFuture<Void> future : pendingSyncFutures) {
                future.complete(null);
            }
            pendingSyncFutures.clear();
            LOG.info("Sync operation completed successfully up to sequence {}", sequence);
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
        private void onFailure(LogEvent failedEvent,
                               ReplicationMode currentMode,
                               long sequence,
                               IOException e) throws IOException {
            // Send the failed event to the current mode
            // this can potentially trigger a mode switch
            currentMode.onFailure(e);
            // retry the batch
            ReplicationMode newMode = getMode();
            replayBatch(currentMode, newMode);
            // retry the failed event
            replayFailedEvent(failedEvent, newMode, sequence);
        }

        private void replayBatch(ReplicationMode oldMode,
                                 ReplicationMode newMode) throws IOException {
            ReplicationLog oldLog = oldMode.getReplicationLog();
            // first replay all appends which were successfully written to the old log
            // but not yet synced
            for (Record r : oldLog.getCurrentBatch()) {
                newMode.append(r);
            }
        }

        private void replayFailedEvent(LogEvent failedEvent,
                                       ReplicationMode currentMode,
                                       long sequence) throws IOException {
            // now retry the event which failed
            // only need to retry append event since for sync event we have already added the
            // sync event future to the pending future list before the sync event can potentially
            // fail.
            if (failedEvent.type == EVENT_TYPE_DATA) {
                currentMode.append(failedEvent.record);
            }
            processPendingSyncs(currentMode, sequence);
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

            // get the mode we are sending the event to
            ReplicationMode current = getMode();
            try {
                if (current != lastMode) {
                    // some other thread switched the mode on the replication group
                    LOG.info("Mode switched at sequence {} from {} to {}", sequence, lastMode, current);
                    replayBatch(lastMode, current);
                    lastMode = current;
                }
                switch (event.type) {
                    case EVENT_TYPE_DATA:
                        current.append(event.record);
                        // Process any pending syncs at the end of batch.
                        if (endOfBatch) {
                            processPendingSyncs(current, sequence);
                        }
                        return;
                    case EVENT_TYPE_SYNC:
                        // Add this sync future to the pending list
                        // OK, to add the same future multiple times when we rewind the batch
                        // as completing an already completed future is a no-op
                        pendingSyncFutures.add(event.syncFuture);
                        // Process any pending syncs at the end of batch.
                        if (endOfBatch) {
                            processPendingSyncs(current, sequence);
                        }
                        return;
                    default:
                        throw new UnsupportedOperationException("Unknown event type: "
                                + event.type);
                }
            } catch (IOException e) {
                try {
                    LOG.info("Failed to process event at sequence {} on mode {}", sequence, current, e);
                    onFailure(event, current, sequence, e);
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
        ReplicationMode current = getMode();
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
            current.onFailure(new IOException(message, e));
            // gracefully handled the failure, retry the sync
            syncInternal();
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
        getMode().closeOnError();
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
            getMode().close();
            metrics.close();
            LOG.info("Closed ReplicationLogGroup for HA Group: {}", haGroupName);
        }
    }

    /**
     * Switch the replication mode.
     *
     * @param newMode The new replication mode
     * @throws IOException If the mode switch fails
     */
    private synchronized void switchMode(ReplicationMode newMode) throws IOException {
        ReplicationMode current = getMode();
        if (current.getState().equals(newMode.getState())) {
            LOG.info("HA group {} is already in new mode {}", this, newMode);
            return;
        }
        EnumSet<ReplicationMode.State> allowedToStates = allowedTransition.get(current.getState());
        if (allowedToStates == null || !allowedToStates.contains(newMode.getState())) {
            throw new DoNotRetryIOException("Can not transit HA Group " + haGroupName +
                    " mode from " + current + " to " + newMode);
        }
        LOG.info("Attempting to switch replication mode for HA Group: {} from {} to {}",
                this, current, newMode);

        try {
            // exit the current mode
            current.onExit();
            // make the switch
            mode.set(newMode);
            // enter the new mode
            getMode().onEnter();
        } catch (IOException e) {
            try {
                //TODO logging
                getMode().onFailure(e);
            } catch (IOException ex) {
                //TODO logging
                throw ex;
            }
        }
        LOG.info("Switched replication mode for HA Group: {} to {}", this, getMode());
    }

    /** Get the current metrics source for monitoring operations. */
    public MetricsReplicationLogGroupSource getMetrics() {
        return metrics;
    }

    /** Create a new metrics source for monitoring operations. */
    protected MetricsReplicationLogGroupSource createMetricsSource() {
        return new MetricsReplicationLogGroupSourceImpl(haGroupName);
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
        ReplicationLog log = new ReplicationLog(this, remoteURI, REMOTE_DIR);
        return log;
    }

    /** Create the local (store and forward) writer. Mainly for tests. */
    protected ReplicationLog createLocalLog() throws IOException {
        URI localURI = getLogURI(ReplicationLogGroup.REPLICATION_LOCAL_HDFS_URL_KEY);
        ReplicationLog log = new ReplicationLog(this, localURI, LOCAL_DIR);
        return log;
    }

    protected ReplicationMode getMode() {
        return mode.get();
    }

    /** Returns the currently active writer. Mainly for tests. */
    protected ReplicationLog getActiveLog() {
        return getMode().getReplicationLog();
    }
}
