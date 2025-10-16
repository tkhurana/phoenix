package org.apache.phoenix.replication;

import java.io.IOException;

/**
 * Tracks the current replication mode of the ReplicationLogGroup.
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
public abstract class ReplicationMode {
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

    private final ReplicationLogGroup.ReplicationMode.State state;

    // The mode manages the underlying log to which the append and sync events will be sent
    protected ReplicationLog log;

    protected ReplicationMode(ReplicationLogGroup.ReplicationMode.State state) {
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

    void append(ReplicationLogGroup.Record r) throws IOException {
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

    ReplicationLogGroup.ReplicationMode.State getState() {
        return state;
    }

    @Override
    public String toString() {
        return getState().name();
    }
}
