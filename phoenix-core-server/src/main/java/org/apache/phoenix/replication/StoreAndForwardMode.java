package org.apache.phoenix.replication;

import java.io.IOException;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.phoenix.replication.metrics.MetricsReplicationLogForwarderSourceFactory;
import org.apache.phoenix.replication.metrics.MetricsReplicationLogTrackerForwarderImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class StoreAndForwardMode extends ReplicationMode implements ReplicationProbe {
    private static final Logger LOG = LoggerFactory.getLogger(StoreAndForwardMode.class);

    private final FileSystem standbyFS;
    private final Path standbyLogFilesPath;
    private ReplicationLogDiscoveryForwarder forwarder;

    protected StoreAndForwardMode(ReplicationLogGroup logGroup,
                                  FileSystem standbyFS,
                                  Path standbyLogFilesPath) {
        super(logGroup, State.STORE_AND_FORWARD);
        // remote filesystem
        this.standbyFS = standbyFS;
        // directory on the remote filesystem where the local logs will be forwarded to
        this.standbyLogFilesPath = standbyLogFilesPath;
    }

    @Override
    void onEnter() throws IOException {
        log = logGroup.createLocalLog();
        log.init();
        ReplicationShardDirectoryManager localShardDirectoryManager = log.getReplicationShardDirectoryManager();
        ReplicationLogTracker localLogTracker = new ReplicationLogTracker(
                logGroup.conf,
                logGroup.getHaGroupName(),
                log.getFileSystem(),
                log.logURI, // local log
                ReplicationLogTracker.DirectoryType.OUT,
                MetricsReplicationLogForwarderSourceFactory.
                        getInstanceForTracker(logGroup.getHaGroupName()));
        localLogTracker.init();
        forwarder = new ReplicationLogDiscoveryForwarder(
                localLogTracker,
                logGroup.getServerName().getServerName(),
                standbyFS,
                standbyLogFilesPath,
                this);
        forwarder.init();
        forwarder.start();
    }

    @Override
    void onExit() {
        close();
    }

    @Override
    void onFailure(Throwable e) throws IOException {
        // TODO logging
        if (e.getCause() instanceof IOException) {
            throw (IOException) e.getCause();
        } else {
            throw new IOException(e.getCause());
        }
    }

    @Override
    public void onProbeSuccess(FileStatus stat, long timeTaken) {
        // calculate throughput in bytes/sec
        //double throughput = stat.getLen()/(timeTaken/1000);
        // TODO Check if the throughput is acceptable
        try {
            logGroup.switchMode(new SyncAndForwardMode(logGroup, forwarder.replicationLogTracker));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void onProbeFailure(IOException ex) {

    }
}
