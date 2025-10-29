package org.apache.phoenix.replication;

import java.io.IOException;

import org.apache.hadoop.fs.FileStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SyncAndForwardMode extends ReplicationMode implements ReplicationProbe{
    private static final Logger LOG = LoggerFactory.getLogger(SyncAndForwardMode.class);

    private final ReplicationLogTracker localLogTracker;
    private ReplicationLogDiscoveryForwarder forwarder;

    protected SyncAndForwardMode(ReplicationLogGroup logGroup,
                                 ReplicationLogTracker localLogTracker) {
        super(logGroup, State.SYNC_AND_FORWARD);
        this.localLogTracker = localLogTracker;
    }

    @Override
    void onEnter() throws IOException {
        log = logGroup.createRemoteLog();
        log.init();
        forwarder = new ReplicationLogDiscoveryForwarder(
                localLogTracker,
                logGroup.getServerName().getServerName(),
                log.getFileSystem(),
                log.getHAGroupLogFilesDir(),
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
        LOG.info("{} mode={} got error", logGroup, this, e);
        logGroup.setHAGroupStatusToStoreAndForward();
        logGroup.switchMode(new StoreAndForwardMode(logGroup,
                log.getFileSystem(), log.getHAGroupLogFilesDir()));
    }

    @Override
    public void onProbeSuccess(FileStatus stat, long timeTaken) {

    }

    @Override
    public void onProbeFailure(IOException ex) {
        try {
            onFailure(ex);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
