package org.apache.phoenix.replication;

import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SyncAndForwardMode extends ReplicationMode {
    private static final Logger LOG = LoggerFactory.getLogger(SyncAndForwardMode.class);

    protected SyncAndForwardMode(ReplicationLogGroup logGroup) {
        super(logGroup, State.SYNC_AND_FORWARD);
    }

    @Override
    void onEnter() throws IOException {
        log = logGroup.createRemoteLog();
        log.init();
    }

    @Override
    void onExit() {
        close();
    }

    @Override
    void onFailure(Throwable e) throws IOException {
        LOG.info("{} mode={} got error", logGroup, this, e);
        logGroup.setHAGroupStatusToStoreAndForward();
        logGroup.switchMode(new StoreAndForwardMode(logGroup));
    }
}
