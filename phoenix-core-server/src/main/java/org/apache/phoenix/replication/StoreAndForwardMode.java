package org.apache.phoenix.replication;

import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class StoreAndForwardMode extends ReplicationMode {
    private static final Logger LOG = LoggerFactory.getLogger(StoreAndForwardMode.class);

    protected StoreAndForwardMode(ReplicationLogGroup logGroup) {
        super(logGroup, State.STORE_AND_FORWARD);
    }

    @Override
    void onEnter() throws IOException {
        log = logGroup.createLocalLog();
        log.init();
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
}
