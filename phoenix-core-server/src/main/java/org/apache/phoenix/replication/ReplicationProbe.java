package org.apache.phoenix.replication;

import java.io.IOException;

import org.apache.hadoop.fs.FileStatus;

public interface ReplicationProbe {

    void onProbeSuccess(FileStatus stat, long timeTaken);

    void onProbeFailure(IOException ex);
}
