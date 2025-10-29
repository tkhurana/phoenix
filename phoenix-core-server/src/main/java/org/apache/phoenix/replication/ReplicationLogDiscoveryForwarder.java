package org.apache.phoenix.replication;

import java.io.IOException;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.phoenix.replication.metrics.MetricsReplicationLogDiscovery;
import org.apache.phoenix.replication.metrics.MetricsReplicationLogForwarderSourceFactory;
import org.apache.phoenix.util.EnvironmentEdgeManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ReplicationLogDiscoveryForwarder extends ReplicationLogDiscovery {
    private static final Logger LOG = LoggerFactory.getLogger(ReplicationLogDiscoveryForwarder.class);

    private final String serverName;
    private final FileSystem standbyFS;
    private final ReplicationShardDirectoryManager standbyShardDirectoryManager;
    private final ReplicationProbe probeHandler;

    public ReplicationLogDiscoveryForwarder(ReplicationLogTracker replicationLogTracker,
                                            String serverName,
                                            FileSystem standbyFS,
                                            Path standbyLogFilesDir,
                                            ReplicationProbe probeHandler) {
        super(replicationLogTracker);
        this.serverName = serverName;
        this.standbyFS = standbyFS;
        this.standbyShardDirectoryManager = new ReplicationShardDirectoryManager(conf, standbyLogFilesDir);
        this.probeHandler = probeHandler;
    }

    @Override
    protected void processFile(Path src) throws IOException {
        FileSystem srcFS = replicationLogTracker.getFileSystem();
        FileStatus srcStat = srcFS.getFileStatus(src);
        long ts = EnvironmentEdgeManager.currentTimeMillis();
        Path dstDir = standbyShardDirectoryManager.getShardDirectory(ts);
        Path dst = standbyShardDirectoryManager.getLogFilePathInShardDirectory(
                dstDir, ts, serverName);
        long startTime = EnvironmentEdgeManager.currentTimeMillis();
        FileUtil.copy(srcFS, srcStat, standbyFS, dst, false, false, conf);
        // successfully copied the file
        long endTime = EnvironmentEdgeManager.currentTimeMillis();
        long copyTime = endTime - startTime;
        LOG.info("Copying file src={} dst={} size={} took {}ms", src, dst, srcStat.getLen(), copyTime);
        probeHandler.onProbeSuccess(srcStat, copyTime);
    }

    @Override
    protected MetricsReplicationLogDiscovery createMetricsSource() {
        return MetricsReplicationLogForwarderSourceFactory.
                getInstanceForDiscovery(replicationLogTracker.getHaGroupName());
    }
}
