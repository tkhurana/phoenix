/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.phoenix.replication;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;

import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.ServerName;
import org.apache.phoenix.jdbc.HAGroupStoreManager;
import org.apache.phoenix.replication.log.LogFileWriter;
import org.apache.phoenix.util.EnvironmentEdgeManager;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.rules.TemporaryFolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ReplicationLogBaseTest {

    private static final Logger LOG = LoggerFactory.getLogger(ReplicationLogBaseTest.class);

    @ClassRule
    public static TemporaryFolder standbyFolder = new TemporaryFolder();
    @ClassRule
    public static TemporaryFolder fallbackFolder = new TemporaryFolder();

    protected Configuration conf;
    protected ServerName serverName;
    protected FileSystem localFs;
    protected URI remoteUri;
    protected URI localUri;
    protected ReplicationLogGroup logGroup;

    static final int TEST_RINGBUFFER_SIZE = 32;
    static final int TEST_SYNC_TIMEOUT = 1000;
    static final int TEST_ROTATION_TIME = 5000;
    static final int TEST_ROTATION_SIZE_BYTES = 10 * 1024;
    static final String HA_GROUP_NAME = "testHAGroup";

    @Before
    public void setUpBase() throws IOException {
        conf = HBaseConfiguration.create();
        localFs = FileSystem.getLocal(conf);
        remoteUri = new Path(standbyFolder.getRoot().toString()).toUri();
        localUri = new Path(fallbackFolder.getRoot().toString()).toUri();
        serverName = ServerName.valueOf("test", 60010, EnvironmentEdgeManager.currentTimeMillis());
        conf.set(ReplicationLogGroup.REPLICATION_REMOTE_HDFS_URL_KEY, remoteUri.toString());
        conf.set(ReplicationLogGroup.REPLICATION_LOCAL_HDFS_URL_KEY, localUri.toString());
        // Small ring buffer size for testing
        conf.setInt(ReplicationLogGroup.REPLICATION_LOG_RINGBUFFER_SIZE_KEY, TEST_RINGBUFFER_SIZE);
        // Set a short sync timeout for testing
        conf.setLong(ReplicationLogGroup.REPLICATION_LOG_SYNC_TIMEOUT_KEY, TEST_SYNC_TIMEOUT);
        // Set rotation time to 10 seconds
        conf.setLong(ReplicationLogGroup.REPLICATION_LOG_ROTATION_TIME_MS_KEY, TEST_ROTATION_TIME);
        // Small size threshold for testing
        conf.setLong(ReplicationLogGroup.REPLICATION_LOG_ROTATION_SIZE_BYTES_KEY,
            TEST_ROTATION_SIZE_BYTES);

        TestableHAGroupStoreManager mock = mock(TestableHAGroupStoreManager.class);
        logGroup = new TestableLogGroup(conf, serverName, HA_GROUP_NAME,
                remoteUri, localUri, mock);
        logGroup.init();
    }

    @After
    public void tearDown() throws Exception {
        if (logGroup != null) {
            logGroup.close();
        }
    }

    static class TestableLogGroup extends ReplicationLogGroup {
        private final URI remoteUri;
        private final URI localUri;

        public TestableLogGroup(Configuration conf,
                                ServerName serverName,
                                String haGroupName,
                                URI remoteUri,
                                URI localUri,
                                HAGroupStoreManager haGroupStoreManager) {
            super(conf, serverName, haGroupName, haGroupStoreManager);
            this.remoteUri = remoteUri;
            this.localUri = localUri;
        }

        @Override
        protected ReplicationLog createRemoteLog() throws IOException {
            return spy(new TestableLog(this, remoteUri, ReplicationLogGroup.REMOTE_DIR));
        }

        @Override
        protected ReplicationLog createLocalLog() throws IOException {
            return spy(new TestableLog(this, localUri, ReplicationLogGroup.LOCAL_DIR));
        }

    }

    /**
     * Testable version of ReplicationLog that allows spying on the log
     */
    static class TestableLog extends ReplicationLog {

        public TestableLog(ReplicationLogGroup logGroup, URI uri, String logDirName) {
            super(logGroup, uri, logDirName);
        }

        @Override
        protected LogFileWriter createNewWriter() throws IOException {
            LogFileWriter writer = super.createNewWriter();
            return spy(writer);
        }
    }

    /**
     *
     */
    static class TestableHAGroupStoreManager extends HAGroupStoreManager {

        public TestableHAGroupStoreManager(Configuration conf) {
            super(conf);
        }
    }
}
