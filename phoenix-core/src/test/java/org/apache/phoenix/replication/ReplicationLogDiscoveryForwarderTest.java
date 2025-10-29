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

import static org.junit.Assert.assertNotNull;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.phoenix.replication.log.LogFileTestUtil;
import org.apache.phoenix.replication.log.LogFileWriter;
import org.apache.phoenix.replication.metrics.MetricsReplicationLogTracker;
import org.apache.phoenix.replication.metrics.MetricsReplicationLogTrackerReplayImpl;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.InOrder;
import org.mockito.Mockito;

public class ReplicationLogDiscoveryForwarderTest extends ReplicationLogBaseTest {


    private static final MetricsReplicationLogTracker METRICS_REPLICATION_LOG_TRACKER =
            new MetricsReplicationLogTrackerReplayImpl(HA_GROUP_NAME);

    @Before
    public void setUp() throws IOException {
        ReplicationMode mode = logGroup.getMode();
        Assert.assertTrue(mode.getState().equals(ReplicationMode.State.SYNC));
        ReplicationLog standbyLog = mode.getReplicationLog();
        // set the mode to STORE_AND_FORWARD
        logGroup.switchMode(new StoreAndForwardMode(logGroup,
                standbyLog.getFileSystem(), standbyLog.getHAGroupLogFilesDir()));
    }

    @After
    public void tearDown() throws IOException {

    }

    @Test
    public void testLogForwarding() throws Exception {
        final String tableName = "TESTTBL";
        final long count = 100L;
        for (long id = 1; id <=count; ++id) {
            Mutation put = LogFileTestUtil.newPut("row_" + id, id, 2);
            logGroup.append(tableName, id, put);
        }
        logGroup.sync();
        Thread.sleep(120000);
        System.out.println("waking");
    }

    private TestableReplicationLogTracker createReplicationLogTracker(final Configuration conf, final String haGroupName, final FileSystem fileSystem, final URI rootURI) throws IOException {
        TestableReplicationLogTracker testableReplicationLogTracker = new TestableReplicationLogTracker(conf, haGroupName, fileSystem, rootURI, ReplicationLogTracker.DirectoryType.IN, METRICS_REPLICATION_LOG_TRACKER);
        testableReplicationLogTracker.init();
        return testableReplicationLogTracker;
    }

    /**
     * Testable implementation of ReplicationLogTracker for unit testing.
     * Exposes protected methods to allow test access.
     */
    private class TestableReplicationLogTracker extends ReplicationLogTracker {
        public TestableReplicationLogTracker(Configuration conf, String haGroupName, FileSystem fileSystem, URI rootURI, DirectoryType directoryType, MetricsReplicationLogTracker metrics) {
            super(conf, haGroupName, fileSystem, rootURI, directoryType, metrics);
        }
        public Path getInProgressDirPath() {
            return super.getInProgressDirPath();
        }
    }
    
    /**
     * Testable implementation of ReplicationLogDiscoveryReplay for unit testing.
     * Provides dependency injection for HAGroupStoreRecord, tracks processed rounds,
     * and supports simulating state changes during replay.
     */
    //private static class TestableReplicationLogDiscoveryForwarder extends ReplicationLogDiscoveryForwarder
}
