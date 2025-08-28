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

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.phoenix.replication.log.LogFileWriter;
import org.apache.phoenix.replication.log.LogFileWriterContext;
import org.apache.phoenix.replication.reader.ReplicationLogReplayFileTracker;
import org.apache.phoenix.replication.reader.ReplicationReplayLogDiscovery;
import org.apache.phoenix.util.EnvironmentEdgeManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Synchronous replication implementation of ReplicationLogGroupWriter.
 * <p>
 * This class implements synchronous replication to a standby cluster's HDFS. It writes replication
 * logs directly to the standby cluster in synchronous mode, providing immediate consistency for
 * failover scenarios.
 */
public class StandbyLogGroupWriter extends ReplicationLogGroupWriter {

    private static final Logger LOG = LoggerFactory.getLogger(StandbyLogGroupWriter.class);
    private static final String WRITER = "STANDBY";

    /**
     * Constructor for StandbyLogGroupWriter.
     */
    public StandbyLogGroupWriter(ReplicationLogGroup logGroup) {
        super(logGroup);
        LOG.debug("Created StandbyLogGroupWriter for HA Group: {}", logGroup);
    }

    @Override
    public String toString() {
        return WRITER;
    }

    @Override
    protected URI getLogURI() throws IOException {
        Configuration conf = logGroup.getConfiguration();
        String standbyUrlString = conf.get(ReplicationLogGroup.REPLICATION_STANDBY_HDFS_URL_KEY);
        if (standbyUrlString == null || standbyUrlString.trim().isEmpty()) {
            throw new IOException("Standby HDFS URL not configured: "
                    + ReplicationLogGroup.REPLICATION_STANDBY_HDFS_URL_KEY);
        }
        try {
            return new URI(standbyUrlString);
        } catch (URISyntaxException e) {
            throw new IOException("Invalid standby HDFS URL: " + standbyUrlString, e);
        }
    }
}
