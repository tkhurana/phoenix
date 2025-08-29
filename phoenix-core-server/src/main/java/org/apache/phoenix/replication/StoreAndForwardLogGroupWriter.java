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
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Store-and-forward replication implementation of ReplicationLogGroupWriter.
 * <p>
 * This class is a stub implementation for future store-and-forward replication functionality.
 * Store-and-forward mode is used when the standby cluster is temporarily unavailable - mutations
 * are stored locally and forwarded when connectivity is restored.
 * <p>
 * Currently this is a stub that throws UnsupportedOperationException for the abstract methods.
 * Future implementation will include:
 * <ul>
 *   <li>Local storage of mutations when standby is unavailable</li>
 *   <li>Background forwarding when connectivity is restored</li>
 *   <li>Proper error handling and retry logic</li>
 *   <li>Integration with HA state management</li>
 *   <li>Dual-mode operation: local storage + forwarding</li>
 * </ul>
 */
public class StoreAndForwardLogGroupWriter extends ReplicationLogGroupWriter {

    private static final Logger LOG = LoggerFactory.getLogger(StoreAndForwardLogGroupWriter.class);
    private static final String WRITER = "STORE_AND_FORWARD";

    /**
     * Constructor for StoreAndForwardLogGroupWriter.
     */
    public StoreAndForwardLogGroupWriter(ReplicationLogGroup logGroup) {
        super(logGroup);
        LOG.debug("Created StoreAndForwardLogGroupWriter for HA Group: {}", logGroup);
    }

    @Override
    public String toString() {
        return WRITER;
    }

    @Override
    protected URI getLogURI() throws IOException {
        Configuration conf = logGroup.getConfiguration();
        String fallbackUrlString = conf.get(ReplicationLogGroup.REPLICATION_FALLBACK_HDFS_URL_KEY);
        if (fallbackUrlString == null || fallbackUrlString.trim().isEmpty()) {
            throw new IOException("Fallback HDFS URL not configured: "
                    + ReplicationLogGroup.REPLICATION_FALLBACK_HDFS_URL_KEY);
        }
        try {
            return new URI(fallbackUrlString);
        } catch (URISyntaxException e) {
            throw new IOException("Invalid fallback HDFS URL: " + fallbackUrlString, e);
        }
    }

    @Override
    protected boolean onFailure(List<Record> currentBatch, Throwable reason) throws IOException {
        return false;
    }
}
