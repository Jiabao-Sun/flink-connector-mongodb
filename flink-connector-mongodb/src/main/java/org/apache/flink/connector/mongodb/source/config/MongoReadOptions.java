/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.connector.mongodb.source.config;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.connector.mongodb.source.enumerator.splitter.PartitionStrategy;

import java.io.Serializable;
import java.util.Objects;

/** The configuration class for MongoDB source. */
@PublicEvolving
public class MongoReadOptions implements Serializable {

    private static final long serialVersionUID = 1L;

    private final int fetchSize;

    private final int cursorBatchSize;

    private final boolean noCursorTimeout;

    private final PartitionStrategy partitionStrategy;

    private final MemorySize partitionSize;

    private final int samplesPerPartition;

    MongoReadOptions(
            int fetchSize,
            int cursorBatchSize,
            boolean noCursorTimeout,
            PartitionStrategy partitionStrategy,
            MemorySize partitionSize,
            int samplesPerPartition) {
        this.fetchSize = fetchSize;
        this.cursorBatchSize = cursorBatchSize;
        this.noCursorTimeout = noCursorTimeout;
        this.partitionStrategy = partitionStrategy;
        this.partitionSize = partitionSize;
        this.samplesPerPartition = samplesPerPartition;
    }

    public int getFetchSize() {
        return fetchSize;
    }

    public int getCursorBatchSize() {
        return cursorBatchSize;
    }

    public boolean isNoCursorTimeout() {
        return noCursorTimeout;
    }

    public PartitionStrategy getPartitionStrategy() {
        return partitionStrategy;
    }

    public MemorySize getPartitionSize() {
        return partitionSize;
    }

    public int getSamplesPerPartition() {
        return samplesPerPartition;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        MongoReadOptions that = (MongoReadOptions) o;
        return cursorBatchSize == that.cursorBatchSize
                && noCursorTimeout == that.noCursorTimeout
                && partitionStrategy == that.partitionStrategy
                && samplesPerPartition == that.samplesPerPartition
                && Objects.equals(partitionSize, that.partitionSize);
    }

    @Override
    public int hashCode() {
        return Objects.hash(
                cursorBatchSize,
                noCursorTimeout,
                partitionStrategy,
                partitionSize,
                samplesPerPartition);
    }

    public static MongoReadOptionsBuilder builder() {
        return new MongoReadOptionsBuilder();
    }
}
