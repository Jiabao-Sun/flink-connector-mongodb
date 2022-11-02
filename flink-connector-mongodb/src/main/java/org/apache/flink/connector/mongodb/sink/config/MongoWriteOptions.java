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

package org.apache.flink.connector.mongodb.sink.config;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.connector.base.DeliveryGuarantee;

import javax.annotation.Nullable;

import java.io.Serializable;
import java.util.Objects;

/**
 * Configurations for MongoSink to control write operations. All the options list here could be
 * configured by {@link MongoWriteOptionsBuilder}.
 */
@PublicEvolving
public final class MongoWriteOptions implements Serializable {

    private static final long serialVersionUID = 1L;

    private final int bulkFlushMaxActions;
    private final long bulkFlushIntervalMs;
    private final int maxRetryTimes;
    private final long retryIntervalMs;
    private final DeliveryGuarantee deliveryGuarantee;
    private final Integer parallelism;

    MongoWriteOptions(
            int bulkFlushMaxActions,
            long bulkFlushIntervalMs,
            int maxRetryTimes,
            long retryIntervalMs,
            DeliveryGuarantee deliveryGuarantee,
            @Nullable Integer parallelism) {
        this.bulkFlushMaxActions = bulkFlushMaxActions;
        this.bulkFlushIntervalMs = bulkFlushIntervalMs;
        this.maxRetryTimes = maxRetryTimes;
        this.retryIntervalMs = retryIntervalMs;
        this.deliveryGuarantee = deliveryGuarantee;
        this.parallelism = parallelism;
    }

    public int getBulkFlushMaxActions() {
        return bulkFlushMaxActions;
    }

    public long getBulkFlushIntervalMs() {
        return bulkFlushIntervalMs;
    }

    public int getMaxRetryTimes() {
        return maxRetryTimes;
    }

    public long getRetryIntervalMs() {
        return retryIntervalMs;
    }

    public DeliveryGuarantee getDeliveryGuarantee() {
        return deliveryGuarantee;
    }

    @Nullable
    public Integer getParallelism() {
        return parallelism;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        MongoWriteOptions that = (MongoWriteOptions) o;
        return bulkFlushMaxActions == that.bulkFlushMaxActions
                && bulkFlushIntervalMs == that.bulkFlushIntervalMs
                && maxRetryTimes == that.maxRetryTimes
                && retryIntervalMs == that.retryIntervalMs
                && deliveryGuarantee == that.deliveryGuarantee
                && Objects.equals(parallelism, that.parallelism);
    }

    @Override
    public int hashCode() {
        return Objects.hash(
                bulkFlushMaxActions,
                bulkFlushIntervalMs,
                maxRetryTimes,
                retryIntervalMs,
                deliveryGuarantee,
                parallelism);
    }

    public static MongoWriteOptionsBuilder builder() {
        return new MongoWriteOptionsBuilder();
    }
}
