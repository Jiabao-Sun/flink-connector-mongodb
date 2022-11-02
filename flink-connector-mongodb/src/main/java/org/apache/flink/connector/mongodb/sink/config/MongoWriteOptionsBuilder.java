/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *   http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.flink.connector.mongodb.sink.config;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.connector.base.DeliveryGuarantee;

import static org.apache.flink.connector.mongodb.table.MongoConnectorOptions.BULK_FLUSH_INTERVAL;
import static org.apache.flink.connector.mongodb.table.MongoConnectorOptions.BULK_FLUSH_MAX_ACTIONS;
import static org.apache.flink.connector.mongodb.table.MongoConnectorOptions.SINK_MAX_RETRIES;
import static org.apache.flink.connector.mongodb.table.MongoConnectorOptions.SINK_RETRY_INTERVAL;
import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

/** Builder for {@link MongoWriteOptions}. */
@PublicEvolving
public class MongoWriteOptionsBuilder {

    private int bulkFlushMaxActions = BULK_FLUSH_MAX_ACTIONS.defaultValue();
    private long bulkFlushIntervalMs = BULK_FLUSH_INTERVAL.defaultValue().toMillis();
    private int maxRetryTimes = SINK_MAX_RETRIES.defaultValue();
    private long retryIntervalMs = SINK_RETRY_INTERVAL.defaultValue().toMillis();
    private DeliveryGuarantee deliveryGuarantee = DeliveryGuarantee.AT_LEAST_ONCE;
    private Integer parallelism;

    /**
     * Sets the maximum number of actions to buffer for each bulk request. You can pass -1 to
     * disable it. The default flush size 1000.
     *
     * @param numMaxActions the maximum number of actions to buffer per bulk request.
     * @return this builder
     */
    public MongoWriteOptionsBuilder setBulkFlushMaxActions(int numMaxActions) {
        checkArgument(
                numMaxActions == -1 || numMaxActions > 0,
                "Max number of buffered actions must be larger than 0.");
        this.bulkFlushMaxActions = numMaxActions;
        return this;
    }

    /**
     * Sets the bulk flush interval, in milliseconds. You can pass -1 to disable it.
     *
     * @param intervalMillis the bulk flush interval, in milliseconds.
     * @return this builder
     */
    public MongoWriteOptionsBuilder setBulkFlushIntervalMs(long intervalMillis) {
        checkArgument(
                intervalMillis == -1 || intervalMillis >= 0,
                "Interval (in milliseconds) between each flush must be larger than "
                        + "or equal to 0.");
        this.bulkFlushIntervalMs = intervalMillis;
        return this;
    }

    /**
     * Sets the max retry times if writing records failed.
     *
     * @param maxRetryTimes the max retry times.
     * @return this builder
     */
    public MongoWriteOptionsBuilder setMaxRetryTimes(int maxRetryTimes) {
        checkArgument(
                maxRetryTimes >= 0, "The sink max retry times must be larger than or equal to 0.");
        this.maxRetryTimes = maxRetryTimes;
        return this;
    }

    /**
     * Sets the retry interval if writing records to database failed.
     *
     * @param retryIntervalMs the retry time interval, in milliseconds.
     * @return this builder
     */
    public MongoWriteOptionsBuilder setRetryInterval(long retryIntervalMs) {
        checkArgument(
                retryIntervalMs > 0, "The retry interval (in milliseconds) must be larger than 0.");
        this.retryIntervalMs = retryIntervalMs;
        return this;
    }

    /**
     * Sets the wanted {@link DeliveryGuarantee}. The default delivery guarantee is {@link
     * DeliveryGuarantee#AT_LEAST_ONCE}
     *
     * @param deliveryGuarantee which describes the record emission behaviour
     * @return this builder
     */
    public MongoWriteOptionsBuilder setDeliveryGuarantee(DeliveryGuarantee deliveryGuarantee) {
        checkArgument(
                deliveryGuarantee != DeliveryGuarantee.EXACTLY_ONCE,
                "Mongo sink does not support the EXACTLY_ONCE guarantee.");
        this.deliveryGuarantee = checkNotNull(deliveryGuarantee);
        return this;
    }

    /**
     * Sets the parallelism of the Mongo sink operator. By default, the parallelism is determined by
     * the framework using the same parallelism of the upstream chained operator.
     */
    public MongoWriteOptionsBuilder setParallelism(int parallelism) {
        checkArgument(parallelism > 0, "Mongo sink parallelism must be larger than 0.");
        this.parallelism = parallelism;
        return this;
    }

    /**
     * Build the {@link MongoWriteOptions}.
     *
     * @return a MongoWriteOptions with the settings made for this builder.
     */
    public MongoWriteOptions build() {
        return new MongoWriteOptions(
                bulkFlushMaxActions,
                bulkFlushIntervalMs,
                maxRetryTimes,
                retryIntervalMs,
                deliveryGuarantee,
                parallelism);
    }
}
