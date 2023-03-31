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

package org.apache.flink.connector.mongodb.sink;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.state.CheckpointListener;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.mongodb.sink.writer.context.MongoSinkContext;
import org.apache.flink.connector.mongodb.sink.writer.serializer.MongoSerializationSchema;
import org.apache.flink.connector.mongodb.testutils.MongoTestUtil;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.test.junit5.MiniClusterExtension;
import org.apache.flink.testutils.junit.SharedObjectsExtension;
import org.apache.flink.testutils.junit.SharedReference;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.UpdateOneModel;
import com.mongodb.client.model.UpdateOptions;
import com.mongodb.client.model.WriteModel;
import org.bson.BsonDocument;
import org.bson.Document;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.MongoDBContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.util.concurrent.atomic.AtomicBoolean;

import static org.apache.flink.connector.mongodb.testutils.MongoTestUtil.assertThatIdsAreWritten;
import static org.assertj.core.api.Assertions.assertThat;

/** IT cases for {@link MongoSink}. */
@Testcontainers
public class MongoSinkITCase {

    private static final Logger LOG = LoggerFactory.getLogger(MongoSinkITCase.class);

    @Container
    private static final MongoDBContainer MONGO_CONTAINER =
            MongoTestUtil.createMongoDBContainer(LOG);

    @RegisterExtension
    static final MiniClusterExtension MINI_CLUSTER_RESOURCE =
            new MiniClusterExtension(
                    new MiniClusterResourceConfiguration.Builder()
                            .setNumberTaskManagers(1)
                            .build());

    @RegisterExtension final SharedObjectsExtension sharedObjects = SharedObjectsExtension.create();

    private static final String TEST_DATABASE = "test_sink";

    private static MongoClient mongoClient;

    @BeforeAll
    static void setUp() {
        mongoClient = MongoClients.create(MONGO_CONTAINER.getConnectionString());
    }

    @AfterAll
    static void tearDown() {
        if (mongoClient != null) {
            mongoClient.close();
        }
    }

    @ParameterizedTest
    @EnumSource(
            value = DeliveryGuarantee.class,
            mode = EnumSource.Mode.EXCLUDE,
            names = "EXACTLY_ONCE")
    void testWriteToMongoWithDeliveryGuarantee(DeliveryGuarantee deliveryGuarantee)
            throws Exception {
        final String collection = "test-sink-with-delivery-" + deliveryGuarantee;
        final MongoSink<Document> sink = createSink(collection, deliveryGuarantee);
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(100L);
        env.setRestartStrategy(RestartStrategies.noRestart());

        env.fromSequence(1, 5).map(new TestMapFunction()).sinkTo(sink);
        env.execute();
        assertThatIdsAreWritten(collectionOf(collection), 1, 2, 3, 4, 5);
    }

    @Test
    void testRecovery() throws Exception {
        final String collection = "test-recovery-mongo-sink";
        final MongoSink<Document> sink = createSink(collection, DeliveryGuarantee.AT_LEAST_ONCE);

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(100L);

        final SharedReference<AtomicBoolean> failed = sharedObjects.add(new AtomicBoolean(false));

        env.fromSequence(1, 5)
                .map(new FailingMapper(failed))
                .map(new TestMapFunction())
                .sinkTo(sink);

        env.execute();
        assertThatIdsAreWritten(collectionOf(collection), 1, 2, 3, 4, 5);
        assertThat(failed.get()).isTrue();
    }

    private static MongoSink<Document> createSink(
            String collection, DeliveryGuarantee deliveryGuarantee) {
        return MongoSink.<Document>builder()
                .setUri(MONGO_CONTAINER.getConnectionString())
                .setDatabase(TEST_DATABASE)
                .setCollection(collection)
                .setBatchSize(5)
                .setDeliveryGuarantee(deliveryGuarantee)
                .setSerializationSchema(new UpsertSerializationSchema())
                .build();
    }

    private static MongoCollection<Document> collectionOf(String collection) {
        return mongoClient.getDatabase(TEST_DATABASE).getCollection(collection);
    }

    private static Document buildMessage(int id) {
        return new Document("_id", id).append("f1", "d_" + id);
    }

    private static class TestMapFunction implements MapFunction<Long, Document> {
        @Override
        public Document map(Long value) {
            return buildMessage(value.intValue());
        }
    }

    private static class UpsertSerializationSchema implements MongoSerializationSchema<Document> {
        @Override
        public WriteModel<BsonDocument> serialize(Document element, MongoSinkContext sinkContext) {
            BsonDocument document = element.toBsonDocument();
            BsonDocument filter = new BsonDocument("_id", document.get("_id"));
            // _id is immutable so we remove it here to prevent exception.
            document.remove("_id");
            BsonDocument update = new BsonDocument("$set", document);
            return new UpdateOneModel<>(filter, update, new UpdateOptions().upsert(true));
        }
    }

    private static class FailingMapper implements MapFunction<Long, Long>, CheckpointListener {

        private final SharedReference<AtomicBoolean> failed;
        private int emittedRecords = 0;

        private FailingMapper(SharedReference<AtomicBoolean> failed) {
            this.failed = failed;
        }

        @Override
        public Long map(Long value) throws Exception {
            Thread.sleep(50);
            emittedRecords++;
            return value;
        }

        @Override
        public void notifyCheckpointComplete(long checkpointId) throws Exception {
            if (failed.get().get() || emittedRecords == 0) {
                return;
            }
            failed.get().set(true);
            throw new Exception("Expected failure");
        }
    }
}
