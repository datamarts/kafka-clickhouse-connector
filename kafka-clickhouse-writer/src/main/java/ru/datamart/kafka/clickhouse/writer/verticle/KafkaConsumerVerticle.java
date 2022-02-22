/*
 * Copyright © 2022 DATAMART LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package ru.datamart.kafka.clickhouse.writer.verticle;

import ru.datamart.kafka.clickhouse.avro.codec.AvroDecoder;
import ru.datamart.kafka.clickhouse.writer.configuration.properties.VerticleProperties;
import ru.datamart.kafka.clickhouse.writer.factory.InsertRequestFactory;
import ru.datamart.kafka.clickhouse.writer.model.DataTopic;
import ru.datamart.kafka.clickhouse.writer.model.InsertDataContext;
import ru.datamart.kafka.clickhouse.writer.model.kafka.Chunk;
import ru.datamart.kafka.clickhouse.writer.model.kafka.InsertChunk;
import ru.datamart.kafka.clickhouse.writer.model.kafka.PartitionOffset;
import ru.datamart.kafka.clickhouse.writer.model.kafka.TopicPartitionConsumer;
import ru.datamart.kafka.clickhouse.writer.model.sql.ClickhouseInsertSqlRequest;
import ru.datamart.kafka.clickhouse.writer.service.kafka.KafkaConsumerService;
import io.vertx.core.DeploymentOptions;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.kafka.client.common.PartitionInfo;
import io.vertx.kafka.client.common.TopicPartition;
import io.vertx.kafka.client.consumer.KafkaConsumerRecord;
import lombok.Builder;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import lombok.val;

import java.util.*;
import java.util.concurrent.atomic.AtomicLong;


@Slf4j
@Getter
@Builder
public class KafkaConsumerVerticle extends ConfigurableVerticle {
    public static final String START_TOPIC = "kafka_consumer_start";
    public static final String KAFKA_COMMIT_TOPIC = "kafka_consumer_offset_commit";
    private final AvroDecoder decoder = new AvroDecoder();
    private final VerticleProperties.KafkaConsumerWorkerProperties workerProperties;
    private final InsertRequestFactory insertRequestFactory;
    private final String id = UUID.randomUUID().toString();
    private final KafkaConsumerService consumerService;
    private final PartitionInfo partitionInfo;
    private final InsertDataContext context;
    private final Queue<InsertChunk> insertChunkQueue;
    private final Map<Long, InsertChunk> insertChunks = new HashMap<>();
    private final AtomicLong lastOffset = new AtomicLong(-1);
    private final HashMap<TopicPartition, TopicPartitionConsumer> consumerMap;
    private volatile TopicPartitionConsumer topicPartitionConsumer;
    private volatile boolean stopped;
    private Future<Object> processFuture;

    @Override
    public void start(Future<Void> startFuture) throws Exception {
        super.start(startFuture);
        vertx.eventBus().consumer(START_TOPIC + context.getContextId(),
                ar -> getTopicPartitionConsumerFuture());
    }

    private void getTopicPartitionConsumerFuture() {
        processFuture = Future.succeededFuture();
        consumerService.createTopicPartitionConsumer(context, partitionInfo)
                .onSuccess(consumer -> {
                    if (stopped) {
                        consumer.getKafkaConsumer().close();
                        return;
                    }

                    setConsumer(consumer);
                    lastOffset.set(topicPartitionConsumer.getLastOffset());
                    consumer.getKafkaConsumer().handler(record ->
                            processFuture = processFuture
                                    .compose(v -> parseChunk(record))
                                    .compose(chunk -> insertChunk(context, chunk))
                                    .onFailure(error -> error(context, error)));
                });
    }

    private Future<Chunk> parseChunk(KafkaConsumerRecord<byte[], byte[]> record) {
        return Future.future((Promise<Chunk> p) -> {
            if (record.value() == null || record.value().length == 0) {
                p.complete(Chunk.builder()
                        .topicPartition(new TopicPartition(record.topic(), record.partition()))
                        .offset(record.offset())
                        .rows(Collections.emptyList())
                        .build());
            } else {
                p.complete(Chunk.builder()
                        .topicPartition(new TopicPartition(record.topic(), record.partition()))
                        .offset(record.offset())
                        .rows(decoder.decode(record.value()))
                        .build());
            }
        });
    }

    private void setConsumer(TopicPartitionConsumer consumer) {
        topicPartitionConsumer = consumer;
        consumerMap.put(consumer.getTopicPartition(), consumer);
    }

    @Override
    public void stop() {
        stopped = true;
        if (topicPartitionConsumer != null) {
            topicPartitionConsumer.getKafkaConsumer().handler(null);
            topicPartitionConsumer.getKafkaConsumer().close();
        }
    }

    private Future<Object> insertChunk(InsertDataContext context, Chunk chunk) {
        return Future.future(p -> {
            log.trace("Received chunk for recording: [{}]", chunk);
            context.getWorkingMessagesNumber().incrementAndGet();
            context.setInsertSql(insertRequestFactory.getSql(context));
            ClickhouseInsertSqlRequest sqlRequest = insertRequestFactory.create(context, chunk.getRows());
            InsertChunk insertChunk = new InsertChunk(new PartitionOffset(
                    chunk.getTopicPartition(),
                    chunk.getOffset()),
                    sqlRequest);
            insertChunks.put(chunk.getOffset(), insertChunk);
            if (insertChunks.containsKey(lastOffset.get())) {
                while (insertChunks.containsKey(lastOffset.incrementAndGet())) ;
                insertChunkQueue.addAll(insertChunks.values());
                new HashSet<>(insertChunks.keySet())
                        .forEach(insertChunks::remove);
                if (insertChunkQueue.size() >= workerProperties.getMaxFetchSize()) {
                    topicPartitionConsumer.getKafkaConsumer().pause();
                }
            }
            p.complete();
        });
    }

    private void error(InsertDataContext context, Throwable throwable) {
        val contextId = context.getContextId();
        context.setCause(throwable);
        log.error("Error consuming message in context: {}", context, throwable.getCause());
        stop(contextId);
    }

    private void stop(String contextId) {
        vertx.eventBus().publish(DataTopic.SEND_RESPONSE.getValue(), contextId);
    }

    @Override
    public DeploymentOptions getDeploymentOptions() {
        return new DeploymentOptions().setWorker(true)
                .setWorkerPoolName(workerProperties.getPoolName())
                .setWorkerPoolSize(workerProperties.getPoolSize());
    }
}
