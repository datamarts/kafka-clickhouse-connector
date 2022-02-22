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

import com.fasterxml.jackson.core.JsonProcessingException;
import ru.datamart.kafka.clickhouse.writer.configuration.properties.VerticleProperties;
import ru.datamart.kafka.clickhouse.writer.factory.InsertRequestFactory;
import ru.datamart.kafka.clickhouse.writer.model.DataTopic;
import ru.datamart.kafka.clickhouse.writer.model.InsertDataContext;
import ru.datamart.kafka.clickhouse.writer.model.kafka.InsertChunk;
import ru.datamart.kafka.clickhouse.writer.model.kafka.PartitionOffset;
import ru.datamart.kafka.clickhouse.writer.service.executor.DataSourceExecutor;
import io.vertx.core.DeploymentOptions;
import io.vertx.core.Future;
import io.vertx.core.json.jackson.DatabindCodec;
import lombok.Builder;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import lombok.val;

import java.util.ArrayList;
import java.util.Queue;
import java.util.UUID;

@Slf4j
@Getter
@Builder
public class InsertVerticle extends ConfigurableVerticle {
    public static final String INSERT_START_TOPIC = "insert_start";
    private final VerticleProperties.InsertWorkerProperties workerProperties;
    private final InsertRequestFactory insertRequestFactory;
    private final String id = UUID.randomUUID().toString();
    private final DataSourceExecutor executor;
    private final InsertDataContext context;
    private final Queue<InsertChunk> insertChunkQueue;
    private volatile boolean stopped;
    private volatile long timerId;

    @Override
    public void start(Future<Void> startFuture) throws Exception {
        super.start(startFuture);
        vertx.eventBus().consumer(INSERT_START_TOPIC + context.getContextId(),
                ar -> runProcessInserts());
    }

    private void runProcessInserts() {
        if (stopped) {
            return;
        }

        timerId = vertx.setTimer(workerProperties.getInsertPeriodMs(), timer -> {
            log.debug("Batch queue size [{}]", insertChunkQueue.size());
            InsertChunk insertChunk = insertChunkQueue.poll();
            if (insertChunk != null) {
                processChunk(insertChunk);
            } else {
                runProcessInserts();
            }
        });
    }

    private void processChunk(InsertChunk insertChunk) {
        val partitionOffsets = new ArrayList<PartitionOffset>();
        partitionOffsets.add(insertChunk.getPartitionOffset());
        int batchSize = workerProperties.getBatchSize();
        while (!insertChunkQueue.isEmpty()) {
            InsertChunk chunk = insertChunkQueue.poll();
            if (chunk != null) {
                insertChunk.getInsertSqlRequest().getParams()
                        .addAll(chunk.getInsertSqlRequest().getParams());
                partitionOffsets.add(chunk.getPartitionOffset());
                if (--batchSize == 0) break;
            } else {
                break;
            }
        }

        executor.processChunk(context, insertChunk, ar -> {
            if (ar.succeeded()) {
                try {
                    log.debug("Written lines [{}] to data source",
                            insertChunk.getInsertSqlRequest().getParams().size());
                    vertx.eventBus().publish(KafkaConsumerVerticle.KAFKA_COMMIT_TOPIC + context.getContextId(),
                            DatabindCodec.mapper().writeValueAsString(partitionOffsets));
                    runProcessInserts();
                } catch (JsonProcessingException e) {
                    log.error("Serialize partitionOffsets error: [{}]: {}", partitionOffsets, e.getMessage());
                    error(context, e);
                }
            } else {
                error(context, ar.cause());
            }
        });
    }


    @Override
    public void stop() {
        stopped = true;
        vertx.cancelTimer(timerId);
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
