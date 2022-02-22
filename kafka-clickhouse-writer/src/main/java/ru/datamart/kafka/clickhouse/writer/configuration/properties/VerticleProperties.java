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
package ru.datamart.kafka.clickhouse.writer.configuration.properties;

import lombok.Data;
import lombok.EqualsAndHashCode;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

@Data
@Component
@ConfigurationProperties(prefix = "verticle.worker")
public class VerticleProperties {

    private WorkerProperties newDataWorker;
    private WorkerProperties taskWorker;
    private KafkaConsumerWorkerProperties kafkaConsumerWorker;
    private CommitWorkerProperties kafkaCommitWorker;
    private InsertWorkerProperties insertWorker;


    @Data
    public static class WorkerProperties {
        private Integer poolSize;
        private String poolName;
        private Long responseTimeoutMs = 864_00_000L;
    }

    @Data
    @EqualsAndHashCode(callSuper = true)
    public static class InsertWorkerProperties extends WorkerProperties {
        private long insertPeriodMs = 1000;
        private int batchSize = 1000_000;
    }

    @Data
    @EqualsAndHashCode(callSuper = true)
    public static class KafkaConsumerWorkerProperties extends WorkerProperties {
        private int maxFetchSize = 10_000;
    }

    @Data
    @EqualsAndHashCode(callSuper = true)
    public static class CommitWorkerProperties extends WorkerProperties {
        private long commitPeriodMs = 1000;
    }

}
