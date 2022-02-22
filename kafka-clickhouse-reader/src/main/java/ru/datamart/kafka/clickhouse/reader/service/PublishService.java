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
package ru.datamart.kafka.clickhouse.reader.service;

import ru.datamart.kafka.clickhouse.avro.model.DtmQueryResponseMetadata;
import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import io.vertx.kafka.client.producer.KafkaProducer;

public interface PublishService {
    void publishQueryResult(KafkaProducer<byte[], byte[]> kafkaProducer,
                            String topicName,
                            DtmQueryResponseMetadata key,
                            byte[] message,
                            Handler<AsyncResult<Void>> handler);
}