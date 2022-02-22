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
package ru.datamart.kafka.clickhouse.reader.factory;

import io.vertx.core.Vertx;
import io.vertx.kafka.client.producer.KafkaProducer;

import java.util.Map;

public class VertxKafkaProducerFactory<T, S> implements KafkaProducerFactory<T, S> {

    private final Vertx vertx;
    private final Map<String, String> defaultProps;

    public VertxKafkaProducerFactory(Vertx vertx, Map<String, String> defaultProps) {
        this.vertx = vertx;
        this.defaultProps = defaultProps;
    }

    @Override
    public KafkaProducer<T, S> create(Map<String, String> config) {
        defaultProps.forEach(config::putIfAbsent);
        return KafkaProducer.create(vertx, defaultProps);
    }
}
