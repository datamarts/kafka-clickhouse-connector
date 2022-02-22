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
package ru.datamart.kafka.clickhouse.writer.factory.impl;

import ru.datamart.kafka.clickhouse.writer.factory.KafkaConsumerFactory;
import io.vertx.core.Vertx;
import io.vertx.kafka.client.consumer.KafkaConsumer;

import java.util.Map;

public class VertxKafkaConsumerFactory<T, S> implements KafkaConsumerFactory<T, S> {

  private final Map<String, String> defaultProps;
  private final Vertx vertx;

  public VertxKafkaConsumerFactory(Vertx vertx, Map<String, String> defaultProps) {
    this.defaultProps = defaultProps;
    this.vertx = vertx;
  }

  @Override
  public KafkaConsumer<T, S> create(Map<String, String> config) {
    defaultProps.forEach(config::putIfAbsent);
    return KafkaConsumer.create(vertx, config);
  }
}
