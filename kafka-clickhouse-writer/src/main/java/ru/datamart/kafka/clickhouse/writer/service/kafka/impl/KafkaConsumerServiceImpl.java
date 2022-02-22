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
package ru.datamart.kafka.clickhouse.writer.service.kafka.impl;

import ru.datamart.kafka.clickhouse.writer.configuration.properties.kafka.KafkaProperties;
import ru.datamart.kafka.clickhouse.writer.factory.KafkaConsumerFactory;
import ru.datamart.kafka.clickhouse.writer.model.InsertDataContext;
import ru.datamart.kafka.clickhouse.writer.model.kafka.KafkaBrokerInfo;
import ru.datamart.kafka.clickhouse.writer.model.kafka.TopicPartitionConsumer;
import ru.datamart.kafka.clickhouse.writer.service.kafka.KafkaConsumerService;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.kafka.client.common.PartitionInfo;
import io.vertx.kafka.client.common.TopicPartition;
import io.vertx.kafka.client.consumer.KafkaConsumer;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.springframework.stereotype.Service;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static java.util.stream.Collectors.toList;

@Slf4j
@Service
public class KafkaConsumerServiceImpl implements KafkaConsumerService {
    private static final String BOOTSTRAP_SERVERS_KEY = "bootstrap.servers";
    private final KafkaConsumerFactory<byte[], byte[]> consumerFactory;
    private final KafkaProperties kafkaProperties;

    public KafkaConsumerServiceImpl(KafkaConsumerFactory<byte[], byte[]> consumerFactory,
                                    KafkaProperties kafkaProperties) {
        this.consumerFactory = consumerFactory;
        this.kafkaProperties = kafkaProperties;
    }

    @Override
    public Future<List<PartitionInfo>> getTopicPartitions(InsertDataContext context) {
        val consumerProperty = getConsumerProperty(context);
        val topicName = context.getRequest().getKafkaTopic();
        return getTopicPartitions(topicName, consumerProperty);
    }

    private Future<List<PartitionInfo>> getTopicPartitions(String topicName,
                                                           Map<String, String> consumerProperty) {
        val partitionInfoConsumer = createConsumer(consumerProperty, topicName);
        return Future.future(p -> partitionInfoConsumer.partitionsFor(topicName, ar -> {
            partitionInfoConsumer.close();
            if (ar.succeeded()) {
                p.complete(ar.result());
            } else {
                val errMsg = String.format("Can't get partition info by topic [%s]", topicName);
                log.error(errMsg, ar.cause());
                p.fail(ar.cause());
            }
        }));
    }

    @Override
    public KafkaConsumer<byte[], byte[]> createConsumer(InsertDataContext context) {
        val consumerProperty = getConsumerProperty(context);
        val topicName = context.getRequest().getKafkaTopic();
        return createConsumer(consumerProperty, topicName);
    }

    private KafkaConsumer<byte[], byte[]> createConsumer(Map<String, String> consumerProperty,
                                                         String topicName) {
        val consumer = consumerFactory.create(consumerProperty);
        consumer.subscribe(topicName, it -> {
            if (it.succeeded()) {
                log.debug("Successful topic subscription {}", topicName);
            } else {
                log.error("Topic Subscription Error {}: {}", topicName, it.cause().getMessage());
            }
        });
        consumer.exceptionHandler(it -> log.error("Error reading message from topic {} message {}: {}", topicName, it.getMessage(), it.getCause()));
        return consumer;
    }

    @Override
    public Future<TopicPartitionConsumer> createTopicPartitionConsumer(InsertDataContext context, PartitionInfo partitionInfo) {
        return createTopicPartitionConsumer(getConsumerProperty(context), partitionInfo);
    }

    private Future<TopicPartitionConsumer> createTopicPartitionConsumer(Map<String, String> consumerProperty,
                                                                        PartitionInfo partitionInfo) {
        val consumer = consumerFactory.create(consumerProperty);
        consumer.exceptionHandler(it -> log.error("Error reading message from TopicPartition {} message {}: {}", partitionInfo, it.getMessage(), it.getCause()));
        val topicPartition = new TopicPartition(partitionInfo.getTopic(), partitionInfo.getPartition());
        return assign(consumer, topicPartition)
                .compose(v -> getPosition(consumer, topicPartition))
                .map(beginningOffset -> new TopicPartitionConsumer(consumer, topicPartition, beginningOffset))
                .onSuccess(topicPartitionConsumer -> log.info("created consumer[{}] by topicPartition [{}]", topicPartitionConsumer, topicPartition));
    }

    private Future<Void> assign(KafkaConsumer<byte[], byte[]> consumer, TopicPartition topicPartition) {
        return Future.future((Promise<Void> p) -> consumer.assign(topicPartition, p));
    }

    private Future<Long> getPosition(KafkaConsumer<byte[], byte[]> consumer, TopicPartition topicPartition) {
        return Future.future((Promise<Long> p) -> consumer.position(topicPartition, p));
    }

    private HashMap<String, String> getConsumerProperty(InsertDataContext context) {
        val props = new HashMap<>(kafkaProperties.getConsumer().getProperty());
        updateGroupId(context, props);
        List<String> brokerAddresses = context.getRequest().getKafkaBrokers().stream()
                .map(KafkaBrokerInfo::getAddress).collect(toList());
        props.put(BOOTSTRAP_SERVERS_KEY, String.join(",", brokerAddresses));
        return props;
    }

    private void updateGroupId(InsertDataContext context, Map<String, String> props) {
        props.put("group.id", context.getRequest().getConsumerGroup());
    }
}
