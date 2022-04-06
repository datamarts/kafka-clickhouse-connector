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
package ru.datamart.kafka.clickhouse.writer.verticle.handler;

import io.vertx.core.Future;
import io.vertx.core.Vertx;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import lombok.var;
import org.springframework.stereotype.Component;
import ru.datamart.kafka.clickhouse.writer.repository.InsertDataContextRepository;

@Slf4j
@Component
@RequiredArgsConstructor
public class SendResponseHandler {
    private final InsertDataContextRepository repository;
    private final Vertx vertx;

    public void handleSendResponse(String contextId) {
        repository.remove(contextId).ifPresent(context -> {
            log.debug("Received context: {}", context);
            log.debug("Processing time: [{}] ms by request [{}]", context.getProcessingTime(), context.getRequest());
            var sequentialUndeploy = Future.<Void>succeededFuture();
            for (String verticleId : context.getVerticleIds()) {
                sequentialUndeploy = sequentialUndeploy.compose(v -> vertx.undeploy(verticleId))
                        .onComplete(ar -> {
                            if (ar.succeeded()) {
                                log.debug("Undeploy verticle success [{}]", verticleId);
                            } else {
                                log.debug("Undeploy verticle error [{}]: {}", verticleId, ar.cause());
                            }
                        })
                        .otherwiseEmpty();
            }
        });
    }
}
