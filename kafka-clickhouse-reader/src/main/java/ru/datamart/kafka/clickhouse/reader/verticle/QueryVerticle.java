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
package ru.datamart.kafka.clickhouse.reader.verticle;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.Promise;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.handler.BodyHandler;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import ru.datamart.kafka.clickhouse.reader.configuration.AppConfiguration;
import ru.datamart.kafka.clickhouse.reader.controller.QueryController;
import ru.datamart.kafka.clickhouse.reader.controller.VersionController;

@Slf4j
@Component
public class QueryVerticle extends AbstractVerticle {

    private final AppConfiguration configuration;
    private final QueryController queryController;
    private final VersionController versionController;

    @Autowired
    public QueryVerticle(AppConfiguration configuration,
                         QueryController queryController,
                         VersionController versionController) {
        this.configuration = configuration;
        this.queryController = queryController;
        this.versionController = versionController;
    }

    @Override
    public void start(Promise<Void> startPromise) {
        Router router = Router.router(vertx);
        router.mountSubRouter("/", apiRouter());
        vertx.createHttpServer().requestHandler(router).listen(configuration.httpPort())
                .onSuccess(httpServer -> {
                    log.info("Registered instance on port: [{}]", httpServer.actualPort());
                    startPromise.complete();
                })
                .onFailure(startPromise::fail);
    }

    private Router apiRouter() {
        Router router = Router.router(vertx);
        router.route().handler(BodyHandler.create());
        router.route().consumes("application/json");
        router.route().produces("application/json");
        router.post("/query").handler(queryController::query);
        router.get("/versions").handler(versionController::version);
        return router;
    }

}
