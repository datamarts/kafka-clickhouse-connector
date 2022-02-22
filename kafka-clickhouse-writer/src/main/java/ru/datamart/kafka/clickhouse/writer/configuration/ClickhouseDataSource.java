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
package ru.datamart.kafka.clickhouse.writer.configuration;

import ru.yandex.clickhouse.BalancedClickhouseDataSource;
import ru.yandex.clickhouse.ClickHouseConnection;
import ru.yandex.clickhouse.settings.ClickHouseProperties;

import java.sql.SQLException;
import java.util.Properties;

public class ClickhouseDataSource extends BalancedClickhouseDataSource {

    public ClickhouseDataSource(String url) {
        super(url);
    }

    public ClickhouseDataSource(String url, Properties properties) {
        super(url, properties);
    }

    public ClickhouseDataSource(String url, ClickHouseProperties properties) {
        super(url, properties);
    }

    @Override
    public ClickHouseConnection getConnection() throws SQLException {
        try {
            return super.getConnection();
        } catch (SQLException ex) {
            throw ex;
        } catch (Exception ex) {
            throw new SQLException(ex);
        }
    }

    @Override
    public ClickHouseConnection getConnection(String username, String password) throws SQLException {
        try {
            return super.getConnection(username, password);
        } catch (SQLException ex) {
            throw ex;
        } catch (Exception ex) {
            throw new SQLException(ex);
        }
    }
}
