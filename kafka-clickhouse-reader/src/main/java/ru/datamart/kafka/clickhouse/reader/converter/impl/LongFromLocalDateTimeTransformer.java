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
package ru.datamart.kafka.clickhouse.reader.converter.impl;

import ru.datamart.kafka.clickhouse.reader.converter.transformer.AbstractColumnTransformer;
import ru.datamart.kafka.clickhouse.util.DateTimeUtils;

import java.time.LocalDateTime;
import java.util.Collection;
import java.util.Collections;

public class LongFromLocalDateTimeTransformer extends AbstractColumnTransformer<Long, LocalDateTime> {

    @Override
    public Long transformValue(LocalDateTime value) {
        return value != null ? DateTimeUtils.toMicros(value) : null;
    }

    @Override
    public Collection<Class<?>> getTransformClasses() {
        return Collections.singletonList(LocalDateTime.class);
    }
}
