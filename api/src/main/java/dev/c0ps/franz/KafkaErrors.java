/*
 * Copyright 2022 Delft University of Technology
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
package dev.c0ps.franz;

import java.util.function.Consumer;

import dev.c0ps.io.TRef;

public interface KafkaErrors {

    <T> void subscribeErrors(String topic, Class<T> type, Consumer<T> callback);

    <T> void subscribeErrors(String topic, TRef<T> typeRef, Consumer<T> callback);

    <T> void publish(T obj, String topic, Lane lane);

    <T> void publish(String key, T obj, String topic, Lane lane);

    void pollAllErrors();

    void stop();
}