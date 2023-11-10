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
package examples.basicpubsub;

import static examples.utils.KafkaUtils.getKafkaInstance;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;

import dev.c0ps.franz.Kafka;

public class Main {

    private static final ExecutorService EXEC = Executors.newSingleThreadExecutor();
    private static final AtomicBoolean IS_RUNNING = new AtomicBoolean(true);

    // base instantiation of a Kafka instance with hard-coded connection details
    private static final Kafka KAFKA = getKafkaInstance();

    public static void main(String[] args) {

        // run terminal reader in second thread
        EXEC.submit(new ReadInput(KAFKA, IS_RUNNING));

        // run transformer that subscribes and re-publishes
        new TransformInput(KAFKA, IS_RUNNING).run();

        EXEC.shutdown();
        KAFKA.stop();
    }
}