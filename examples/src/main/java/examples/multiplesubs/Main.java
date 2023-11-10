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
package examples.multiplesubs;

import static dev.c0ps.franz.Lane.PRIORITY;
import static examples.utils.KafkaUtils.getKafkaInstance;
import static examples.utils.LineReader.readNextLine;

import java.util.Date;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import dev.c0ps.franz.Kafka;
import dev.c0ps.franz.Lane;

public class Main {

    private static final Logger LOG = LoggerFactory.getLogger(Main.class);
    private static final ExecutorService EXEC = Executors.newSingleThreadExecutor();

    // base instantiation of a Kafka instance with hard-coded connection details
    private static final Kafka KAFKA = getKafkaInstance();
    private static final String SOME_TOPIC_A = "example.mutisubs.a";
    private static final String SOME_TOPIC_B = "example.mutisubs.b";

    private static final AtomicBoolean IS_RUNNING = new AtomicBoolean(true);

    public static void main(String[] args) {

        // run a separate thread to read/publish terminal input
        EXEC.submit(new Reader());

        LOG.info("Publishing two messages");
        KAFKA.publish(createSomeMsg(), SOME_TOPIC_A, PRIORITY);
        KAFKA.publish(createSomeMsg(), SOME_TOPIC_B, PRIORITY);

        KAFKA.subscribe(SOME_TOPIC_A, String.class, (s, l) -> {
            LOG.info("(A) {}", s);
        });

        KAFKA.subscribe(SOME_TOPIC_B, String.class, (s, l) -> {
            LOG.info("(B) {}", s);
        });

        // please note: in contrast to regular polling, no loop is required here
        LOG.info("Found the following messages in both topics:", SOME_TOPIC_A);
        while (IS_RUNNING.get()) {
            KAFKA.poll();
        }

        KAFKA.stop();
        EXEC.shutdown();
    }

    private static String createSomeMsg() {
        return String.format("«Message at %s»", new Date());
    }

    private static class Reader implements Runnable {

        @Override
        public void run() {
            while (IS_RUNNING.get()) {

                var input = readNextLine("Please enter 'a', 'b', or an empty line to abort: ");
                if (input.isEmpty()) {
                    LOG.info("No input provided, aborting.");
                    IS_RUNNING.set(false);
                    continue;
                }

                switch (input) {
                case "a":
                    KAFKA.publish(createSomeMsg(), SOME_TOPIC_A, Lane.PRIORITY);
                    break;
                case "b":
                    KAFKA.publish(createSomeMsg(), SOME_TOPIC_B, Lane.PRIORITY);
                    break;
                default:
                    LOG.warn("Only enter 'a', 'b', or an empty line!");
                }
            }
        }

    }
}