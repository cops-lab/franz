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
package examples.errorhandling;

import static dev.c0ps.franz.Lane.ERROR;
import static examples.utils.KafkaUtils.getKafkaInstance;

import java.util.Date;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import dev.c0ps.franz.KafkaErrors;

public class Main {

    private static final Logger LOG = LoggerFactory.getLogger(Main.class);

    // base instantiation of a Kafka instance with hard-coded connection details
    private static final KafkaErrors KAFKA = getKafkaInstance();
    private static final String SOME_TOPIC = "example.errors";

    public static void main(String[] args) {

        var e = createSomeError();
        LOG.info("Publishing an error: {}", e);
        KAFKA.publish(e, SOME_TOPIC, ERROR);

        KAFKA.subscribeErrors(SOME_TOPIC, String.class, s -> {
            LOG.info("- {}", s);
        });

        // please note: in contrast to regular polling, no loop is required here
        LOG.info("Found the following ERRORs in the {} topic:", SOME_TOPIC);
        KAFKA.pollAllErrors();

        KAFKA.stop();
    }

    private static String createSomeError() {
        return String.format("«Crash at %s»", new Date());
    }
}