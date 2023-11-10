/*
 * Copyright 2021 Delft University of Technology
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

import static examples.utils.LineReader.readNextLine;

import java.util.concurrent.atomic.AtomicBoolean;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import dev.c0ps.franz.Kafka;
import dev.c0ps.franz.Lane;
import examples.basicpubsub.data.SomeInputData;

public class ReadInput implements Runnable {

    private static final Logger LOG = LoggerFactory.getLogger(ReadInput.class);

    public static final String INPUT_TOPIC = "example.in";

    private final Kafka kafka;
    private final AtomicBoolean isRunning;

    public ReadInput(Kafka kafka, AtomicBoolean isRunning) {
        this.kafka = kafka;
        this.isRunning = isRunning;
    }

    @Override
    public void run() {
        LOG.info("Reading from terminal, publishing to {} ...\n", INPUT_TOPIC);

        while (isRunning.get()) {

            var input = readNextLine("Please provide a string (empty to abort): ");
            if (input.isEmpty()) {
                LOG.info("No input provided, aborting.");
                isRunning.set(false);
                continue;
            }
            var t = new SomeInputData(input, "another parameter");
            kafka.publish(t, INPUT_TOPIC, Lane.PRIORITY);
        }
    }
}