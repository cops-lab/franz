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
package dev.c0ps.franz.clients;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import dev.c0ps.franz.Kafka;
import dev.c0ps.franz.Lane;
import dev.c0ps.franz.data.SomeInputData;

public class ReadInput {

    private static final Logger LOG = LoggerFactory.getLogger(ReadInput.class);

    public static final String INPUT_TOPIC = "example.in";

    private final Kafka kafka;

    public ReadInput(Kafka kafka) {
        this.kafka = kafka;
    }

    public void run() {
        LOG.info("Reading from terminal, publishing to {} ...\n", INPUT_TOPIC);

        var isRunning = true;
        while (isRunning) {

            var input = readNextLine();
            if (input.isEmpty()) {
                LOG.info("No input provided, aborting.");
                isRunning = false;
                continue;
            }
            var t = new SomeInputData(input, "another parameter");
            kafka.publish(t, INPUT_TOPIC, Lane.PRIORITY);
        }
    }

    private String readNextLine() {
        LOG.info("Please provide a string: ");
        var br = new BufferedReader(new InputStreamReader(System.in));
        try {
            return br.readLine();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}