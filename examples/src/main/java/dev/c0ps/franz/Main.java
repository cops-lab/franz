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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;

import dev.c0ps.franz.clients.ReadInput;
import dev.c0ps.franz.clients.TransformInput;
import dev.c0ps.franz.data.SomeInputData;
import dev.c0ps.franz.data.SomeInputDataJson;
import dev.c0ps.io.JsonUtilsImpl;

public class Main {

    private static final Logger LOG = LoggerFactory.getLogger(Main.class);

    // configuration is assuming that the server auto-creates unknown topics
    private static final String KAFKA_URL = "localhost:9092";
    private static final String KAFKA_GROUP_ID = "kafka-example";

    private static final String PRODUCE = "produce";
    private static final String CONSUME = "consume";

    public static void main(String[] args) {

        var kafka = new KafkaImpl(initJsonUtils(), initConnector(), true);

        // use system argument to start the two connected applications
        if (isProduce(args)) {
            // either read CLI input and publish in topics ...
            new ReadInput(kafka).run();
        } else {
            // .. or consume topics and transform input
            new TransformInput(kafka).run();
        }
    }

    private static boolean isProduce(String[] args) {
        if (args.length != 1) {
            LOG.info("Need exactly one parameter");
            System.exit(1);
        }
        var isProduce = args[0].equals(PRODUCE);
        var isConsume = args[0].equals(CONSUME);
        if (!isProduce && !isConsume) {
            LOG.info("Parameter must be '{}' or '{}'", PRODUCE, CONSUME);
            System.exit(1);
        }
        return isProduce;
    }

    private static KafkaConnector initConnector() {
        return new KafkaConnector(KAFKA_URL, KAFKA_GROUP_ID);
    }

    private static JsonUtilsImpl initJsonUtils() {
        var om = initObjectMapper();
        return new JsonUtilsImpl(om);
    }

    private static ObjectMapper initObjectMapper() {
        // register a (de-)serializer for the data structure of the example
        var m = new SimpleModule();
        m.addSerializer(SomeInputData.class, new SomeInputDataJson.SomeInputDataSerializer());
        m.addDeserializer(SomeInputData.class, new SomeInputDataJson.SomeInputDataDeserializer());

        // instead of instantiation, consider using dev.c0ps.io.ObjectMapperBuilder
        return new ObjectMapper().registerModule(m);
    }
}