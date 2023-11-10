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
package examples.utils;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;

import dev.c0ps.franz.KafkaConnector;
import dev.c0ps.franz.KafkaImpl;
import dev.c0ps.io.JsonUtils;
import dev.c0ps.io.JsonUtilsImpl;
import examples.basicpubsub.data.SomeInputData;
import examples.basicpubsub.data.SomeInputDataJson;

public class KafkaUtils {

    // configuration is assuming that the server auto-creates unknown topics
    private static final String KAFKA_URL = "localhost:19092";
    private static final String KAFKA_GROUP_ID = "kafka-example";

    public static KafkaImpl getKafkaInstance() {
        return new KafkaImpl(initJsonUtils(), initConnector(), true);
    }

    private static KafkaConnector initConnector() {
        return new KafkaConnector(KAFKA_URL, KAFKA_GROUP_ID);
    }

    private static JsonUtils initJsonUtils() {
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