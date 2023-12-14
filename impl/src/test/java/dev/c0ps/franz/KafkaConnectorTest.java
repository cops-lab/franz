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
package dev.c0ps.franz;

import static dev.c0ps.franz.Lane.NORMAL;
import static dev.c0ps.franz.Lane.PRIORITY;
import static java.lang.String.format;
import static org.apache.kafka.clients.consumer.ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.AUTO_OFFSET_RESET_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.CLIENT_ID_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.FETCH_MAX_BYTES_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.GROUP_ID_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.GROUP_INSTANCE_ID_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.MAX_POLL_RECORDS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.REQUEST_TIMEOUT_MS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.BOOTSTRAP_SERVERS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.MAX_REQUEST_SIZE_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.security.InvalidParameterException;
import java.util.Properties;

import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import dev.c0ps.commons.AssertsException;

public class KafkaConnectorTest {

    private static final String INTERNAL_LEAVE_GROUP_ON_CLOSE = "internal.leave.group.on.close";

    private static final String KAFKA_URL = "1.2.3.4:1234";
    private static final String SOME_PLUGIN = "p";
    private static final String SOME_INSTANCE = "X";

    private KafkaConnector sut;

    @BeforeEach
    public void setup() {
        sut = new KafkaConnector(KAFKA_URL, SOME_PLUGIN);
    }

    @Test
    public void checkDefaultProducerProperties() {
        var actual = sut.getProducerProperties();

        Properties expected = new Properties();
        expected.setProperty(BOOTSTRAP_SERVERS_CONFIG, KAFKA_URL);
        expected.setProperty(KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        expected.setProperty(VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        expected.setProperty(MAX_REQUEST_SIZE_CONFIG, Integer.toString(50 * 1024 * 1024));

        assertEquals(expected, actual);
    }

    @Test
    public void checkDefaultConsumerProperties() {
        for (var l : Lane.values()) {
            var actual = sut.getConsumerProperties(l);

            Properties expected = new Properties();
            expected.setProperty(BOOTSTRAP_SERVERS_CONFIG, KAFKA_URL);
            expected.setProperty(GROUP_ID_CONFIG, SOME_PLUGIN + "-" + l);
            expected.setProperty(AUTO_OFFSET_RESET_CONFIG, "earliest");
            expected.setProperty(KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            expected.setProperty(VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            expected.setProperty(FETCH_MAX_BYTES_CONFIG, Integer.toString(50 * 1024 * 1024));
            expected.setProperty(MAX_POLL_RECORDS_CONFIG, "1");
            expected.setProperty(MAX_POLL_INTERVAL_MS_CONFIG, Integer.toString(1000 * 60 * 30));
            expected.setProperty(ENABLE_AUTO_COMMIT_CONFIG, "false");
            expected.setProperty(AUTO_COMMIT_INTERVAL_MS_CONFIG, "0");
            expected.setProperty(REQUEST_TIMEOUT_MS_CONFIG, "60000");
            expected.setProperty(INTERNAL_LEAVE_GROUP_ON_CLOSE, "false");

            assertEquals(expected, actual);
        }
    }

    @Test
    public void checkDefaultConsumerPropertiesInstanceId() {
        sut = new KafkaConnector(KAFKA_URL, SOME_PLUGIN, SOME_INSTANCE);
        for (var l : Lane.values()) {
            var actual = sut.getConsumerProperties(l);

            // same as non-instanced
            Properties expected = new Properties();
            expected.setProperty(BOOTSTRAP_SERVERS_CONFIG, KAFKA_URL);
            expected.setProperty(GROUP_ID_CONFIG, SOME_PLUGIN + "-" + l);
            expected.setProperty(AUTO_OFFSET_RESET_CONFIG, "earliest");
            expected.setProperty(KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            expected.setProperty(VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            expected.setProperty(FETCH_MAX_BYTES_CONFIG, Integer.toString(50 * 1024 * 1024));
            expected.setProperty(MAX_POLL_RECORDS_CONFIG, "1");
            expected.setProperty(MAX_POLL_INTERVAL_MS_CONFIG, Integer.toString(1000 * 60 * 30));
            expected.setProperty(ENABLE_AUTO_COMMIT_CONFIG, "false");
            expected.setProperty(AUTO_COMMIT_INTERVAL_MS_CONFIG, "0");
            expected.setProperty(REQUEST_TIMEOUT_MS_CONFIG, "60000");
            expected.setProperty(INTERNAL_LEAVE_GROUP_ON_CLOSE, "false");

            // additional
            var instanceId = format("%s-%s-%s", SOME_PLUGIN, SOME_INSTANCE, l);
            expected.setProperty(CLIENT_ID_CONFIG, instanceId);
            expected.setProperty(GROUP_INSTANCE_ID_CONFIG, instanceId);

            assertEquals(expected, actual);
        }
    }

    @Test
    public void smokeTestCanConstructProducerAndConsumer() {
        assertNotNull(sut.getConsumerConnection(NORMAL));
        assertNotNull(sut.getConsumerConnection(PRIORITY));
        assertNotNull(sut.getProducerConnection());
    }

    @Test
    public void smokeTestCanConstructProducerAndConsumerWithInstanceId() {
        sut = new KafkaConnector(KAFKA_URL, SOME_PLUGIN, SOME_INSTANCE);
        assertNotNull(sut.getConsumerConnection(NORMAL));
        assertNotNull(sut.getConsumerConnection(PRIORITY));
        assertNotNull(sut.getProducerConnection());
    }

    @Test
    public void instanceCanOnlyBeUsedOnce() {
        sut = new KafkaConnector(KAFKA_URL, SOME_PLUGIN, SOME_INSTANCE);
        sut.getConsumerProperties(NORMAL);
        assertThrows(InvalidParameterException.class, () -> {
            sut.getConsumerProperties(NORMAL);
        });
    }

    @Test
    public void insufficientParameterValidation() {
        assertThrows(AssertsException.class, () -> {
            new KafkaConnector(null, SOME_PLUGIN, SOME_INSTANCE);
        });
        assertThrows(AssertsException.class, () -> {
            new KafkaConnector("", SOME_PLUGIN, SOME_INSTANCE);
        });
        assertThrows(AssertsException.class, () -> {
            new KafkaConnector(KAFKA_URL, null, SOME_INSTANCE);
        });
        assertThrows(AssertsException.class, () -> {
            new KafkaConnector(KAFKA_URL, "", SOME_INSTANCE);
        });
        assertThrows(AssertsException.class, () -> {
            new KafkaConnector(KAFKA_URL, SOME_PLUGIN, "");
        });
    }
}