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
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anySet;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import java.time.Duration;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import dev.c0ps.io.JsonUtils;
import dev.c0ps.io.TRef;
import dev.c0ps.test.TestLoggerUtils;

public class KafkaImplTest {

    private static final Duration ZEROISH = Duration.ofMillis(50);
    private static final TRef<String> T_STRING = new TRef<String>() {};
    private static final BiConsumer<String, Lane> SOME_CB = (s, l) -> {};
    private static final BiConsumer<String, Lane> OTHER_CB = (s, l) -> {};
    private static final BiFunction<String, Throwable, ?> SOME_ERR_CB = (s, t) -> null;

    private JsonUtils jsonUtils;
    private KafkaConnector connector;
    private BackgroundHeartbeatHelper helper;
    private KafkaConsumer<String, String> consumerNorm;
    private KafkaConsumer<String, String> consumerPrio;
    private KafkaProducer<String, String> producer;

    private Map<Object, String> jsons = new HashMap<>();

    private KafkaImpl sut;

    @BeforeEach
    @SuppressWarnings("unchecked")
    public void setup() {
        TestLoggerUtils.clearLog();
        jsonUtils = mock(JsonUtils.class);
        helper = mock(BackgroundHeartbeatHelper.class);

        connector = mock(KafkaConnector.class);
        consumerNorm = mock(KafkaConsumer.class);
        consumerPrio = mock(KafkaConsumer.class);
        producer = mock(KafkaProducer.class);

        when(connector.getConsumerConnection(NORMAL)).thenReturn(consumerNorm);
        when(connector.getConsumerConnection(PRIORITY)).thenReturn(consumerPrio);
        when(connector.getProducerConnection()).thenReturn(producer);

        sut = new KafkaImpl(jsonUtils, connector, true);
        sut.setBackgroundHeartbeatHelper(helper);
    }

    @Test
    public void openThreeConnectionsOnConstruction() {
        verify(connector).getConsumerConnection(Lane.PRIORITY);
        verify(connector).getConsumerConnection(Lane.NORMAL);
        verify(connector).getProducerConnection();
    }

    @Test
    public void subscribesTwice() {
        sut.subscribe("t", String.class, SOME_CB);
        sut.subscribe("t", String.class, OTHER_CB);
        verifySubscribe(consumerNorm, 2, "t-NORMAL");
    }

    @Test
    public void subscribesEndUpAtConnection_Class() {
        sut.subscribe("t", String.class, SOME_CB);
        verifySubscribe(consumerNorm, "t-NORMAL");
        verifySubscribe(consumerPrio, "t-PRIORITY");

        sut.subscribe("t2", String.class, SOME_CB);
        verifySubscribe(consumerNorm, 2, "t-NORMAL", "t2-NORMAL");
        verifySubscribe(consumerPrio, 2, "t-PRIORITY", "t2-PRIORITY");
    }

    @Test
    public void subscribesEndUpAtConnection_ClassErr() {
        sut.subscribe("t", String.class, SOME_CB, SOME_ERR_CB);
        verifySubscribe(consumerNorm, "t-NORMAL");
        verifySubscribe(consumerPrio, "t-PRIORITY");

        sut.subscribe("t2", String.class, SOME_CB, SOME_ERR_CB);
        verifySubscribe(consumerNorm, 2, "t-NORMAL", "t2-NORMAL");
        verifySubscribe(consumerPrio, 2, "t-PRIORITY", "t2-PRIORITY");
    }

    @Test
    public void subscribesEndUpAtConnection_TRef() {
        sut.subscribe("t", T_STRING, SOME_CB);
        verifySubscribe(consumerNorm, "t-NORMAL");
        verifySubscribe(consumerPrio, "t-PRIORITY");

        sut.subscribe("t2", T_STRING, SOME_CB);
        verifySubscribe(consumerNorm, 2, "t-NORMAL", "t2-NORMAL");
        verifySubscribe(consumerPrio, 2, "t-PRIORITY", "t2-PRIORITY");
    }

    @Test
    public void subscribesEndUpAtConnection_TRefErr() {
        sut.subscribe("t", T_STRING, SOME_CB, SOME_ERR_CB);
        verifySubscribe(consumerNorm, "t-NORMAL");
        verifySubscribe(consumerPrio, "t-PRIORITY");

        sut.subscribe("t2", T_STRING, SOME_CB, SOME_ERR_CB);
        verifySubscribe(consumerNorm, 2, "t-NORMAL", "t2-NORMAL");
        verifySubscribe(consumerPrio, 2, "t-PRIORITY", "t2-PRIORITY");
    }

    @Test
    public void publishEndsUpAtConnection() {
        @SuppressWarnings("unchecked")
        ArgumentCaptor<ProducerRecord<String, String>> captor = ArgumentCaptor.forClass(ProducerRecord.class);

        registerSerialization("x", String.class);

        sut.publish("x", "t", PRIORITY);
        verify(producer).send(captor.capture());
        verify(producer).flush();

        var actual = captor.getValue().value();
        assertNotNull(actual);
        var expected = jsons.get("x");
        assertEquals(expected, actual);
    }

    @Test
    public void publishWithKeyEndsUpAtConnection() {
        @SuppressWarnings("unchecked")
        ArgumentCaptor<ProducerRecord<String, String>> captor = ArgumentCaptor.forClass(ProducerRecord.class);

        registerSerialization("x", String.class);

        sut.publish("k", "x", "t", PRIORITY);
        verify(producer).send(captor.capture());
        verify(producer).flush();

        var actual = captor.getValue();
        assertEquals("k", actual.key());
        var val = actual.value();
        assertNotNull(val);
        var expected = jsons.get("x");
        assertEquals(expected, val);
    }

    @Test
    public void poll_propagatesToConsumers() {
        when(consumerNorm.poll(any(Duration.class))).thenReturn(ConsumerRecords.empty());
        when(consumerPrio.poll(any(Duration.class))).thenReturn(ConsumerRecords.empty());
        sut.poll();
        verify(consumerPrio).poll(eq(ZEROISH));
        verify(consumerNorm).poll(eq(ZEROISH));
        verify(consumerPrio).commitSync();
        verify(consumerNorm).commitSync();
    }

    @Test
    public void poll_waitsAfterNoMessages() {
        when(consumerNorm.poll(any(Duration.class))).thenReturn(ConsumerRecords.empty());
        when(consumerPrio.poll(any(Duration.class))).thenReturn(ConsumerRecords.empty());
        sut.poll();
        sut.poll();
        verify(consumerPrio).poll(eq(ZEROISH));
        verify(consumerPrio).poll(eq(Duration.ofSeconds(10)));
        verify(consumerNorm, times(2)).poll(eq(ZEROISH));
        verify(consumerPrio, times(2)).commitSync();
        verify(consumerNorm, times(2)).commitSync();
        verifyNoMoreInteractions(consumerNorm);
    }

    @Test
    public void poll_prioCausesHeartbeat() {
        when(consumerNorm.poll(any(Duration.class))).thenReturn(ConsumerRecords.empty());
        when(consumerPrio.poll(any(Duration.class))).thenReturn(records("t-PRIORITY", "a"));
        sut.subscribe("t", String.class, SOME_CB);
        sut.poll();

        // regular poll
        verify(consumerPrio).poll(ZEROISH);
        verify(consumerPrio).commitSync();

        // heartbeat
        verifySubscribe(consumerNorm, "t-NORMAL");
        verify(consumerNorm).assignment();
        verify(consumerNorm).pause(anySet());
        verify(consumerNorm).poll(ZEROISH);
        verify(consumerNorm).resume(anySet());
        verifyNoMoreInteractions(consumerNorm);
    }

    @Test
    public void sendHeartbeat() {
        sut.sendHeartbeat();

        verify(consumerNorm).assignment();
        verify(consumerNorm).pause(anySet());
        verify(consumerNorm).poll(ZEROISH);
        verify(consumerNorm).resume(anySet());
        verifyNoMoreInteractions(consumerNorm);

        verify(consumerPrio).assignment();
        verify(consumerPrio).pause(anySet());
        verify(consumerPrio).poll(ZEROISH);
        verify(consumerPrio).resume(anySet());
        verifyNoMoreInteractions(consumerPrio);
    }

    @Test
    public void subIsTriggered() {
        registerSerialization("some_input", String.class);
        var wasCalled = new boolean[] { false };
        when(consumerNorm.poll(any(Duration.class))).thenReturn(ConsumerRecords.empty());
        when(consumerPrio.poll(any(Duration.class))).thenReturn(records("t-PRIORITY", jsons.get("some_input")));
        sut.subscribe("t", String.class, (actual, l) -> {
            assertEquals(Lane.PRIORITY, l);
            assertEquals("some_input", actual);
            wasCalled[0] = true;
        });
        sut.poll();
        assertTrue(wasCalled[0]);
    }

    @Test
    public void errorCallbacksAreTriggered() {
        registerSerialization("some_input", String.class);
        registerSerialization("some_output", String.class);
        when(consumerNorm.poll(any(Duration.class))).thenReturn(ConsumerRecords.empty());
        when(consumerPrio.poll(any(Duration.class))).thenReturn(records("t-PRIORITY", jsons.get("some_input")));

        var wasCalled = new boolean[] { false };
        var t = new IllegalArgumentException();
        sut.subscribe("t", String.class, (s, l) -> {
            throw t;
        }, (s, e) -> {
            assertTrue(e == t);
            assertEquals("some_input", s);
            wasCalled[0] = true;
            return "some_output";
        });
        sut.poll();
        assertTrue(wasCalled[0]);

        @SuppressWarnings("unchecked")
        ArgumentCaptor<ProducerRecord<String, String>> captor = ArgumentCaptor.forClass(ProducerRecord.class);
        verify(producer).send(captor.capture());
        var actual = captor.getValue();
        assertEquals(jsons.get("some_output"), actual.value());
        assertEquals("t-ERROR", actual.topic());

        var log = TestLoggerUtils.getFormattedLogs(KafkaImpl.class);
        assertFalse(log.contains("ERROR Unhandled exception in callback"));
    }

    @Test
    public void errorCallbackIgnoresNone() {
        registerSerialization("some_input", String.class);
        when(consumerNorm.poll(any(Duration.class))).thenReturn(ConsumerRecords.empty());
        when(consumerPrio.poll(any(Duration.class))).thenReturn(records("t-PRIORITY", jsons.get("some_input")));
        sut.subscribe("t", String.class, (actual, l) -> {
            throw new RuntimeException();
        });
        sut.poll();

        var log = TestLoggerUtils.getFormattedLogs(KafkaImpl.class);
        var found = false;
        for (var l : log) {
            var part = "ERROR Unhandled exception when processing ";
            if (l.startsWith(part)) {
                found = true;
                assertTrue(l.length() > part.length());
            }
        }
        assertTrue(found);
    }

    @Test
    public void commitIsAutoCalledWhenEnabled() {
        sut = new KafkaImpl(jsonUtils, connector, true);
        when(consumerPrio.poll(any(Duration.class))).thenReturn(ConsumerRecords.empty());
        when(consumerNorm.poll(any(Duration.class))).thenReturn(ConsumerRecords.empty());
        sut.poll();
        verify(consumerPrio, times(1)).commitSync();
        verify(consumerNorm, times(1)).commitSync();
    }

    @Test
    public void commitIsNotCalledWhenDisabled() {
        sut = new KafkaImpl(jsonUtils, connector, false);
        when(consumerPrio.poll(any(Duration.class))).thenReturn(ConsumerRecords.empty());
        when(consumerNorm.poll(any(Duration.class))).thenReturn(ConsumerRecords.empty());
        sut.poll();
        verify(consumerPrio, times(0)).commitSync();
        verify(consumerNorm, times(0)).commitSync();
    }

    @Test
    public void commitCanBeManuallyCalledForBothConsumers() {
        sut = new KafkaImpl(jsonUtils, connector, true);
        sut.commit();
        verify(consumerPrio, times(1)).commitSync();
        verify(consumerNorm, times(1)).commitSync();
    }

    @Test
    public void stopClosesConnections() {
        sut.stop();
        verify(consumerNorm).wakeup();
        verify(consumerNorm).close();
        verify(consumerPrio).wakeup();
        verify(consumerPrio).close();
        verify(producer).close();
    }

    @Test
    public void stopWithoutHelperDoesNotCrash() {
        sut.setBackgroundHeartbeatHelper(null);
        sut.stop();
        verify(consumerNorm).wakeup();
        verify(consumerNorm).close();
        verify(consumerPrio).wakeup();
        verify(consumerPrio).close();
        verify(producer).close();
    }

    @Test
    public void backgroundHeartbeatHelperMustBeInstalledToEnable() {
        sut.setBackgroundHeartbeatHelper(null);
        var e = assertThrows(IllegalStateException.class, () -> {
            sut.enableBackgroundHeartbeat();
        });
        var helperClass = BackgroundHeartbeatHelper.class.getSimpleName();
        var kafkaClass = KafkaImpl.class.getSimpleName();
        assertEquals(format("No %s configured in this %s instance", helperClass, kafkaClass), e.getMessage());
    }

    @Test
    public void backgroundHeartbeatHelperMustBeInstalledToDisable() {
        sut.setBackgroundHeartbeatHelper(null);
        var e = assertThrows(IllegalStateException.class, () -> {
            sut.disableBackgroundHeartbeat();
        });
        var helperClass = BackgroundHeartbeatHelper.class.getSimpleName();
        var kafkaClass = KafkaImpl.class.getSimpleName();
        assertEquals(format("No %s configured in this %s instance", helperClass, kafkaClass), e.getMessage());
    }

    @Test
    public void backgroundHeartbeatHelperIsDisabledWhenChanged() {
        verifyNoInteractions(helper);
        sut.setBackgroundHeartbeatHelper(null);
        verify(helper).disable();
    }

    @Test
    public void backgroundHeartbeatEnabled() {
        sut.enableBackgroundHeartbeat();
        verify(helper).enable();
        verifyNoMoreInteractions(helper);
    }

    @Test
    public void backgroundHeartbeatDisabled() {
        sut.disableBackgroundHeartbeat();
        verify(helper).disable();
        verifyNoMoreInteractions(helper);
    }

    // utils

    private static ConsumerRecords<String, String> records(String topic, String... values) {
        var list = new LinkedList<ConsumerRecord<String, String>>();
        for (var value : values) {
            var r = new ConsumerRecord<String, String>(topic, 0, 0, null, value);
            list.add(r);
        }
        var records = new HashMap<TopicPartition, List<ConsumerRecord<String, String>>>();
        records.put(new TopicPartition("...", 0), list);
        return new ConsumerRecords<>(records);
    }

    @SuppressWarnings("unchecked")
    private <T> void registerSerialization(T o, Class<T> c) {
        var rnd = Double.toString(Math.random());
        jsons.put(o, rnd);
        when(jsonUtils.toJson(eq(o))).thenReturn(rnd);
        when(jsonUtils.fromJson(eq(rnd), any(Class.class))).thenReturn(o);
        when(jsonUtils.fromJson(eq(rnd), any(TRef.class))).thenReturn(o);
    }

    private static void verifySubscribe(KafkaConsumer<String, String> con, String... topics) {
        verifySubscribe(con, 1, topics);
    }

    private static void verifySubscribe(KafkaConsumer<String, String> con, int times, String... topics) {
        verify(con, times(times)).subscribe(eq(Set.of(topics)));
    }
}