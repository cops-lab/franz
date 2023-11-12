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

import static dev.c0ps.franz.Lane.ERROR;
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
import java.util.function.Consumer;

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

    private static final Duration TIMEOUT_ZEROISH = Duration.ofMillis(1);
    private static final Duration TIMEOUT_REGULAR = Duration.ofSeconds(10);

    private static final TRef<String> T_STRING = new TRef<String>() {};
    private static final BiConsumer<String, Lane> SOME_CB = (s, l) -> {};
    private static final BiConsumer<String, Lane> OTHER_CB = (s, l) -> {};
    private static final BiFunction<String, Throwable, ?> SOME_ERR_CB = (s, t) -> null;
    private static final Consumer<String> ERR_CB = s -> {};

    private JsonUtils jsonUtils;
    private KafkaConnector connector;
    private BackgroundHeartbeatHelper helper;
    private KafkaConsumer<String, String> consumerNorm;
    private KafkaConsumer<String, String> consumerPrio;
    private KafkaConsumer<String, String> consumerErr;
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
        consumerErr = mock(KafkaConsumer.class);
        producer = mock(KafkaProducer.class);

        when(connector.getConsumerConnection(NORMAL)).thenReturn(consumerNorm);
        when(connector.getConsumerConnection(PRIORITY)).thenReturn(consumerPrio);
        when(connector.getConsumerConnection(ERROR)).thenReturn(consumerErr);
        when(connector.getProducerConnection()).thenReturn(producer);

        sut = new KafkaImpl(jsonUtils, connector, true);
        sut.setBackgroundHeartbeatHelper(helper);
    }

    private void mockSubs(KafkaConsumer<String, String> con, String... topics) {
        when(con.subscription()).thenReturn(Set.of(topics));
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
        mockSubs(consumerNorm, "n1");
        mockSubs(consumerPrio, "p1");

        sut.subscribe("t", String.class, SOME_CB);
        verifySubscribe(consumerNorm, "n1", "t-NORMAL");
        verifySubscribe(consumerPrio, "p1", "t-PRIORITY");
    }

    @Test
    public void subscribesEndUpAtConnection_ClassErr() {
        mockSubs(consumerNorm, "n1");
        mockSubs(consumerPrio, "p1");

        sut.subscribe("t", String.class, SOME_CB, SOME_ERR_CB);
        verifySubscribe(consumerNorm, "n1", "t-NORMAL");
        verifySubscribe(consumerPrio, "p1", "t-PRIORITY");
    }

    @Test
    public void subscribesEndUpAtConnection_TRef() {
        mockSubs(consumerNorm, "n1");
        mockSubs(consumerPrio, "p1");

        sut.subscribe("t", T_STRING, SOME_CB);
        verifySubscribe(consumerNorm, "n1", "t-NORMAL");
        verifySubscribe(consumerPrio, "p1", "t-PRIORITY");
    }

    @Test
    public void subscribesEndUpAtConnection_TRefErr() {
        mockSubs(consumerNorm, "n1");
        mockSubs(consumerPrio, "p1");

        sut.subscribe("t", T_STRING, SOME_CB, SOME_ERR_CB);
        verifySubscribe(consumerNorm, "n1", "t-NORMAL");
        verifySubscribe(consumerPrio, "p1", "t-PRIORITY");
    }

    @Test
    public void subscribeErrorEndUpAtConnection_Class() {
        mockSubs(consumerErr, "e1");

        sut.subscribeErrors("t", String.class, ERR_CB);
        verifySubscribe(consumerErr, "e1", "t-ERROR");
    }

    @Test
    public void subscribeErrorEndUpAtConnection_TRef() {
        mockSubs(consumerErr, "e1");

        sut.subscribeErrors("t", T_STRING, ERR_CB);
        verifySubscribe(consumerErr, "e1", "t-ERROR");
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
        verify(consumerPrio).poll(eq(TIMEOUT_ZEROISH));
        verify(consumerNorm).poll(eq(TIMEOUT_ZEROISH));
        verify(consumerPrio).commitSync();
        verify(consumerNorm).commitSync();
    }

    @Test
    public void poll_waitsAfterNoMessages() {
        when(consumerNorm.poll(any(Duration.class))).thenReturn(ConsumerRecords.empty());
        when(consumerPrio.poll(any(Duration.class))).thenReturn(ConsumerRecords.empty());
        sut.poll();
        sut.poll();
        verify(consumerPrio).poll(eq(TIMEOUT_ZEROISH));
        verify(consumerPrio).poll(eq(Duration.ofSeconds(10)));
        verify(consumerNorm, times(2)).poll(eq(TIMEOUT_ZEROISH));
        verify(consumerPrio, times(2)).commitSync();
        verify(consumerNorm, times(2)).commitSync();
        verifyNoMoreInteractions(consumerNorm);
    }

    @Test
    public void poll_prioCausesHeartbeat() {
        when(consumerNorm.assignment()).thenReturn(Set.of(mock(TopicPartition.class)));
        when(consumerPrio.assignment()).thenReturn(Set.of(mock(TopicPartition.class)));

        when(consumerNorm.poll(any(Duration.class))).thenReturn(ConsumerRecords.empty());
        when(consumerPrio.poll(any(Duration.class))).thenReturn(records("t-PRIORITY", "a"));
        sut.subscribe("t", String.class, SOME_CB);
        sut.poll();

        // regular poll
        verify(consumerPrio).poll(TIMEOUT_ZEROISH);
        verify(consumerPrio).commitSync();

        // heartbeat
        verifySubscribe(consumerNorm, "t-NORMAL");
        verify(consumerNorm).subscription();
        verify(consumerNorm).assignment();
        verify(consumerNorm).pause(anySet());
        verify(consumerNorm).poll(TIMEOUT_ZEROISH);
        verify(consumerNorm).resume(anySet());
        verifyNoMoreInteractions(consumerNorm);
    }

    @Test
    @SuppressWarnings({ "rawtypes", "unchecked" })
    public void poll_allErrors() {
        var TP1 = mock(TopicPartition.class);
        when(consumerErr.assignment()).thenReturn(Set.of()).thenReturn(Set.of(TP1));
        when(consumerErr.poll(any(Duration.class))).thenReturn(new ConsumerRecords(new HashMap<>()));

        sut.pollAllErrors();

        verify(consumerErr, times(3)).assignment();
        verify(consumerErr).seekToBeginning(eq(Set.of(TP1)));
        verify(consumerErr).poll(TIMEOUT_ZEROISH);
        verify(consumerErr).poll(TIMEOUT_REGULAR);
    }

    @Test
    public void sendHeartbeat_hasSubscription() {
        when(consumerNorm.assignment()).thenReturn(Set.of(mock(TopicPartition.class)));
        when(consumerPrio.assignment()).thenReturn(Set.of(mock(TopicPartition.class)));

        sut.sendHeartbeat();

        verify(consumerNorm).assignment();
        verify(consumerNorm).pause(anySet());
        verify(consumerNorm).poll(TIMEOUT_ZEROISH);
        verify(consumerNorm).resume(anySet());
        verifyNoMoreInteractions(consumerNorm);

        verify(consumerPrio).assignment();
        verify(consumerPrio).pause(anySet());
        verify(consumerPrio).poll(TIMEOUT_ZEROISH);
        verify(consumerPrio).resume(anySet());
        verifyNoMoreInteractions(consumerPrio);
    }

    @Test
    public void sendHeartbeat_noSubscription() {
        when(consumerNorm.assignment()).thenReturn(Set.of());
        when(consumerPrio.assignment()).thenReturn(Set.of());

        sut.sendHeartbeat();

        verify(consumerNorm).assignment();
        verifyNoMoreInteractions(consumerNorm);

        verify(consumerPrio).assignment();
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