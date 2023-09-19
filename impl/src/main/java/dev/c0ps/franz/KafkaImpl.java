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

import java.time.Duration;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Function;

import org.apache.kafka.clients.consumer.CommitFailedException;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.errors.WakeupException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import dev.c0ps.io.JsonUtils;
import dev.c0ps.io.TRef;
import jakarta.inject.Inject;
import jakarta.inject.Named;

public class KafkaImpl implements Kafka {

    private static final Logger LOG = LoggerFactory.getLogger(KafkaImpl.class);

    // Avoid special polling behavior with Duration.ZERO timeout, by using a small timeout
    private static final Duration POLL_TIMEOUT_ZEROISH = Duration.ofMillis(100);
    private static final Duration POLL_TIMEOUT_PRIO = Duration.ofSeconds(10);
    private static final Object NONE = new Object();

    private static final String EXT_NORM = "-" + Lane.NORMAL;
    private static final String EXT_PRIO = "-" + Lane.PRIORITY;
    private static final String EXT_ERR = "-" + Lane.ERROR;

    private final JsonUtils jsonUtils;
    private final boolean shouldAutoCommit;

    private final KafkaConsumer<String, String> connNorm;
    private final KafkaConsumer<String, String> connPrio;
    private final KafkaProducer<String, String> producer;

    // keep track of subscriptions to enable incremental subscriptions
    private final Set<String> subsNorm = new HashSet<>();
    private final Set<String> subsPrio = new HashSet<>();

    private final Map<String, String> baseTopics = new HashMap<>();
    private final Map<String, Set<Callback<?>>> callbacks = new HashMap<>();

    private BackgroundHeartbeatHelper helper;
    private boolean hadMessages = true;

    @Inject
    public KafkaImpl(JsonUtils jsonUtils, KafkaConnector connector, @Named("KafkaImpl.shouldAutoCommit") boolean shouldAutoCommit) {
        this.jsonUtils = jsonUtils;
        this.shouldAutoCommit = shouldAutoCommit;
        connNorm = connector.getConsumerConnection(NORMAL);
        connPrio = connector.getConsumerConnection(PRIORITY);
        producer = connector.getProducerConnection();
    }

    @Override
    public void sendHeartbeat() {
        LOG.debug("Sending a heartbeat on both consumer connections ...");
        sendHeartBeat(connNorm);
        sendHeartBeat(connPrio);
    }

    public void setBackgroundHeartbeatHelper(BackgroundHeartbeatHelper helper) {
        if (this.helper != null) {
            this.helper.disable();
        }
        this.helper = helper;
    }

    private void assertBackgroundHelper() {
        if (helper == null) {
            throw new IllegalStateException("No BackgroundHeartbeatHelper configured in this KafkaImpl instance");
        }
    }

    @Override
    public void enableBackgroundHeartbeat() {
        assertBackgroundHelper();
        helper.enable();
    }

    @Override
    public void disableBackgroundHeartbeat() {
        assertBackgroundHelper();
        helper.disable();
    }

    @Override
    public void stop() {
        if (helper != null) {
            disableBackgroundHeartbeat();
        }
        connNorm.wakeup();
        connNorm.close();
        connPrio.wakeup();
        connPrio.close();
        producer.close();
    }

    @Override
    public <T> void subscribe(String topic, Class<T> type, BiConsumer<T, Lane> callback) {
        subscribe(topic, type, callback, (x, y) -> NONE);
    }

    @Override
    public <T> void subscribe(String topic, Class<T> type, BiConsumer<T, Lane> callback, BiFunction<T, Throwable, ?> errors) {
        subscribe(topic, new Callback<T>(type, callback, errors));
    }

    @Override
    public <T> void subscribe(String topic, TRef<T> typeRef, BiConsumer<T, Lane> callback) {
        subscribe(topic, typeRef, callback, (x, y) -> NONE);
    }

    @Override
    public <T> void subscribe(String topic, TRef<T> typeRef, BiConsumer<T, Lane> callback, BiFunction<T, Throwable, ?> errors) {
        subscribe(topic, new Callback<T>(typeRef, callback, errors));
    }

    private <T> void subscribe(String topic, Callback<T> cb) {
        for (var lane : Lane.values()) {
            getCallbacks(topic, lane).add(cb);
        }

        subsNorm.add(combine(topic, NORMAL));
        connNorm.subscribe(subsNorm);
        LOG.debug("Subscribed ({}): {}", NORMAL, subsNorm);

        subsPrio.add(combine(topic, PRIORITY));
        connPrio.subscribe(subsPrio);
        LOG.debug("Subscribed ({}): {}", PRIORITY, subsPrio);
    }

    private Set<Callback<?>> getCallbacks(String baseTopic, Lane lane) {
        var combinedTopic = combine(baseTopic, lane);
        baseTopics.put(combinedTopic, baseTopic);

        Set<Callback<?>> vals;
        if (!callbacks.containsKey(combinedTopic)) {
            vals = new HashSet<Callback<?>>();
            callbacks.put(combinedTopic, vals);
        } else {
            vals = callbacks.get(combinedTopic);
        }
        return vals;
    }

    @Override
    public <T> void publish(T obj, String topic, Lane lane) {
        LOG.debug("Publishing to {} ({})", topic, lane);
        String json = jsonUtils.toJson(obj);
        var combinedTopic = combine(topic, lane);
        var record = new ProducerRecord<String, String>(combinedTopic, json);
        producer.send(record);
        producer.flush();
    }

    @Override
    public void poll() {
        LOG.debug("Polling ...");
        // don't wait if any lane had messages, otherwise, only wait in PRIO
        var timeout = hadMessages ? POLL_TIMEOUT_ZEROISH : POLL_TIMEOUT_PRIO;
        if (process(connPrio, Lane.PRIORITY, timeout)) {
            // make sure the session does not time out
            sendHeartBeat(connNorm);
        } else {
            process(connNorm, NORMAL, POLL_TIMEOUT_ZEROISH);
        }
    }

    @Override
    public void commit() {
        LOG.debug("Committing ...");
        connPrio.commitSync();
        connNorm.commitSync();
    }

    private boolean process(KafkaConsumer<String, String> con, Lane lane, Duration timeout) {
        LOG.debug("Processing record ...");
        hadMessages = false;
        try {
            wakeup(con);
            for (var r : con.poll(timeout)) {
                LOG.debug("Received message on ('combined') topic {}, invoking callbacks ...", r.topic());
                hadMessages = true;
                var json = r.value();
                var cbs = callbacks.get(r.topic());
                for (var cb : cbs) {
                    cb.exec(r.topic(), json, lane);
                }
            }
            if (shouldAutoCommit) {
                con.commitSync();
            }
        } catch (WakeupException e) {
            // used by Kafka to interrupt long polls, can be ignored
        } catch (CommitFailedException e) {
            LOG.warn("Offset commit failed, stopping Kafka ...");
            stop();
            throw new KafkaException(e);
        }
        return hadMessages;
    }

    private static void wakeup(KafkaConsumer<String, String> con) {
        try {
            con.wakeup();
        } catch (WakeupException e) {
            // can be safely ignored, exception is thrown when poll was waiting
        }
    }

    private String combine(String topic, Lane lane) {
        return new StringBuilder().append(topic).append(getSuffix(lane)).toString();
    }

    protected String getSuffix(Lane lane) {
        switch (lane) {
        case PRIORITY:
            return EXT_PRIO;
        case ERROR:
            return EXT_ERR;
        case NORMAL:
            return EXT_NORM;
        default:
            throw new IllegalStateException();
        }
    }

    private static void sendHeartBeat(KafkaConsumer<?, ?> c) {
        LOG.debug("Sending heartbeat ...");
        // See https://stackoverflow.com/a/43722731
        var partitions = c.assignment();
        c.pause(partitions);
        c.poll(POLL_TIMEOUT_ZEROISH);
        c.resume(partitions);
    }

    private class Callback<T> {

        private final Function<String, T> deserializer;
        private final BiConsumer<T, Lane> callback;
        private final BiFunction<T, Throwable, ?> errors;

        private Callback(Class<T> type, BiConsumer<T, Lane> callback, BiFunction<T, Throwable, ?> errors) {
            this.callback = callback;
            this.errors = errors;
            this.deserializer = json -> {
                return jsonUtils.fromJson(json, type);
            };
        }

        private Callback(TRef<T> typeRef, BiConsumer<T, Lane> callback, BiFunction<T, Throwable, ?> errors) {
            this.callback = callback;
            this.errors = errors;
            this.deserializer = json -> {
                return jsonUtils.fromJson(json, typeRef);
            };
        }

        public void exec(String combinedTopic, String json, Lane lane) {
            T obj = null;
            try {
                obj = deserializer.apply(json);
                callback.accept(obj, lane);
            } catch (Exception e) {
                var err = errors.apply(obj, e);
                // check instance equality!
                if (err == NONE) {
                    var msg = new StringBuilder("Unhandled exception when processing ").append(json).toString();
                    LOG.error(msg, e);
                } else {
                    var baseTopic = baseTopics.get(combinedTopic);
                    publish(err, baseTopic, ERROR);
                }
            }
        }
    }
}