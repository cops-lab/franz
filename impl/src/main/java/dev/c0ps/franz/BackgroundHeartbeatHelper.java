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

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;

public class BackgroundHeartbeatHelper {

    private final Object sync = new Object();
    private final AtomicBoolean isSendingHeartbeats = new AtomicBoolean(false);
    private final Kafka kafka;
    private final long timeoutMS;

    private ExecutorService exec;

    public BackgroundHeartbeatHelper(Kafka kafka, long timeoutMS) {
        this.kafka = kafka;
        this.timeoutMS = timeoutMS;

    }

    public void enable() {
        synchronized (sync) {
            isSendingHeartbeats.set(true);
            if (exec == null) {
                exec = Executors.newFixedThreadPool(1);
                exec.submit(() -> {
                    while (isSendingHeartbeats.get()) {
                        kafka.sendHeartbeat();
                        try {
                            Thread.sleep(timeoutMS);
                        } catch (InterruptedException e) {
                            // ignore
                        }
                    }
                });
            }
        }
    }

    public void disable() {
        synchronized (sync) {
            isSendingHeartbeats.set(false);
            if (exec != null) {
                exec.shutdownNow();
                exec = null;
            }
        }
    }
}