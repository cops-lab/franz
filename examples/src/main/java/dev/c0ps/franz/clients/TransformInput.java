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

import dev.c0ps.franz.Kafka;
import dev.c0ps.franz.data.SomeInputData;
import dev.c0ps.io.TRef;

public class TransformInput {

    public static final String OUT_TOPIC = "example.out";

    private final Kafka kafka;

    public TransformInput(Kafka kafka) {
        this.kafka = kafka;
    }

    public void run() {
        System.out.printf("Subscribing to %s, publishing to %s ...\n", ReadInput.INPUT_TOPIC, OUT_TOPIC);

        // either subscribe and define the class of the message ...
        kafka.subscribe(ReadInput.INPUT_TOPIC, SomeInputData.class, (in, l) -> {
            System.out.printf("Message via .class: %s\n", in);

            var out = in.a.toUpperCase();

            System.out.printf("Publishing transformation: %s\n", out);
            kafka.publish(out, OUT_TOPIC, l);
        });

        // ... or use TRef<..>, especially if the message type is generic
        kafka.subscribe(ReadInput.INPUT_TOPIC, new TRef<SomeInputData>() {}, (in, l) -> {
            System.out.printf("Message via TRef: %s\n", in);
        });

        // consume incoming messages in an endless loop
        while (true) {
            kafka.poll();
        }
    }
}