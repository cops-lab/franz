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

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

public class BackgroundHeartbeatHelperTest {

    private static final int VARIANCE = 2;

    private Kafka kafka;
    private int numHeartbeats;

    private BackgroundHeartbeatHelper sut;

    @BeforeEach
    public void setup() {
        numHeartbeats = 0;
        kafka = mock(Kafka.class);
        doAnswer(new Answer<Object>() {
            @Override
            public Object answer(InvocationOnMock invocation) throws Throwable {
                numHeartbeats++;
                return null;
            }

        }).when(kafka).sendHeartbeat();
    }

    @Test
    public void doesNotCountOnStart() {
        init(50);
        sleep(150);
        assertMinMaxHeartbeats(0, 0);
    }

    @Test
    public void doesCountWhenEnabled() {
        init(50);
        sut.enable();
        sleep(300);
        assertMinMaxHeartbeats(6 - VARIANCE, 6 + VARIANCE);
    }

    @Test
    public void secondEnabledDoesNotCountTwice() {
        init(50);
        sut.enable();
        sut.enable();
        sleep(300);
        assertMinMaxHeartbeats(6 - VARIANCE, 6 + VARIANCE);
    }

    @Test
    public void doesStopWhenDisabled() {
        init(50);
        sut.enable();
        sleep(150);
        sut.disable();
        sleep(10);
        var tmp = numHeartbeats;
        assertTrue(tmp > 0);
        sleep(150);
        assertMinMaxHeartbeats(tmp, tmp);
    }

    @Test
    public void secondDisableDoesNotCrash() {
        init(50);
        sut.enable();
        sut.disable();
        sut.disable();
    }

    private void init(int timeoutMS) {
        sut = new BackgroundHeartbeatHelper(kafka, timeoutMS);
    }

    private void assertMinMaxHeartbeats(int min, int max) {
        assertTrue(min <= numHeartbeats, String.format("Expected a minimum of %d heartbeats, was %d", min, numHeartbeats));
        assertTrue(numHeartbeats <= max, String.format("Expected a maximum of %d heartbeats, was %d", max, numHeartbeats));
    }

    private void sleep(int ms) {
        try {
            Thread.sleep(ms);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}