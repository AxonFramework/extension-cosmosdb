/*
 * Copyright (c) 2010-2024. Axon Framework
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

package org.axonframework.extensions.cosmosdb.eventsourcing.tokenstore;

import org.axonframework.eventhandling.GlobalSequenceTrackingToken;
import org.axonframework.eventhandling.TrackingToken;
import org.axonframework.serialization.SerializedObject;
import org.axonframework.serialization.Serializer;
import org.axonframework.serialization.json.JacksonSerializer;
import org.junit.jupiter.api.*;

import java.time.Duration;
import java.util.concurrent.atomic.AtomicReference;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.*;

class CosmosTokenItemTest {

    private final Serializer serializer = JacksonSerializer.defaultSerializer();
    private static final String DEFAULT_PROCESSOR_NAME = "processorName";
    private static final int DEFAULT_SEGMENT = 2;
    private static final String DEFAULT_OWNER = "42";

    @Test
    void createAndToStringWorks() {
        CosmosTokenItem testSubject = fromToken(new GlobalSequenceTrackingToken(100L));

        assertNotNull(testSubject);
        assertEquals(DEFAULT_SEGMENT, testSubject.getSegment());
        assertEquals(DEFAULT_OWNER, testSubject.getOwner());
        assertEquals(GlobalSequenceTrackingToken.class.getName(), testSubject.getTokenType());
        assertTrue(testSubject.getToken().length > 0);
        assertNotNull(testSubject.toString());
    }

    @Test
    void initialTokenCreationWorks() {
        CosmosTokenItem testSubject = initialToken(new GlobalSequenceTrackingToken(-1L));

        assertNotNull(testSubject);
        assertEquals(DEFAULT_SEGMENT, testSubject.getSegment());
        assertNull(testSubject.getOwner());
        assertEquals(GlobalSequenceTrackingToken.class.getName(), testSubject.getTokenType());
        assertTrue(testSubject.getToken().length > 0);
    }

    @Test
    void releaseWorksAndUpdatesTheTimeStamp() {
        CosmosTokenItem cosmosTokenItem = fromToken(new GlobalSequenceTrackingToken(100L));
        AtomicReference<CosmosTokenItem> testSubject = new AtomicReference<>();
        await().pollDelay(Duration.ofMillis(2L))
               .atMost(Duration.ofMillis(200L))
               .untilAsserted(
                       () -> assertDoesNotThrow(() -> testSubject.set(cosmosTokenItem.release()))
               );
        assertNotNull(testSubject.get());
        assertNull(testSubject.get().getOwner());
        assertNotEquals(cosmosTokenItem.getTimestamp(), testSubject.get().getTimestamp());
    }

    private CosmosTokenItem fromToken(TrackingToken token) {
        SerializedObject<byte[]> serializedObject = serializer.serialize(token, byte[].class);
        return new CosmosTokenItem(DEFAULT_PROCESSOR_NAME,
                                   DEFAULT_SEGMENT,
                                   serializedObject.getData(),
                                   serializedObject.getType().getName(),
                                   DEFAULT_OWNER);
    }

    private CosmosTokenItem initialToken(TrackingToken token) {
        SerializedObject<byte[]> serializedObject = serializer.serialize(token, byte[].class);
        return CosmosTokenItem.initialToken(DEFAULT_PROCESSOR_NAME,
                                            DEFAULT_SEGMENT,
                                            serializedObject.getData(),
                                            serializedObject.getType().getName());
    }
}
