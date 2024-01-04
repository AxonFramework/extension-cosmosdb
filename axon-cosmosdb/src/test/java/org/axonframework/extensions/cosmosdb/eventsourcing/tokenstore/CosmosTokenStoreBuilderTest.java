package org.axonframework.extensions.cosmosdb.eventsourcing.tokenstore;

import org.axonframework.common.AxonConfigurationException;
import org.axonframework.serialization.json.JacksonSerializer;
import org.junit.jupiter.api.*;

import static org.junit.jupiter.api.Assertions.*;

class CosmosTokenStoreBuilderTest {

    private CosmosTokenStore.Builder testSubject;

    @BeforeEach
    void setBuilder() {
        testSubject = CosmosTokenStore.builder();
    }

    @Test
    void cantSetSerializerToNullValue() {
        assertThrows(AxonConfigurationException.class, () -> testSubject.serializer(null));
    }

    @Test
    void cantSetDatabaseNameToEmptyValue() {
        assertThrows(AxonConfigurationException.class, () -> testSubject.databaseName(""));
    }

    @Test
    void cantSetClaimTimeOutToNull() {
        assertThrows(AxonConfigurationException.class, () -> testSubject.claimTimeout(null));
    }

    @Test
    void needToProvideClient() {
        testSubject.serializer(JacksonSerializer.defaultSerializer());
        assertThrows(AxonConfigurationException.class, () -> testSubject.validate());
    }
}
