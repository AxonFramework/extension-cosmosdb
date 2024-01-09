package org.axonframework.extensions.cosmosdb.integration;

import org.axonframework.eventhandling.GlobalSequenceTrackingToken;
import org.axonframework.eventhandling.TrackingToken;
import org.axonframework.eventhandling.tokenstore.TokenStore;
import org.axonframework.extensions.cosmosdb.eventsourcing.tokenstore.CosmosTokenStore;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.io.*;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.context.annotation.EnableMBeanExport;
import org.springframework.jmx.support.RegistrationPolicy;
import org.springframework.test.context.ContextConfiguration;
import org.testcontainers.containers.CosmosDBEmulatorContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.cert.CertificateException;

import static org.junit.jupiter.api.Assertions.*;

@Testcontainers
class TokenStoreIntegrationTest {

    @Container
    private static final CosmosDBEmulatorContainer COSMOS_CONTAINER = new CosmosDBEmulatorContainer(
            DockerImageName.parse("mcr.microsoft.com/cosmosdb/linux/azure-cosmos-emulator:latest")
    );

    @TempDir
    private static Path tempFolder;

    private ApplicationContextRunner testApplicationContext;

    @BeforeAll
    static void setup() throws IOException, CertificateException, KeyStoreException, NoSuchAlgorithmException {
        Path keyStoreFile = tempFolder.resolve("azure-cosmos-emulator.keystore");
        KeyStore keyStore = COSMOS_CONTAINER.buildNewKeyStore();
        keyStore.store(Files.newOutputStream(keyStoreFile.toFile().toPath()),
                       COSMOS_CONTAINER.getEmulatorKey().toCharArray());

        System.setProperty("javax.net.ssl.trustStore", keyStoreFile.toString());
        System.setProperty("javax.net.ssl.trustStorePassword", COSMOS_CONTAINER.getEmulatorKey());
        System.setProperty("javax.net.ssl.trustStoreType", "PKCS12");
    }

    @BeforeEach
    void setUp() {
        testApplicationContext = new ApplicationContextRunner()
                .withPropertyValues("axon.axonserver.enabled=false");
    }

    @Test
    void tokenStoreWillUseCosmosDB() {
        testApplicationContext
                .withPropertyValues("spring.cloud.azure.cosmos.endpoint=" + COSMOS_CONTAINER.getEmulatorEndpoint())
                .withPropertyValues("spring.cloud.azure.cosmos.key=" + COSMOS_CONTAINER.getEmulatorKey())
                .withPropertyValues("spring.cloud.azure.cosmos.database=someProjection")
                .withPropertyValues("spring.cloud.azure.cosmos.endpoint-discovery-enabled=false")
                .withPropertyValues("spring.cloud.azure.cosmos.connection-mode=gateway")
                .withUserConfiguration(DefaultContext.class)
                .run(context -> {
                    TokenStore tokenStore = context.getBean(TokenStore.class);
                    assertNotNull(tokenStore);
                    assertInstanceOf(CosmosTokenStore.class, tokenStore);
                    testTokenStore(tokenStore);
                });
    }

    private void testTokenStore(TokenStore tokenStore) {
        String testProcessorName = "testProcessorName";
        int testSegment = 9;
        tokenStore.initializeTokenSegments(testProcessorName, testSegment + 1);
        assertNull(tokenStore.fetchToken(testProcessorName, testSegment));
        TrackingToken token = new GlobalSequenceTrackingToken(1L);
        tokenStore.storeToken(token, testProcessorName, testSegment);
        assertEquals(token, tokenStore.fetchToken(testProcessorName, testSegment));
    }

    @ContextConfiguration
    @EnableAutoConfiguration
    @EnableMBeanExport(registration = RegistrationPolicy.IGNORE_EXISTING)
    public static class DefaultContext {

    }
}