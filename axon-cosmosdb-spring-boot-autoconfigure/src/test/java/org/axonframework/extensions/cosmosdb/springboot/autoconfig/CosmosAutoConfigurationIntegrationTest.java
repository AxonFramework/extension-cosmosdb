package org.axonframework.extensions.cosmosdb.springboot.autoconfig;

import com.azure.cosmos.CosmosAsyncClient;
import com.azure.cosmos.CosmosClientBuilder;
import org.axonframework.common.ReflectionUtils;
import org.axonframework.eventhandling.GlobalSequenceTrackingToken;
import org.axonframework.eventhandling.TrackingToken;
import org.axonframework.eventhandling.tokenstore.TokenStore;
import org.axonframework.extensions.cosmosdb.eventsourcing.tokenstore.CosmosTokenStore;
import org.axonframework.serialization.Serializer;
import org.axonframework.serialization.json.JacksonSerializer;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.io.*;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.EnableMBeanExport;
import org.springframework.context.annotation.Primary;
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
import java.time.Duration;
import java.time.temporal.TemporalAmount;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

@Testcontainers
class CosmosAutoConfigurationIntegrationTest {

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
    void setTokenStoreIsSetupCorrectly() {
        testApplicationContext
                .withUserConfiguration(DefaultContext.class)
                .withPropertyValues("axon.eventhandling.tokenstore.claim-timeout=15s")
                .run(context -> {
                    Map<String, TokenStore> tokenStores =
                            context.getBeansOfType(TokenStore.class);
                    assertTrue(tokenStores.containsKey("tokenStore"));
                    TokenStore tokenStore = tokenStores.get("tokenStore");
                    TrackingToken testToken = new GlobalSequenceTrackingToken(42);
                    tokenStore.initializeSegment(null, "someName", 5);
                    tokenStore.storeToken(testToken, "someName", 5);
                    assertEquals(testToken, tokenStore.fetchToken("someName", 5));
                });
    }

    @Test
    void setTokenStoreClaimTimeout() {
        testApplicationContext
                .withUserConfiguration(DefaultContext.class)
                .withPropertyValues("axon.eventhandling.tokenstore.claim-timeout=15s")
                .run(context -> {
                    Map<String, TokenStore> tokenStores =
                            context.getBeansOfType(TokenStore.class);
                    assertTrue(tokenStores.containsKey("tokenStore"));
                    TokenStore tokenStore = tokenStores.get("tokenStore");
                    TemporalAmount tokenClaimInterval = ReflectionUtils.getFieldValue(
                            CosmosTokenStore.class.getDeclaredField("claimTimeout"), tokenStore
                    );
                    assertEquals(Duration.ofSeconds(15L), tokenClaimInterval);
                });
    }

    @ContextConfiguration
    @EnableAutoConfiguration
    @EnableMBeanExport(registration = RegistrationPolicy.IGNORE_EXISTING)
    public static class DefaultContext {

        @Bean
        @Primary
        public Serializer serializer() {
            return JacksonSerializer.defaultSerializer();
        }

        @Bean
        @Qualifier("azureCosmosAsyncClient")
        public CosmosAsyncClient client() {
            return new CosmosClientBuilder()
                    .gatewayMode()
                    .endpointDiscoveryEnabled(false)
                    .endpoint(COSMOS_CONTAINER.getEmulatorEndpoint())
                    .key(COSMOS_CONTAINER.getEmulatorKey())
                    .buildAsyncClient();
        }
    }
}