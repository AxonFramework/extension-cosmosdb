package org.axonframework.extensions.cosmosdb.eventsourcing.tokenstore;

import com.azure.cosmos.CosmosAsyncClient;
import com.azure.cosmos.CosmosClientBuilder;
import com.thoughtworks.xstream.XStream;
import org.axonframework.common.AxonConfigurationException;
import org.axonframework.eventhandling.GlobalSequenceTrackingToken;
import org.axonframework.eventhandling.Segment;
import org.axonframework.eventhandling.TrackingToken;
import org.axonframework.eventhandling.tokenstore.TokenStore;
import org.axonframework.eventhandling.tokenstore.UnableToClaimTokenException;
import org.axonframework.eventhandling.tokenstore.UnableToInitializeTokenException;
import org.axonframework.serialization.Serializer;
import org.axonframework.serialization.json.JacksonSerializer;
import org.axonframework.serialization.xml.XStreamSerializer;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.io.*;
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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.*;

@Testcontainers
class CosmosTokenStoreIntegrationTest {

    @Container
    private static final CosmosDBEmulatorContainer COSMOS_CONTAINER = new CosmosDBEmulatorContainer(
            DockerImageName.parse("mcr.microsoft.com/cosmosdb/linux/azure-cosmos-emulator:latest")
    );

    @TempDir
    private static Path tempFolder;
    private CosmosTokenStore testSubject;
    private CosmosTokenStore otherTokenStore;

    private List<CosmosAsyncClient> clients = new ArrayList<>();

    private final Serializer serializer = JacksonSerializer.defaultSerializer();
    private static final String DATABASE_NAME = "someAxon";
    private static final String TEST_OWNER = "testOwner";
    private static final String OTHER_OWNER = "otherOwner";
    private static final String PROCESSOR_ONE = "pg1";
    private static final String PROCESSOR_TWO = "pg2";
    private static final int TEST_SEGMENT = 9;
    private static final int TEST_SEGMENT_COUNT = 10;

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
    void createNewTestSubject() {
        testSubject = getTokenStore(TEST_OWNER);
        otherTokenStore = getTokenStore(OTHER_OWNER);
    }

    @AfterEach
    void cleanup() {
        getClient().getDatabase(DATABASE_NAME).delete().block();
        clients.forEach(CosmosAsyncClient::close);
        clients = new ArrayList<>();
    }

    @Test
    void getStorageIdentifier() {
        Optional<String> result = testSubject.retrieveStorageIdentifier();
        assertTrue(result.isPresent());
        UUID uuid = UUID.fromString(result.get());
        assertNotNull(uuid);

        Optional<String> otherResult = testSubject.retrieveStorageIdentifier();
        assertEquals(result, otherResult);
    }

    @Test
    void concurrentIdentifierCallsReturnSame() throws InterruptedException {
        Optional<String> result = testConcurrencyBothSameAnswer(
                () -> testSubject.retrieveStorageIdentifier(),
                () -> otherTokenStore.retrieveStorageIdentifier());
        assertTrue(result.isPresent());
    }

    @Test
    void claimAndUpdateToken() {
        testSubject.initializeTokenSegments(PROCESSOR_ONE, TEST_SEGMENT_COUNT);
        assertNull(testSubject.fetchToken(PROCESSOR_ONE, TEST_SEGMENT));
        TrackingToken token = new GlobalSequenceTrackingToken(1L);
        testSubject.storeToken(token, PROCESSOR_ONE, TEST_SEGMENT);
        assertEquals(token, testSubject.fetchToken(PROCESSOR_ONE, TEST_SEGMENT));
    }

    @Test
    void initializeTokens() {
        testSubject.initializeTokenSegments(PROCESSOR_ONE, TEST_SEGMENT_COUNT);

        int[] actual = testSubject.fetchSegments(PROCESSOR_ONE);
        Arrays.sort(actual);
        assertArrayEquals(new int[]{0, 1, 2, 3, 4, 5, 6, 7, 8, 9}, actual);
    }

    @Test
    void initializeTokensAtGivenPosition() {
        testSubject.initializeTokenSegments(PROCESSOR_TWO, TEST_SEGMENT_COUNT, new GlobalSequenceTrackingToken(10));

        int[] actual = testSubject.fetchSegments(PROCESSOR_TWO);
        Arrays.sort(actual);
        assertArrayEquals(new int[]{0, 1, 2, 3, 4, 5, 6, 7, 8, 9}, actual);

        for (int segment : actual) {
            assertEquals(new GlobalSequenceTrackingToken(10), testSubject.fetchToken(PROCESSOR_TWO, segment));
        }
    }

    @Test
    void initializeTokensWhileAlreadyPresentWithThrow() {
        testSubject.initializeTokenSegments(PROCESSOR_ONE, TEST_SEGMENT_COUNT);
        assertThrows(
                UnableToClaimTokenException.class,
                () -> testSubject.initializeTokenSegments(PROCESSOR_ONE, TEST_SEGMENT_COUNT)
        );
    }

    @Test
    void attemptToClaimAlreadyClaimedToken() {
        testSubject.initializeTokenSegments(PROCESSOR_ONE, TEST_SEGMENT_COUNT);
        assertNull(testSubject.fetchToken(PROCESSOR_ONE, TEST_SEGMENT));

        TrackingToken token = new GlobalSequenceTrackingToken(1L);
        testSubject.storeToken(token, PROCESSOR_ONE, TEST_SEGMENT);

        assertThrows(
                UnableToClaimTokenException.class,
                () -> otherTokenStore.storeToken(token, PROCESSOR_ONE, TEST_SEGMENT)
        );
    }

    @Test
    void attemptToExtendClaimOnAlreadyClaimedTokenWillThrow() {
        testSubject.initializeTokenSegments(PROCESSOR_ONE, TEST_SEGMENT_COUNT);
        assertNull(testSubject.fetchToken(PROCESSOR_ONE, TEST_SEGMENT));

        assertThrows(
                UnableToClaimTokenException.class,
                () -> otherTokenStore.extendClaim(PROCESSOR_ONE, TEST_SEGMENT)
        );
    }

    @Test
    void claimAndExtend() {
        TrackingToken token = new GlobalSequenceTrackingToken(1L);
        testSubject.initializeSegment(token, PROCESSOR_ONE, TEST_SEGMENT);

        testSubject.storeToken(token, PROCESSOR_ONE, TEST_SEGMENT);
        assertDoesNotThrow(() -> testSubject.extendClaim(PROCESSOR_ONE, TEST_SEGMENT));
    }

    @Test
    void releaseClaimAndExtendClaim() {
        testSubject.initializeTokenSegments(PROCESSOR_ONE, TEST_SEGMENT_COUNT);
        TrackingToken token = new GlobalSequenceTrackingToken(1L);
        testSubject.storeToken(token, PROCESSOR_ONE, TEST_SEGMENT);

        testSubject.releaseClaim(PROCESSOR_ONE, TEST_SEGMENT);
        assertThrows(
                UnableToClaimTokenException.class,
                () -> otherTokenStore.extendClaim(PROCESSOR_ONE, TEST_SEGMENT)
        );
    }

    @Test
    void fetchSegmentsShouldReturnInitializedOnes() {
        testSubject.initializeTokenSegments(PROCESSOR_ONE, 3);
        testSubject.initializeTokenSegments(PROCESSOR_TWO, 1);

        assertArrayEquals(new int[]{0, 1, 2}, testSubject.fetchSegments(PROCESSOR_ONE));
        assertArrayEquals(new int[]{0}, testSubject.fetchSegments(PROCESSOR_TWO));
        assertArrayEquals(new int[0], testSubject.fetchSegments("processor_three"));
    }

    @Test
    void fetchAvailableSegmentsShouldReturnCorrectSegments() {
        testSubject.initializeTokenSegments(PROCESSOR_ONE, 3);
        otherTokenStore.fetchToken(PROCESSOR_ONE, 0);

        List<Segment> availableSegments = testSubject.fetchAvailableSegments(PROCESSOR_ONE);
        assertEquals(2, availableSegments.size());
        assertEquals(3, otherTokenStore.fetchAvailableSegments(PROCESSOR_ONE).size());
        assertEquals(Segment.computeSegment(1, 0, 1, 2), availableSegments.get(0));
        assertEquals(Segment.computeSegment(2, 0, 1, 2), availableSegments.get(1));
    }

    @Test
    void fromFourConcurrentCallsOnlyOneShouldStoreTheTokenSuccessfully() throws Exception {
        final int attempts = 4;
        ExecutorService executorService = Executors.newFixedThreadPool(attempts);

        List<Future<Integer>> futures = new ArrayList<>();
        for (int i = 0; i < attempts; i++) {
            final int iteration = i;
            Future<Integer> future = executorService.submit(() -> {
                try {
                    String owner = String.valueOf(iteration);
                    TokenStore tokenStore = getTokenStore(owner);
                    GlobalSequenceTrackingToken token = new GlobalSequenceTrackingToken(iteration);
                    tokenStore.initializeSegment(token, PROCESSOR_ONE, TEST_SEGMENT);
                    tokenStore.storeToken(token, PROCESSOR_ONE, TEST_SEGMENT);
                    return iteration;
                } catch (UnableToClaimTokenException exception) {
                    return null;
                }
            });
            futures.add(future);
        }
        executorService.shutdown();
        assertTrue(executorService.awaitTermination(10, TimeUnit.SECONDS));

        List<Future<Integer>> successfulAttempts =
                futures.stream()
                       .filter(future -> {
                           try {
                               return future.get() != null;
                           } catch (InterruptedException | ExecutionException e) {
                               return false;
                           }
                       })
                       .collect(Collectors.toList());
        assertEquals(1, successfulAttempts.size());

        Integer iterationOfSuccessfulAttempt = successfulAttempts.get(0)
                                                                 .get();
        TokenStore tokenStore = getTokenStore(String.valueOf(iterationOfSuccessfulAttempt));

        assertEquals(new GlobalSequenceTrackingToken(iterationOfSuccessfulAttempt),
                     tokenStore.fetchToken(PROCESSOR_ONE, TEST_SEGMENT));
    }

    @Test
    void storeAndFetchTokenResultsInTheSameTokenWithXStreamSerializer() {
        TokenStore xStreamTokenStore = CosmosTokenStore.builder()
                                                       .client(getClient())
                                                       .databaseName(DATABASE_NAME)
                                                       .serializer(XStreamSerializer.builder()
                                                                                    .xStream(new XStream())
                                                                                    .build())
                                                       .nodeId(TEST_OWNER)
                                                       .build();
        TrackingToken testToken = new GlobalSequenceTrackingToken(100);
        xStreamTokenStore.initializeSegment(testToken, PROCESSOR_ONE, TEST_SEGMENT);

        xStreamTokenStore.storeToken(testToken, PROCESSOR_ONE, TEST_SEGMENT);
        TrackingToken resultToken = xStreamTokenStore.fetchToken(PROCESSOR_ONE, TEST_SEGMENT);
        assertEquals(testToken, resultToken);
    }

    @Test
    void storeAndFetchTokenResultsInTheSameTokenWithJacksonSerializer() {
        TrackingToken testToken = new GlobalSequenceTrackingToken(100);
        int testSegment = 0;

        testSubject.initializeSegment(testToken, PROCESSOR_ONE, testSegment);

        testSubject.storeToken(testToken, PROCESSOR_ONE, testSegment);
        TrackingToken resultToken = testSubject.fetchToken(PROCESSOR_ONE, testSegment);
        assertEquals(testToken, resultToken);
    }

    @Test
    void requiresExplicitSegmentInitializationReturnsTrue() {
        assertTrue(testSubject.requiresExplicitSegmentInitialization());
    }

    @Test
    void initializeSegmentForNullTokenOnlyCreatesSegments() {
        testSubject.initializeSegment(null, PROCESSOR_ONE, TEST_SEGMENT);

        int[] actual = testSubject.fetchSegments(PROCESSOR_ONE);
        Arrays.sort(actual);
        assertArrayEquals(new int[]{TEST_SEGMENT}, actual);

        assertNull(testSubject.fetchToken(PROCESSOR_ONE, TEST_SEGMENT));
    }

    @Test
    void initializeSegmentInsertsTheProvidedTokenAndInitializesTheGivenSegment() {
        GlobalSequenceTrackingToken testToken = new GlobalSequenceTrackingToken(100);
        testSubject.initializeSegment(testToken, PROCESSOR_ONE, TEST_SEGMENT);

        int[] actual = testSubject.fetchSegments(PROCESSOR_ONE);
        Arrays.sort(actual);
        assertArrayEquals(new int[]{TEST_SEGMENT}, actual);

        TrackingToken resultToken = testSubject.fetchToken(PROCESSOR_ONE, TEST_SEGMENT);
        assertEquals(testToken, resultToken);
    }

    @Test
    void testInitializeSegmentThrowsUnableToInitializeTokenExceptionForDuplicateKey() {
        GlobalSequenceTrackingToken testToken = new GlobalSequenceTrackingToken(100);
        testSubject.initializeSegment(testToken, PROCESSOR_ONE, TEST_SEGMENT);
        // Initializes the given token twice, causing the exception
        assertThrows(
                UnableToInitializeTokenException.class,
                () -> testSubject.initializeSegment(testToken, PROCESSOR_ONE, TEST_SEGMENT)
        );
    }

    @Test
    void deleteTokenRemovesTheSpecifiedToken() {
        testSubject.initializeSegment(null, PROCESSOR_ONE, TEST_SEGMENT);
        // Claim the token by fetching it to be able to delete it
        testSubject.fetchToken(PROCESSOR_ONE, TEST_SEGMENT);
        assertArrayEquals(new int[]{TEST_SEGMENT}, testSubject.fetchSegments(PROCESSOR_ONE));
        testSubject.deleteToken(PROCESSOR_ONE, TEST_SEGMENT);
        assertArrayEquals(new int[0], testSubject.fetchSegments(PROCESSOR_ONE));
    }

    @Test
    void deleteTokenWhenNotExistsThrows() {
        assertThrows(UnableToClaimTokenException.class, () -> testSubject.deleteToken(PROCESSOR_ONE, TEST_SEGMENT));
    }

    @Test
    void deleteTokenThrowsUnableToClaimTokenExceptionIfTheCallingProcessDoesNotOwnTheToken() {
        testSubject.initializeSegment(null, PROCESSOR_ONE, TEST_SEGMENT);
        // The token should be fetched to be claimed by somebody, so this should throw a UnableToClaimTokenException
        assertThrows(
                UnableToClaimTokenException.class,
                () -> testSubject.deleteToken(PROCESSOR_ONE, TEST_SEGMENT)
        );
    }

    @Test
    void storeTokenConcurrently() throws InterruptedException {
        testSubject.initializeSegment(null, PROCESSOR_ONE, TEST_SEGMENT);
        TrackingToken someToken = new GlobalSequenceTrackingToken(42);
        testConcurrencyOneFailing(
                () -> {
                    testSubject.storeToken(someToken, PROCESSOR_ONE, TEST_SEGMENT);
                    return true;
                },
                () -> {
                    otherTokenStore.storeToken(someToken, PROCESSOR_ONE, TEST_SEGMENT);
                    return true;
                });
    }

    @Test
    void deleteTokenConcurrently() throws InterruptedException {
        testSubject.initializeSegment(null, PROCESSOR_ONE, TEST_SEGMENT);
        TrackingToken someToken = new GlobalSequenceTrackingToken(42);
        testSubject.storeToken(someToken, PROCESSOR_ONE, TEST_SEGMENT);
        testSubject.releaseClaim(PROCESSOR_ONE, TEST_SEGMENT);
        testConcurrencyOneFailing(
                () -> {
                    // Claim the token by fetching it to be able to delete it
                    testSubject.fetchToken(PROCESSOR_ONE, TEST_SEGMENT);
                    testSubject.deleteToken(PROCESSOR_ONE, TEST_SEGMENT);
                    return true;
                },
                () -> {
                    // Claim the token by fetching it to be able to delete it
                    otherTokenStore.fetchToken(PROCESSOR_ONE, TEST_SEGMENT);
                    otherTokenStore.deleteToken(PROCESSOR_ONE, TEST_SEGMENT);
                    return true;
                });
    }

    @Test
    void fetchTokenConcurrently() throws InterruptedException {
        testSubject.initializeSegment(null, PROCESSOR_ONE, TEST_SEGMENT);
        TrackingToken someToken = new GlobalSequenceTrackingToken(42);
        testSubject.storeToken(someToken, PROCESSOR_ONE, TEST_SEGMENT);
        testSubject.releaseClaim(PROCESSOR_ONE, TEST_SEGMENT);
        TrackingToken result = testConcurrencyOneFailing(
                () -> testSubject.fetchToken(PROCESSOR_ONE, TEST_SEGMENT),
                () -> otherTokenStore.fetchToken(PROCESSOR_ONE, TEST_SEGMENT)
        );
        assertEquals(someToken, result);
    }

    @Test
    void initializeSegmentWithTokenConcurrently() throws InterruptedException {
        TrackingToken initialToken = new GlobalSequenceTrackingToken(42);
        testConcurrencyOneFailing(
                () -> {
                    testSubject.initializeSegment(initialToken, PROCESSOR_ONE, TEST_SEGMENT);
                    return true;
                },
                () -> {
                    otherTokenStore.initializeSegment(initialToken, PROCESSOR_ONE, TEST_SEGMENT);
                    return true;
                });
    }

    @Test
    void shouldBeAbleToStealAfterTimeoutHasPassed() throws InterruptedException {
        TokenStore storeWithDecreasedTimeout = CosmosTokenStore.builder()
                                                               .claimTimeout(Duration.ofMillis(200L))
                                                               .databaseName(DATABASE_NAME)
                                                               .client(getClient())
                                                               .serializer(serializer)
                                                               .nodeId(OTHER_OWNER)
                                                               .build();
        TrackingToken testToken = new GlobalSequenceTrackingToken(42);
        testSubject.initializeSegment(null, PROCESSOR_ONE, TEST_SEGMENT);
        testSubject.storeToken(testToken, PROCESSOR_ONE, TEST_SEGMENT);
        assertThrows(UnableToClaimTokenException.class,
                     () -> storeWithDecreasedTimeout.fetchToken(PROCESSOR_ONE, TEST_SEGMENT));
        Thread.sleep(200L);
        assertDoesNotThrow(() -> storeWithDecreasedTimeout.fetchToken(PROCESSOR_ONE, TEST_SEGMENT));
    }

    @Test
    void settingSerializerIsRequired() {
        CosmosTokenStore.Builder builder = CosmosTokenStore.builder().client(getClient());
        assertThrows(AxonConfigurationException.class, builder::validate);
    }


    private CosmosAsyncClient getClient() {
        CosmosAsyncClient client = new CosmosClientBuilder()
                .gatewayMode()
                .endpointDiscoveryEnabled(false)
                .endpoint(COSMOS_CONTAINER.getEmulatorEndpoint())
                .key(COSMOS_CONTAINER.getEmulatorKey())
                .buildAsyncClient();
        clients.add(client);
        return client;
    }

    private CosmosTokenStore getTokenStore(String owner) {
        return CosmosTokenStore
                .builder()
                .client(getClient())
                .databaseName(DATABASE_NAME)
                .serializer(serializer)
                .nodeId(owner)
                .build();
    }

    private <T> T testConcurrencyOneFailing(Supplier<T> s1, Supplier<T> s2) throws InterruptedException {
        ExecutorService executor = Executors.newFixedThreadPool(2);
        AtomicReference<T> r1 = new AtomicReference<>(null);
        AtomicReference<T> r2 = new AtomicReference<>(null);
        executor.execute(() -> r1.set(s1.get()));
        executor.execute(() -> r2.set(s2.get()));
        executor.shutdown();
        boolean done = executor.awaitTermination(6L, TimeUnit.SECONDS);
        assertTrue(done, "should complete in 6 seconds");
        if (r1.get() == null) {
            assertNotNull(r2.get(), "at least one of the results should be valid");
            return r2.get();
        } else {
            assertNull(r2.get(), "only one of the results should be valid");
            return r1.get();
        }
    }

    private <T> T testConcurrencyBothSameAnswer(Supplier<T> s1, Supplier<T> s2) throws InterruptedException {
        ExecutorService executor = Executors.newFixedThreadPool(2);
        AtomicReference<T> r1 = new AtomicReference<>(null);
        AtomicReference<T> r2 = new AtomicReference<>(null);
        executor.execute(() -> r1.set(s1.get()));
        executor.execute(() -> r2.set(s2.get()));
        executor.shutdown();
        boolean done = executor.awaitTermination(6L, TimeUnit.SECONDS);
        assertTrue(done, "should complete in 6 seconds");
        if (r1.get() == null) {
            assertNull(r2.get(), "both should return same value, in this case null");
            return r2.get();
        } else {
            assertEquals(r1.get(), r2.get(), "both should have same value");
            return r1.get();
        }
    }
}
