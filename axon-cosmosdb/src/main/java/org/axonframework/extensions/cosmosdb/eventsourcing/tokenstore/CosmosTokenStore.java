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

import com.azure.cosmos.CosmosAsyncClient;
import com.azure.cosmos.CosmosAsyncContainer;
import com.azure.cosmos.CosmosAsyncDatabase;
import com.azure.cosmos.CosmosException;
import com.azure.cosmos.models.CosmosItemRequestOptions;
import com.azure.cosmos.models.CosmosItemResponse;
import com.azure.cosmos.models.PartitionKey;
import org.axonframework.common.AxonConfigurationException;
import org.axonframework.common.BuilderUtils;
import org.axonframework.eventhandling.Segment;
import org.axonframework.eventhandling.TrackingToken;
import org.axonframework.eventhandling.tokenstore.ConfigToken;
import org.axonframework.eventhandling.tokenstore.TokenStore;
import org.axonframework.eventhandling.tokenstore.UnableToClaimTokenException;
import org.axonframework.eventhandling.tokenstore.UnableToInitializeTokenException;
import org.axonframework.eventhandling.tokenstore.UnableToRetrieveIdentifierException;
import org.axonframework.serialization.SerializedObject;
import org.axonframework.serialization.Serializer;
import org.axonframework.serialization.SimpleSerializedObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Mono;

import java.lang.management.ManagementFactory;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.TemporalAmount;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.stream.IntStream;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import static java.util.Objects.isNull;
import static org.axonframework.common.BuilderUtils.assertNonEmpty;
import static org.axonframework.common.BuilderUtils.assertNonNull;

/**
 * An implementation of the {@link TokenStore} that allows you to store and retrieve tracking tokens with Cosmos DB. It
 * allows to set a database and container name in which all the tokens are kept. If the database and/or the container
 * don't exist yet, they will be created.
 *
 * @author Gerard Klijs-Nefkens
 * @since 4.9.0
 */
public class CosmosTokenStore implements TokenStore {

    private static final Logger logger = LoggerFactory.getLogger(CosmosTokenStore.class);
    private static final String ID_PARTITION_KEY = "/id";
    private static final String STORAGE_IDENTIFIER_PROCESSING_GROUP = "__STORAGE_IDENTIFIER__";
    private static final int STORAGE_IDENTIFIER_SEGMENT = 42;
    private static final String STORAGE_IDENTIFIER_ID =
            STORAGE_IDENTIFIER_PROCESSING_GROUP + STORAGE_IDENTIFIER_SEGMENT;
    private static final String STORAGE_IDENTIFIER_KEY = "id";
    private static final String ALL_PROCESSING_NAME_QUERY_FORMAT = "SELECT * FROM item WHERE item.processorName = '%s'";
    private static final String NO_CURRENT_ITEM_MESSAGE = "Could not claim token.\n" +
            "Current item could not be retrieved successfully.\n" +
            "You likely need to initialize the token first or configure the database name.";
    private static final String CLAIM_EXCEPTION_MESSAGE_FORMAT = "Could not claim token.\n" +
            "Current owner is: '%s' and not: '%s'.\n" +
            "Time in milliseconds until token can be claimed: '%d'.";
    private static final String CANT_DELETE_MESSAGE_FORMAT =
            "Not the owner of the token, so can't delete the token.\n" +
                    "Current owner is: '%s' and not: '%s'.";
    private static final String CANT_EXTEND_MESSAGE_FORMAT =
            "Not the owner of the token, so can't extend the claim.\n" +
                    "Current owner is: '%s' and not: '%s'.";

    private final CosmosAsyncContainer container;
    private final CosmosAsyncClient client;
    private final Serializer serializer;
    private final TemporalAmount claimTimeout;
    private final String nodeId;

    /**
     * Instantiate a {@link CosmosTokenStore} based on the fields contained in the {@link Builder}.
     * <p>
     * Will assert that the {@link CosmosAsyncClient} and {@link Serializer} are not {@code null}, and will throw an
     * {@link AxonConfigurationException} if any of them is {@code null}.
     *
     * @param builder the {@link Builder} used to instantiate a {@link CosmosTokenStore} instance
     */
    protected CosmosTokenStore(Builder builder) {
        builder.validate();
        this.client = builder.client;
        this.serializer = builder.serializer;
        this.claimTimeout = builder.claimTimeout;
        this.nodeId = builder.nodeId;
        this.container = getContainer(builder.databaseName, builder.containerName);
    }

    /**
     * Instantiate a Builder to be able to create a {@link CosmosTokenStore}.
     * <p>
     * The {@code claimTimeout} is defaulted to a 10 seconds duration (by using {@link Duration#ofSeconds(long)},
     * {@code nodeId} is defaulted to the {@code ManagementFactory#getRuntimeMXBean#getName} output, the {@link String}
     * database name defaults to 'axon', the {@link String} container name defaults to 'tokenstore'. The
     * {@link CosmosAsyncClient} and {@link Serializer} are <b>hard requirements</b> and as such should be provided.
     *
     * @return a Builder to be able to crete a {@link CosmosTokenStore}
     */
    public static Builder builder() {
        return new Builder();
    }

    @Override
    public void initializeTokenSegments(@Nonnull String processorName, int segmentCount)
            throws UnableToClaimTokenException {
        initializeTokenSegments(processorName, segmentCount, null);
    }

    @Override
    public void initializeTokenSegments(@Nonnull String processorName,
                                        int segmentCount,
                                        @Nullable TrackingToken initialToken) throws UnableToClaimTokenException {
        if (fetchSegments(processorName).length > 0) {
            throw new UnableToClaimTokenException(
                    "Unable to initialize segments. Some tokens were already present for the given processor."
            );
        }
        IntStream.range(0, segmentCount).forEach(
                i -> initializeSegment(initialToken, processorName, i)
        );
    }

    @Override
    public void storeToken(TrackingToken token, @Nonnull String processorName, int segment)
            throws UnableToClaimTokenException {
        try {
            getCurrent(processorName, segment)
                    .flatMap(current -> {
                        validateCurrent(current.getItem());
                        CosmosTokenItem newItem = createNewItem(token, processorName, segment);
                        return replaceWithConcurrencyControl(current, newItem);
                    })
                    .block();
        } catch (UnableToClaimTokenException e) {
            throw e;
        } catch (Exception e) {
            throw new UnableToClaimTokenException("Could not store token.", e);
        }
    }

    @Override
    public TrackingToken fetchToken(@Nonnull String processorName, int segment) throws UnableToClaimTokenException {
        CosmosItemResponse<CosmosTokenItem> response = getCurrent(processorName, segment)
                .flatMap(current -> {
                    validateCurrent(current.getItem());
                    return replaceWithConcurrencyControl(current, current.getItem().withOwner(nodeId));
                })
                .flatMap(r -> {
                    if (r.getStatusCode() == 200) {
                        return getCurrent(processorName, segment);
                    } else {
                        return Mono.empty();
                    }
                })
                .block();
        if (response == null || response.getItem() == null) {
            throw new UnableToClaimTokenException(NO_CURRENT_ITEM_MESSAGE);
        }
        return toTrackingToken(response.getItem());
    }

    @Override
    public void extendClaim(@Nonnull String processorName, int segment) throws UnableToClaimTokenException {
        try {
            getCurrent(processorName, segment)
                    .flatMap(current -> {
                        String owner = current.getItem().getOwner();
                        if (!nodeId.equals(owner)) {
                            return Mono.error(new UnableToClaimTokenException(
                                    String.format(CANT_EXTEND_MESSAGE_FORMAT, owner, nodeId)));
                        }
                        return replaceWithConcurrencyControl(current, current.getItem().extend());
                    })
                    .block();
        } catch (UnableToClaimTokenException e) {
            throw e;
        } catch (Exception e) {
            throw new UnableToClaimTokenException("Could not extend token claim.", e);
        }
    }

    @Override
    public void releaseClaim(@Nonnull String processorName, int segment) {
        try {
            getCurrent(processorName, segment)
                    .flatMap(current -> {
                        validateCurrent(current.getItem());
                        return replaceWithConcurrencyControl(current, current.getItem().release());
                    })
                    .block();
        } catch (Exception e) {
            logger.warn("Failed to release token. {}", e.getMessage());
        }
    }

    @Override
    public void initializeSegment(@Nullable TrackingToken token, @Nonnull String processorName, int segment)
            throws UnableToInitializeTokenException {
        try {
            container.createItem(createInitialItem(token, processorName, segment)
            ).block();
        } catch (Exception e) {
            throw new UnableToInitializeTokenException("Could not initialize token.", e);
        }
    }

    @Override
    public void deleteToken(@Nonnull String processorName, int segment) throws UnableToClaimTokenException {
        try {
            getCurrent(processorName, segment)
                    .flatMap(current -> {
                        String owner = current.getItem().getOwner();
                        if (!nodeId.equals(owner)) {
                            return Mono.error(new UnableToClaimTokenException(
                                    String.format(CANT_DELETE_MESSAGE_FORMAT, owner, nodeId)));
                        }
                        return container.deleteItem(
                                current.getItem().getId(),
                                new PartitionKey(current.getItem().getId())
                        );
                    })
                    .block();
        } catch (UnableToClaimTokenException e) {
            throw e;
        } catch (Exception e) {
            throw new UnableToClaimTokenException("Could not delete token.", e);
        }
    }

    @Override
    public boolean requiresExplicitSegmentInitialization() {
        return true;
    }

    @Override
    public int[] fetchSegments(@Nonnull String processorName) {
        String query = String.format(ALL_PROCESSING_NAME_QUERY_FORMAT, processorName);
        List<Integer> segments = container
                .queryItems(query, CosmosTokenItem.class)
                .map(CosmosTokenItem::getSegment)
                .collectList()
                .block();
        if (isNull(segments)) {
            return new int[0];
        } else {
            return segments.stream().mapToInt(i -> i).toArray();
        }
    }

    @Override
    public List<Segment> fetchAvailableSegments(@Nonnull String processorName) {
        int[] all = fetchSegments(processorName);
        String query = String.format(ALL_PROCESSING_NAME_QUERY_FORMAT, processorName);
        return container
                .queryItems(query, CosmosTokenItem.class)
                .filter(this::claimable)
                .map(item -> Segment.computeSegment(item.getSegment(), all))
                .collectList()
                .block();
    }

    private CosmosAsyncContainer getContainer(@Nonnull String databaseName, @Nonnull String containerName) {
        CosmosAsyncDatabase database = createDatabaseIfNotExists(databaseName);
        return createContainerIfNotExists(database, containerName);
    }

    @Nonnull
    private CosmosAsyncDatabase createDatabaseIfNotExists(String databaseName) {
        CosmosAsyncDatabase database = client
                .createDatabaseIfNotExists(databaseName)
                .map(r -> client.getDatabase(r.getProperties().getId()))
                .block();
        assert database != null;
        return database;
    }

    private CosmosAsyncContainer createContainerIfNotExists(@Nonnull CosmosAsyncDatabase database,
                                                            @Nonnull String containerName) {
        return database.createContainerIfNotExists(containerName, ID_PARTITION_KEY)
                       .map(r -> database.getContainer(r.getProperties().getId()))
                       .block();
    }

    private Mono<CosmosItemResponse<CosmosTokenItem>> getCurrent(String processorName, int segment) {
        String id = processorName + segment;
        return container.readItem(id, new PartitionKey(id), CosmosTokenItem.class);
    }

    private CosmosTokenItem createNewItem(TrackingToken token, String processorName, int segment) {
        SerializedObject<byte[]> serializedObject = serializer.serialize(token, byte[].class);
        return new CosmosTokenItem(processorName,
                                   segment,
                                   serializedObject.getData(),
                                   serializedObject.getType().getName(),
                                   nodeId);
    }

    private Mono<CosmosItemResponse<CosmosTokenItem>> replaceWithConcurrencyControl(
            CosmosItemResponse<CosmosTokenItem> current,
            CosmosTokenItem newItem
    ) {
        return container.replaceItem(
                newItem,
                current.getItem().getId(),
                new PartitionKey(current.getItem().getId()),
                new CosmosItemRequestOptions().setIfMatchETag(current.getETag())
        );
    }

    private CosmosTokenItem createInitialItem(@Nullable TrackingToken token, String processorName, int segment) {
        if (isNull(token)) {
            return CosmosTokenItem.initialToken(processorName, segment, null, null);
        } else {
            SerializedObject<byte[]> serializedObject = serializer.serialize(token, byte[].class);
            return CosmosTokenItem.initialToken(processorName,
                                                segment,
                                                serializedObject.getData(),
                                                serializedObject.getType().getName());
        }
    }

    @SuppressWarnings({"rawtypes", "unchecked"})
    private TrackingToken toTrackingToken(CosmosTokenItem cosmosTokenItem) {
        if (isNull(cosmosTokenItem.getToken())) {
            return null;
        }
        SimpleSerializedObject<Byte[]> serializedObject = new SimpleSerializedObject(
                cosmosTokenItem.getToken(),
                byte[].class,
                cosmosTokenItem.getTokenType(),
                null);
        return serializer.deserialize(serializedObject);
    }

    private void validateCurrent(CosmosTokenItem current) {
        if (current == null) {
            throw new UnableToClaimTokenException(NO_CURRENT_ITEM_MESSAGE);
        }
        if (isNull(current.getOwner())) {
            return;
        }
        Instant now = Instant.now();
        if (!nodeId.equals(current.getOwner()) &&
                current.getTimestamp().isAfter(now.minus(claimTimeout))) {
            throw new UnableToClaimTokenException(
                    String.format(CLAIM_EXCEPTION_MESSAGE_FORMAT,
                                  nodeId, current.getOwner(),
                                  Duration.between(now.minus(claimTimeout), current.getTimestamp()).toMillis())
            );
        }
    }

    private boolean claimable(CosmosTokenItem item) {
        try {
            validateCurrent(item);
        } catch (UnableToClaimTokenException e) {
            return false;
        }
        return true;
    }

    private void setStorageIdentifier() {
        Map<String, String> map = new HashMap<>();
        map.put(STORAGE_IDENTIFIER_KEY, UUID.randomUUID().toString());
        TrackingToken token = new ConfigToken(map);
        container.createItem(
                createInitialItem(token, STORAGE_IDENTIFIER_PROCESSING_GROUP, STORAGE_IDENTIFIER_SEGMENT)
        ).block();
    }

    @Override
    public Optional<String> retrieveStorageIdentifier() throws UnableToRetrieveIdentifierException {
        try {
            CosmosItemResponse<CosmosTokenItem> response = container
                    .readItem(STORAGE_IDENTIFIER_ID,
                              new PartitionKey(STORAGE_IDENTIFIER_ID),
                              CosmosTokenItem.class)
                    .block();
            if (response == null) {
                setStorageIdentifier();
                return retrieveStorageIdentifier();
            }
            ConfigToken configToken = (ConfigToken) toTrackingToken(response.getItem());
            if (isNull(configToken)) {
                return Optional.empty();
            }
            return Optional.ofNullable(configToken.get(STORAGE_IDENTIFIER_KEY));
        } catch (CosmosException e) {
            setStorageIdentifier();
            return retrieveStorageIdentifier();
        } catch (Exception e) {
            throw new UnableToRetrieveIdentifierException(
                    "Exception occurred while trying to establish storage identifier", e
            );
        }
    }

    /**
     * Builder class to initiate a {@link CosmosTokenStore}
     * </p>
     * The {@code claimTimeout} is defaulted to a 10 seconds duration (by using {@link Duration#ofSeconds(long)},
     * {@code nodeId} is defaulted to the {@code ManagementFactory#getRuntimeMXBean#getName} output, the {@link String}
     * database name defaults to 'axon', the {@link String} container name defaults to 'tokenstore'. The
     * {@link CosmosAsyncClient} and {@link Serializer} are <b>hard requirements</b> and as such should be provided.
     */
    public static class Builder {

        private CosmosAsyncClient client;
        private String databaseName = "axon";
        private String containerName = "tokenstore";
        private Serializer serializer;
        private TemporalAmount claimTimeout = Duration.ofSeconds(10);
        private String nodeId = ManagementFactory.getRuntimeMXBean().getName();

        /**
         * Sets the {@link CosmosAsyncClient} providing access to manage {@link TrackingToken}s.
         *
         * @param client the {@link CosmosAsyncClient} providing access to the {@link TrackingToken}s
         * @return the current Builder instance, for fluent interfacing
         */
        public Builder client(CosmosAsyncClient client) {
            assertNonNull(client, "CosmosAsyncClient may not be null");
            this.client = client;
            return this;
        }

        /**
         * Sets the {@link String} with the database name to be used. Defaults to {@code axon}.
         *
         * @param databaseName the {@link String} value of the database name.
         * @return the current Builder instance, for fluent interfacing
         */
        public Builder databaseName(String databaseName) {
            assertNonEmpty(databaseName, "The database name should not be null or empty");
            this.databaseName = databaseName;
            return this;
        }

        /**
         * Sets the {@link String} with the container name to be used. Defaults to {@code tokenstore}.
         *
         * @param containerName the {@link String} value of the container name.
         * @return the current Builder instance, for fluent interfacing
         */
        public Builder containerName(String containerName) {
            assertNonEmpty(containerName, "The container name should not be null or empty");
            this.containerName = containerName;
            return this;
        }

        /**
         * Sets the {@link Serializer} used to de-/serialize {@link TrackingToken}s with.
         *
         * @param serializer a {@link Serializer} used to de-/serialize {@link TrackingToken}s with
         * @return the current Builder instance, for fluent interfacing
         */
        public Builder serializer(Serializer serializer) {
            assertNonNull(serializer, "Serializer may not be null");
            this.serializer = serializer;
            return this;
        }

        /**
         * Sets the {@code claimTimeout} specifying the amount of time this process will wait after which this process
         * will force a claim of a {@link TrackingToken}. Thus if a claim has not been updated for the given
         * {@code claimTimeout}, this process will 'steal' the claim. Defaults to a duration of 10 seconds.
         *
         * @param claimTimeout a timeout specifying the time after which this process will force a claim
         * @return the current Builder instance, for fluent interfacing
         */
        public Builder claimTimeout(TemporalAmount claimTimeout) {
            assertNonNull(claimTimeout, "The claim timeout may not be null");
            this.claimTimeout = claimTimeout;
            return this;
        }

        /**
         * Sets the {@code nodeId} to identify ownership of the tokens. Defaults to
         * {@code ManagementFactory#getRuntimeMXBean#getName} output as the node id.
         *
         * @param nodeId the id as a {@link String} to identify ownership of the tokens
         * @return the current Builder instance, for fluent interfacing
         */
        public Builder nodeId(String nodeId) {
            BuilderUtils.assertNonEmpty(nodeId, "The nodeId may not be null or empty");
            this.nodeId = nodeId;
            return this;
        }

        /**
         * Initializes a {@link CosmosTokenStore} as specified through this Builder.
         *
         * @return a {@link CosmosTokenStore} as specified through this Builder
         */
        public CosmosTokenStore build() {
            return new CosmosTokenStore(this);
        }

        /**
         * Validates whether the fields contained in this Builder are set accordingly.
         *
         * @throws AxonConfigurationException if one field is asserted to be incorrect according to the Builder's
         *                                    specifications
         */
        protected void validate() throws AxonConfigurationException {
            assertNonNull(client, "The CosmosAsyncClient is a hard requirement and should be provided");
            assertNonNull(serializer, "The Serializer is a hard requirement and should be provided");
        }
    }
}
