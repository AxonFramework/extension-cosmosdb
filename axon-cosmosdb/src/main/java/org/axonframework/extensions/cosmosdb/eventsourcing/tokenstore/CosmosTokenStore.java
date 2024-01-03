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
import org.axonframework.common.AxonConfigurationException;
import org.axonframework.common.BuilderUtils;
import org.axonframework.eventhandling.TrackingToken;
import org.axonframework.eventhandling.tokenstore.TokenStore;
import org.axonframework.eventhandling.tokenstore.UnableToClaimTokenException;
import org.axonframework.serialization.Serializer;

import java.lang.management.ManagementFactory;
import java.time.Duration;
import java.time.temporal.TemporalAmount;

import static org.axonframework.common.BuilderUtils.assertNonNull;

/**
 * An implementation of the {@link TokenStore} that allows you to store and retrieve tracking tokens with Cosmos DB.
 */
public class CosmosTokenStore implements TokenStore {

    private final CosmosAsyncClient client;
    private final Serializer serializer;
    private final TemporalAmount claimTimeout;
    private final String nodeId;

    protected CosmosTokenStore(Builder builder) {
        builder.validate();
        this.client = builder.client;
        this.serializer = builder.serializer;
        this.claimTimeout = builder.claimTimeout;
        this.nodeId = builder.nodeId;
    }

    public static Builder builder() {
        return new Builder();
    }

    @Override
    public void storeToken(TrackingToken token, String processorName, int segment) throws UnableToClaimTokenException {

    }

    @Override
    public TrackingToken fetchToken(String processorName, int segment) throws UnableToClaimTokenException {
        return null;
    }

    @Override
    public void releaseClaim(String processorName, int segment) {

    }

    @Override
    public int[] fetchSegments(String processorName) {
        return new int[0];
    }

    public static class Builder {

        private CosmosAsyncClient client;
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

        public CosmosTokenStore build() {
            return new CosmosTokenStore(this);
        }

        protected void validate() throws AxonConfigurationException {
            assertNonNull(client, "The CosmosAsyncClient is a hard requirement and should be provided");
            assertNonNull(serializer, "The Serializer is a hard requirement and should be provided");
        }
    }
}
