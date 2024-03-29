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

import java.time.Instant;
import java.util.Arrays;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * Class used with the {@link CosmosTokenStore} to store and retrieve the tokens from Cosmos DB. There are both
 * functions for jackson, and those to be used in the token store. The functions for the token store regard this as
 * immutable, returning a new instance instead.
 *
 * @author Gerard Klijs-Nefkens
 * @since 4.9.0
 */
public class CosmosTokenItem {

    private String id;
    private String processorName;
    private int segment;
    private byte[] token;
    private String tokenType;
    private String owner;
    private Instant timestamp;

    /**
     * Needed for Jackson serialisation, don't use in code.
     */
    @SuppressWarnings("unused")
    CosmosTokenItem() {
    }

    /**
     * Typical constructor used from the token store to create the initial items.
     *
     * @param processorName the {@link String} representation if the processor name, can't be null.
     * @param segment   the {@code segment} of the token, can't be null as it's also the id.
     * @param token     the {@code byte[]} serialized token, can be null if there is no token yet.
     * @param tokenType the {@link String} token type, can be null if there is no token yet.
     * @param owner     the {@link String} owner of the token, can be null if there is no owner.
     */
    public CosmosTokenItem(
            @Nonnull String processorName,
            int segment,
            byte[] token,
            @Nullable String tokenType,
            @Nullable String owner) {
        this.id = processorName + segment;
        this.processorName = processorName;
        this.segment = segment;
        this.token = token;
        this.tokenType = tokenType;
        this.owner = owner;
        this.timestamp = Instant.now();
    }

    /**
     * Created a token without an owner, like needed for the initial tokens.
     *
     * @param processorName the {@link String} representation if the processor name, can't be null.
     * @param segment   the {@code segment} of the token, can't be null as it's also the id.
     * @param token     the {@code byte[]} serialized token, can be null if there is no token yet.
     * @param tokenType the {@link String} token type, can be null if there is no token yet.
     * @return the new {@link CosmosTokenItem} item
     */
    public static CosmosTokenItem initialToken(
            @Nonnull String processorName,
            int segment,
            byte[] token,
            @Nullable String tokenType
    ) {
        return new CosmosTokenItem(processorName, segment, token, tokenType, null);
    }

    /**
     * Creates a new token where the owner is null.
     *
     * @return a {@link CosmosTokenItem} with the owner set to null, and a new timestamp.
     */
    public CosmosTokenItem release() {
        return new CosmosTokenItem(
                this.processorName,
                this.segment,
                this.token,
                this.tokenType,
                null
        );
    }

    /**
     * Creates a new token with the owner set to the given value. This task is used to claim a token.
     *
     * @param owner the {@link String} of the new owner.
     * @return a {@link CosmosTokenItem} with the owner set to null, and a new timestamp.
     */
    public CosmosTokenItem withOwner(String owner) {
        return new CosmosTokenItem(
                this.processorName,
                this.segment,
                this.token,
                this.tokenType,
                owner
        );
    }

    /**
     * Creates a new token with the same values except the timestamp.
     *
     * @return a {@link CosmosTokenItem} with a new timestamp.
     */
    public CosmosTokenItem extend() {
        return new CosmosTokenItem(
                this.processorName,
                this.segment,
                this.token,
                this.tokenType,
                this.owner
        );
    }

    /**
     * Gets the id.
     *
     * @return the {@link String} value if the id, which is based on the {@code segment}
     */
    @Nonnull
    public String getId() {
        return id;
    }

    /**
     * Gets the processor name. Needed for Jackson serialisation.
     *
     * @return the {@link String} of the processor name
     */
    @Nonnull
    @SuppressWarnings("unused")
    public String getProcessorName() {
        return processorName;
    }

    /**
     * Gets the segment.
     * @return the {@code int} of the segment
     */
    public int getSegment() {
        return segment;
    }

    /**
     * Gets the token.
     * @return the {@code byte[]} of the serialized token
     */
    public byte[] getToken() {
        return token;
    }

    /**
     * Gets the token type.
     * @return the {@link String} value of the token type, used to deserialize the token to a class instance.
     */
    @Nullable
    public String getTokenType() {
        return tokenType;
    }

    /**
     * Gets the owner
     * @return the {@link String} of the owner.
     */
    @Nullable
    public String getOwner() {
        return owner;
    }

    /**
     * Gets the timestamp
     * @return the {@link Instant} of the timestamp, this is the time the instance was created.
     */
    @Nonnull
    public Instant getTimestamp() {
        return timestamp;
    }

    /**
     * Needed for Jackson serialisation, don't use in code.
     *
     * @param id the {@link String} representing the id in the serialized format
     */
    @SuppressWarnings("unused")
    void setId(@Nonnull String id) {
        this.id = id;
    }

    /**
     * Needed for Jackson serialisation, don't use in code.
     *
     * @param processorName the {@link String} representing the processor name.
     */
    @SuppressWarnings("unused")
    void setProcessorName(@Nonnull String processorName) {
        this.processorName = processorName;
    }

    /**
     * Needed for Jackson serialisation, don't use in code.
     *
     * @param segment the {@code int} representing the segment
     */
    @SuppressWarnings("unused")
    void setSegment(int segment) {
        this.segment = segment;
    }

    /**
     * Needed for Jackson serialisation, don't use in code.
     *
     * @param token the {@code byte[]} representing the serialized token in the serialized format
     */
    @SuppressWarnings("unused")
    void setToken(byte[] token) {
        this.token = token;
    }

    /**
     * Needed for Jackson serialisation, don't use in code.
     *
     * @param tokenType the {@link String} representing the token type in the serialized format
     */
    @SuppressWarnings("unused")
    void setTokenType(@Nullable String tokenType) {
        this.tokenType = tokenType;
    }

    /**
     * Needed for Jackson serialisation, don't use in code.
     *
     * @param owner the {@link String} representing the owner in the serialized format
     */
    @SuppressWarnings("unused")
    void setOwner(@Nullable String owner) {
        this.owner = owner;
    }

    /**
     * Needed for Jackson serialisation, don't use in code.
     *
     * @param timestamp the {@link Instant} representing the timestamp in the serialized format
     */
    @SuppressWarnings("unused")
    void setTimestamp(@Nonnull Instant timestamp) {
        this.timestamp = timestamp;
    }

    @Override
    public String toString() {
        return "CosmosTokenItem{" +
                "id='" + id + '\'' +
                ", processorName='" + processorName + '\'' +
                ", segment='" + segment + '\'' +
                ", token='" + Arrays.toString(token) + '\'' +
                ", tokenType='" + tokenType + '\'' +
                ", owner='" + owner + '\'' +
                ", timestamp='" + timestamp + '\'' +
                '}';
    }
}
