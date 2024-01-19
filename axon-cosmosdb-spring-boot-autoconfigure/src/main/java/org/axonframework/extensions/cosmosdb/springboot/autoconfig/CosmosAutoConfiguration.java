package org.axonframework.extensions.cosmosdb.springboot.autoconfig;

import com.azure.cosmos.CosmosAsyncClient;
import org.axonframework.eventhandling.tokenstore.TokenStore;
import org.axonframework.extensions.cosmosdb.eventsourcing.tokenstore.CosmosTokenStore;
import org.axonframework.serialization.Serializer;
import org.axonframework.springboot.TokenStoreProperties;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.AutoConfigureAfter;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;

import static java.util.Objects.isNull;

/**
 * Cosmos autoconfiguration class for Axon Framework application. Constructs the components which can be supplied by the
 * {@code axon-cosmosdb}. This is just the token store ({@link CosmosTokenStore}).
 * <p>
 *
 * @author Gerard Klijs-Nefkens
 * @since 4.9.0
 */
@AutoConfiguration
@EnableConfigurationProperties(TokenStoreProperties.class)
@AutoConfigureAfter(name = "org.axonframework.springboot.autoconfig.JdbcAutoConfiguration")
public class CosmosAutoConfiguration {

    private final TokenStoreProperties tokenStoreProperties;

    public CosmosAutoConfiguration(
            TokenStoreProperties tokenStoreProperties
    ) {
        this.tokenStoreProperties = tokenStoreProperties;
    }

    @Value("${azure.cosmos.database:#{null}}")
    private String dbName;

    @Value("${spring.cloud.azure.cosmos.database:#{null}}")
    private String cloudDbName;

    @Bean("tokenStore")
    @ConditionalOnMissingBean(TokenStore.class)
    public TokenStore tokenStore(
            @Qualifier(value = "azureCosmosAsyncClient") CosmosAsyncClient client,
            Serializer serializer
    ) {
        String databaseName = "axon";
        if (!isNull(dbName)) {
            databaseName = dbName;
        } else if (!isNull(cloudDbName)) {
            databaseName = cloudDbName;
        }
        return CosmosTokenStore.builder()
                               .client(client)
                               .serializer(serializer)
                               .databaseName(databaseName)
                               .claimTimeout(tokenStoreProperties.getClaimTimeout())
                               .build();
    }
}
