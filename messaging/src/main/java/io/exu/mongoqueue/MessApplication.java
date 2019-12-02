package io.exu.mongoqueue;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.convert.converter.Converter;
import org.springframework.data.mongodb.MongoDbFactory;
import org.springframework.expression.common.LiteralExpression;
import org.springframework.integration.annotation.IntegrationComponentScan;
import org.springframework.integration.core.MessageSource;
import org.springframework.integration.dsl.IntegrationFlow;
import org.springframework.integration.dsl.IntegrationFlows;
import org.springframework.integration.dsl.Pollers;
import org.springframework.integration.dsl.Transformers;
import org.springframework.integration.endpoint.AbstractMessageSource;
import org.springframework.integration.handler.LoggingHandler;
import org.springframework.integration.mongodb.outbound.MongoDbStoringMessageHandler;
import org.springframework.integration.mongodb.support.MongoHeaders;
import org.springframework.integration.support.AbstractIntegrationMessageBuilder;
import org.springframework.messaging.MessageHandler;
import org.springframework.util.Assert;

import java.util.concurrent.TimeUnit;

import static com.mongodb.client.model.Filters.eq;
import static com.mongodb.client.model.Updates.set;

@Configuration
@EnableAutoConfiguration
@IntegrationComponentScan
public class MessApplication {

    @Bean
    public MessageSource<Object> mongoMessageSource(MongoDbFactory mongo) {
        return new QueueMessageSource(mongo);
    }

    @Bean
    @Autowired
    public IntegrationFlow processProduct(MongoDbFactory mongo) {
        return IntegrationFlows.from(mongoMessageSource(mongo), c -> c.poller(Pollers.fixedDelay(3, TimeUnit.SECONDS)))
                .log(LoggingHandler.Level.WARN)
                .get();
    }


    public static void main(String[] args) {
        SpringApplication.run(MessApplication.class, args);
    }
}

class QueueMessageSource extends AbstractMessageSource<Object> {

    private volatile String collectionName = "product";
    private volatile MongoDbFactory mongoDbFactory;

    public QueueMessageSource(MongoDbFactory mongoDbFactory) {
        this.mongoDbFactory = mongoDbFactory;
    }

    @Override
    public String getComponentType() {
            return "mongo:inbound-queue-channel-adapter";
    }


    @Override
    protected Object doReceive() {
        Assert.notNull(collectionName, "'collectionName' must not evaluate to null");

        AbstractIntegrationMessageBuilder<Object> messageBuilder = null;

        Object result = null;
        result = this.mongoDbFactory
                .getDb()
                .getCollection(collectionName)
                .findOneAndUpdate(
                        eq("processed", false),
                        set("processed", true)
                );

        if (result != null) {
            messageBuilder = this.getMessageBuilderFactory().withPayload(result)
                    .setHeader(MongoHeaders.COLLECTION_NAME, collectionName);
        }

        return messageBuilder;
    }
}

class Product {
    private String id;
    private String status = "ready";
    private Boolean processed;

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getStatus() {
        return status;
    }

    public void setStatus(String status) {
        this.status = status;
    }

    public Boolean getProcessed() {
        return processed;
    }

    public void setProcessed(Boolean processed) {
        this.processed = processed;
    }

    @Override
    public String toString() {
        return "Product{" +
                "id='" + id + '\'' +
                ", status='" + status + '\'' +
                ", processed=" + processed +
                '}';
    }
}
