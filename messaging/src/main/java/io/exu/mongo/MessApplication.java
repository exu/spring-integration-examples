package io.exu.mongo;

import org.bson.BsonDocument;
import org.bson.conversions.Bson;
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
    ProductToOffersConverter converter(){
        return new ProductToOffersConverter();
    }


    @Bean
    @Autowired
    public MessageSource<Object> mongoMessageSource(MongoDbFactory mongo) {
        return new QueueMessageSource(mongo);
    }


//
//    @Bean
//    @Autowired
//    public MessageSource<Object> mongoMessageSource(MongoDbFactory mongo) {
//        MongoDbMessageSource messageSource = new MongoDbMessageSource(mongo, new LiteralExpression("{'processed' : false}"));
//        messageSource.setExpectSingleResult(true);
//        messageSource.setEntityClass(Product.class);
//        messageSource.setCollectionNameExpression(new LiteralExpression("product"));
//
//        return messageSource;
//    }


    @Bean
    @Autowired
    public IntegrationFlow processProduct(MongoDbFactory mongo) {
        return IntegrationFlows.from(mongoMessageSource(mongo), c -> c.poller(Pollers.fixedDelay(3, TimeUnit.SECONDS)))
                .log(LoggingHandler.Level.ERROR)
                .transform(Transformers.converter(converter()))
                .log(LoggingHandler.Level.WARN)
                .handle(mongoOutboundAdapter(mongo))
                .get();
    }

    @Bean
    @Autowired
    public MessageHandler mongoOutboundAdapter(MongoDbFactory mongo) {
        MongoDbStoringMessageHandler mongoHandler = new MongoDbStoringMessageHandler(mongo);
        mongoHandler.setCollectionNameExpression(new LiteralExpression("offers"));
        return mongoHandler;
    }

    public static void main(String[] args) {
        SpringApplication.run(MessApplication.class, args);
    }


}


class QueueMessageSource extends AbstractMessageSource<Object> {

    private final String queryString = "{\"processed\" : false}";
    private final String updateString = "{\"processed\" : true}";
    private volatile String collectionName = "product";
    private volatile MongoDbFactory mongoDbFactory;

    public QueueMessageSource(MongoDbFactory mongoDbFactory) {
        this.mongoDbFactory = mongoDbFactory;
    }

    @Override
    public String getComponentType() {
            return "mongo:inbound-channel-adapter-findandmodify";
    }


    @Override
    protected Object doReceive() {
        Assert.notNull(queryString, "'queryString' must not evaluate to null");

        AbstractIntegrationMessageBuilder<Object> messageBuilder = null;
        Bson query = BsonDocument.parse(this.queryString);
        Bson update = BsonDocument.parse(updateString);
        Assert.notNull(collectionName, "'collectionNameE' must not evaluate to null");

        Object result = null;
        result = this.mongoDbFactory.getDb().getCollection(collectionName).findOneAndUpdate(eq("processed", true), set("processed", false));

        if (result != null) {
            messageBuilder = this.getMessageBuilderFactory().withPayload(result)
                    .setHeader(MongoHeaders.COLLECTION_NAME, collectionName);
        }

        return messageBuilder;
    }
}

class ProductToOffersConverter implements Converter<Product, Offers> {
    @Override
    public Offers convert(Product product) {
        return new Offers(product, 10);
    }
}

class Offers {
    Product product;
    Integer count = 0;

    public Offers(Product product, Integer count) {
        this.product = product;
        this.count = count;
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
