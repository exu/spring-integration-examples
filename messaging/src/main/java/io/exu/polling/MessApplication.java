package io.exu.polling;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.mongodb.MongoDbFactory;
import org.springframework.data.mongodb.core.convert.MongoConverter;
import org.springframework.integration.annotation.Gateway;
import org.springframework.integration.annotation.MessagingGateway;
import org.springframework.integration.annotation.ServiceActivator;
import org.springframework.integration.config.EnableIntegration;
import org.springframework.integration.dsl.IntegrationFlow;
import org.springframework.integration.dsl.IntegrationFlows;
import org.springframework.integration.handler.LoggingHandler;
import org.springframework.integration.mongodb.dsl.MongoDb;
import org.springframework.integration.mongodb.dsl.MongoDbOutboundGatewaySpec;
import org.springframework.integration.mongodb.outbound.MongoDbOutboundGateway;
import org.springframework.messaging.MessageHandler;

import java.util.List;

@Configuration
@EnableIntegration
@SpringBootApplication
public class MessApplication  {

	public static void main(String[] args) {
		SpringApplication.run(MessApplication.class, args);
	}

	@Autowired
	private MongoDbFactory mongoDbFactory;

	@Autowired
	private MongoConverter mongoConverter;


	@Bean
	public IntegrationFlow gatewaySingleQueryFlow() {
		return f -> f
				.handle(queryOutboundGateway())
				.channel(c -> c.queue("retrieveResults"));
	}

	@Bean
	public IntegrationFlow flow() {
		return IntegrationFlows.from("retriveResults")
				.log(LoggingHandler.Level.ERROR)
				.get();
	}

	private MongoDbOutboundGatewaySpec queryOutboundGateway() {
		return MongoDb.outboundGateway(this.mongoDbFactory, this.mongoConverter)
				.query("{name : 'Bob'}")
				.collectionNameFunction(m -> (String) m.getHeaders().get("collection"))
				.expectSingleResult(true)
				.entityClass(Person.class);
	}

//	@Override
//	public void run(String... args) throws Exception {
//
//	}
}

class Person {
	private String name;
	private  Integer age;
}