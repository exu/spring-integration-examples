package io.exu.simpleMessaging;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.integration.annotation.Gateway;
import org.springframework.integration.annotation.MessagingGateway;
import org.springframework.integration.config.EnableIntegration;
import org.springframework.integration.dsl.IntegrationFlow;
import org.springframework.integration.dsl.IntegrationFlows;
import org.springframework.integration.handler.LoggingHandler;

import java.util.List;

@Configuration
@EnableIntegration
@SpringBootApplication
public class MessApplication implements CommandLineRunner {

	@Autowired
	private DataNotifier notifier;

	public static void main(String[] args) {
		SpringApplication.run(MessApplication.class, args);
	}

	@Override
	public void run(String... args) {
		notifier.notify(List.of("LAMPA", "DUPA"));
		notifier.notify(List.of("LAMPA", "DUPA"));
		notifier.notify(List.of("LAMPA", "DUPA"));
	}

	@MessagingGateway
	public interface DataNotifier {
		@Gateway(requestChannel = "output")
		void notify(List<String> message);
	}

	@Bean
	public IntegrationFlow flow() {
		return IntegrationFlows.from("output")
				.split()
				.transform(m -> "INS".concat(m.toString()))
				.log(LoggingHandler.Level.ERROR)
				.aggregate()
				.log(LoggingHandler.Level.INFO)
				.get();
	}
}