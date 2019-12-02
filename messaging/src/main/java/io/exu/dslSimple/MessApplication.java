package io.exu.dslSimple;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.event.ContextRefreshedEvent;
import org.springframework.context.event.EventListener;
import org.springframework.integration.annotation.Gateway;
import org.springframework.integration.annotation.IntegrationComponentScan;
import org.springframework.integration.annotation.MessagingGateway;
import org.springframework.integration.dsl.IntegrationFlow;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;

@Configuration
@EnableAutoConfiguration
@IntegrationComponentScan
public class MessApplication {

    @Autowired
    Upcase upcase;

    public static void main(String[] args) {
        SpringApplication.run(MessApplication.class, args);
    }

    @EventListener
    public void onApplicationEvent(ContextRefreshedEvent event) {
        System.out.println(upcase.upcase(List.of("foo", "bar")));
    }

    @MessagingGateway
    public interface Upcase {
        @Gateway(requestChannel = "upcase.input")
        Collection<String> upcase(Collection<String> strings);

    }

    @Bean
    public IntegrationFlow upcase() {
        return f -> f
                .split()                                         // 1
                .<String, String>transform(String::toUpperCase)  // 2
                .aggregate();                                    // 3
    }

}