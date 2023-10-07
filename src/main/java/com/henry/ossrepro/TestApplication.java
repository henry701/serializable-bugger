package com.henry.ossrepro;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.amqp.RabbitAutoConfiguration;
import org.springframework.boot.autoconfigure.kafka.KafkaAutoConfiguration;
import org.springframework.boot.context.properties.ConfigurationPropertiesScan;
import org.springframework.cache.annotation.EnableCaching;
import org.springframework.transaction.annotation.EnableTransactionManagement;

@EnableCaching
@EnableTransactionManagement
@ConfigurationPropertiesScan
@SpringBootApplication(exclude = {KafkaAutoConfiguration.class, RabbitAutoConfiguration.class})
public class TestApplication {

  public static void main(final String[] args) {
    SpringApplication.run(TestApplication.class, args);
  }
}
