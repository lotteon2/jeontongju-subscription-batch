package com.jeontongju.subscriptionPaymentBatch;

import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
@EnableBatchProcessing
public class SubscriptionPaymentBatchApplication {

	public static void main(String[] args) {
		SpringApplication.run(SubscriptionPaymentBatchApplication.class, args);
	}

}
