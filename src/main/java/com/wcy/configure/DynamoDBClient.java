package com.wcy.configure;

import org.springframework.context.annotation.Bean;	
import org.springframework.context.annotation.Configuration;

import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;

@Configuration
public class DynamoDBClient {

	@Bean
	AmazonDynamoDB client(){
		AmazonDynamoDB client=AmazonDynamoDBClientBuilder.standard()
				.withEndpointConfiguration(
						new AwsClientBuilder.EndpointConfiguration("http://localhost:8000", "ap-south-1"))
				.build();
		return client;
	}
}
