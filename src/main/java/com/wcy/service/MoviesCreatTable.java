package com.wcy.service;

import java.util.Arrays;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.dynamodbv2.model.AttributeDefinition;
import com.amazonaws.services.dynamodbv2.model.KeySchemaElement;
import com.amazonaws.services.dynamodbv2.model.KeyType;
import com.amazonaws.services.dynamodbv2.model.ProvisionedThroughput;
import com.amazonaws.services.dynamodbv2.model.ScalarAttributeType;

@Service
public class MoviesCreatTable {
	
	@Autowired
	AmazonDynamoDB client;
	
	public void movieCreatTabel(){
		DynamoDB dynamoDB=new DynamoDB(client);
		try {
			System.out.println("Attempting to creat table,please wait...");
			Table table=dynamoDB.createTable("Movies", 
					Arrays.asList(new KeySchemaElement("year", KeyType.HASH),new KeySchemaElement("title", KeyType.RANGE)),
					Arrays.asList(new AttributeDefinition("year", ScalarAttributeType.N),new AttributeDefinition("title", ScalarAttributeType.S)), 
					new ProvisionedThroughput(10L, 5L));
			table.waitForActive();
			System.out.println("Success. Table Status: "+table.getDescription().getTableStatus());
		} catch (InterruptedException e) {
			System.err.println("Unable to create table: ");
			System.err.println(e.getMessage());
		}
	}
}
