package com.wcy.controller;

import java.util.HashMap;
import java.util.Map;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.model.AttributeDefinition;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.CreateGlobalSecondaryIndexAction;
import com.amazonaws.services.dynamodbv2.model.DeleteGlobalSecondaryIndexAction;
import com.amazonaws.services.dynamodbv2.model.DeleteItemRequest;
import com.amazonaws.services.dynamodbv2.model.GetItemResult;
import com.amazonaws.services.dynamodbv2.model.GlobalSecondaryIndexUpdate;
import com.amazonaws.services.dynamodbv2.model.KeySchemaElement;
import com.amazonaws.services.dynamodbv2.model.KeyType;
import com.amazonaws.services.dynamodbv2.model.Projection;
import com.amazonaws.services.dynamodbv2.model.ProjectionType;
import com.amazonaws.services.dynamodbv2.model.ProvisionedThroughput;
import com.amazonaws.services.dynamodbv2.model.PutItemRequest;
import com.amazonaws.services.dynamodbv2.model.PutItemResult;
import com.amazonaws.services.dynamodbv2.model.QueryRequest;
import com.amazonaws.services.dynamodbv2.model.QueryResult;
import com.amazonaws.services.dynamodbv2.model.ReturnValue;
import com.amazonaws.services.dynamodbv2.model.ScalarAttributeType;
import com.amazonaws.services.dynamodbv2.model.ScanRequest;
import com.amazonaws.services.dynamodbv2.model.ScanResult;
import com.amazonaws.services.dynamodbv2.model.UpdateItemRequest;
import com.amazonaws.services.dynamodbv2.model.UpdateItemResult;
import com.amazonaws.services.dynamodbv2.model.UpdateTableRequest;
import com.amazonaws.services.dynamodbv2.model.UpdateTableResult;
import com.wcy.domain.Movie;

@RestController
public class LowestInterface {

	@Autowired
	AmazonDynamoDB client;
	
	@PostMapping("/put-item-l")
	Object putItem(@RequestBody Movie movie){
		Map<String,AttributeValue> item=new Movie().movie2item(movie);
		PutItemRequest request=new PutItemRequest().withTableName("Movies").withItem(item).withReturnConsumedCapacity("TOTAL");
		PutItemResult response=client.putItem(request);
		return response.getConsumedCapacity();
	}
	
	@GetMapping("/get-item-l")
	Object getItem(Integer year,String title){
		Map<String,AttributeValue> key=new HashMap<String, AttributeValue>();
		key.put("year", new AttributeValue().withN(year.toString()));
		key.put("title", new AttributeValue().withS(title));
		
		GetItemResult result=client.getItem("Movies", key);
		return result.getItem();
	}
	
	
	@PutMapping("/update-item-l")
	Object updateItem(@RequestBody Movie movie){
		Map<String, AttributeValue> key=new HashMap<String, AttributeValue>();
		key.put("year", new AttributeValue().withN(movie.getYear().toString()));
		key.put("title",new AttributeValue().withS(movie.getTitle()));
		
		Map<String, AttributeValue> valueMap=new HashMap<String, AttributeValue>();
		valueMap.put(":r", new AttributeValue().withN(movie.getRating().toString()));
		Map<String, String> nameMap=new HashMap<String, String>();
		nameMap.put("#R", "rating");
		
		UpdateItemRequest request=new UpdateItemRequest().withTableName("Movies").withKey(key)
				.withUpdateExpression("SET #R = :r").withExpressionAttributeNames(nameMap).withExpressionAttributeValues(valueMap)
				.withReturnValues(ReturnValue.ALL_NEW);
		
		UpdateItemResult result=client.updateItem(request);
		return result.getAttributes();
	}
	
	@DeleteMapping("/delete-item-l/{year},{title}")
	Object deleteItem(@PathVariable Integer year,@PathVariable String title){
		Map<String, AttributeValue> key=new HashMap<String, AttributeValue>();
		key.put("year", new AttributeValue().withN(year.toString()));
		key.put("title", new AttributeValue().withS(title));
		
		DeleteItemRequest request=new DeleteItemRequest().withTableName("Movies").withKey(key);
		client.deleteItem(request);
		return null;
	}
	
	//使用query检索"title"小于等于"t3"的电影
	@GetMapping("query-l")
	Object query(Integer year){
		Map<String, AttributeValue> valuemMap=new HashMap<String, AttributeValue>();
		valuemMap.put(":pkName", new AttributeValue().withN(year.toString()));
		valuemMap.put(":rkName", new AttributeValue().withS("t3"));
		
		Map<String, String> keyMap=new HashMap<String, String>();
		keyMap.put("#Y", "year");
		keyMap.put("#T", "title");
		
		QueryRequest request=new QueryRequest().withTableName("Movies")
				.withKeyConditionExpression("#Y = :pkName AND #T <= :rkName")
				.withExpressionAttributeNames(keyMap)
				.withExpressionAttributeValues(valuemMap);
		
		QueryResult result=client.query(request);
		return result.getItems();
	}
	
	@GetMapping("/scan-l")
	Object scan(){
		ScanRequest request=new ScanRequest().withTableName("Movies");
		ScanResult result=client.scan(request);
		return result.getItems();
	}
	
	//创建和删除GSI("genres","rating")
	@PutMapping("/update-table-l")
	void updateTable(){
		KeySchemaElement kse1=new KeySchemaElement().withAttributeName("genres").withKeyType(KeyType.HASH);
		KeySchemaElement kse2=new KeySchemaElement().withAttributeName("rating").withKeyType(KeyType.RANGE);
		
		ProvisionedThroughput provisionedThroughput=new ProvisionedThroughput(10L, 5L);
		
		CreateGlobalSecondaryIndexAction creatGSIAction=new CreateGlobalSecondaryIndexAction()
				.withKeySchema(kse1,kse2).withIndexName("genres-rating")
				.withProvisionedThroughput(provisionedThroughput)
				.withProjection(new Projection().withProjectionType(ProjectionType.ALL));
		
		//创建索引
		GlobalSecondaryIndexUpdate GSIUpdate=new GlobalSecondaryIndexUpdate().withCreate(creatGSIAction);
		
		AttributeDefinition attrDef1=new AttributeDefinition("genres", ScalarAttributeType.S);
		AttributeDefinition attrDef2=new AttributeDefinition("rating",ScalarAttributeType.N);
		
		UpdateTableRequest request=new UpdateTableRequest().withTableName("Movies")
				.withAttributeDefinitions(attrDef1,attrDef2)
				.withGlobalSecondaryIndexUpdates(GSIUpdate);
		UpdateTableResult result=client.updateTable(request);
	
		
		
		//删除索引
//		DeleteGlobalSecondaryIndexAction deleteGSIAction=new DeleteGlobalSecondaryIndexAction().withIndexName("genres-rating");
//		GlobalSecondaryIndexUpdate GSIUpdate2=new GlobalSecondaryIndexUpdate().withDelete(deleteGSIAction);
//		
//		UpdateTableRequest request2=new UpdateTableRequest().withTableName("Movies")
//				.withGlobalSecondaryIndexUpdates(GSIUpdate2);
//		UpdateTableResult result2=client.updateTable(request2);
	}
	
	@GetMapping("/query-index-l")
	Object queryIndex(String genres){
		Map<String, AttributeValue> valueMap=new HashMap<String, AttributeValue>();
		valueMap.put(":g", new AttributeValue().withS(genres));
		valueMap.put(":r", new AttributeValue().withN("5"));
		
		QueryRequest request=new QueryRequest().withTableName("Movies")
				.withKeyConditionExpression("genres = :g AND rating <= :r")
				.withExpressionAttributeValues(valueMap)
				.withIndexName("genres-rating");
		
		QueryResult result=client.query(request);
		
		return result.getItems();
	}
}
