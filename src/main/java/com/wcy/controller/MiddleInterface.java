package com.wcy.controller;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

import com.amazonaws.services.dynamodbv2.document.AttributeUpdate;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Index;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.ItemCollection;
import com.amazonaws.services.dynamodbv2.document.PutItemOutcome;
import com.amazonaws.services.dynamodbv2.document.QueryOutcome;
import com.amazonaws.services.dynamodbv2.document.ScanOutcome;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.dynamodbv2.document.UpdateItemOutcome;
import com.amazonaws.services.dynamodbv2.document.spec.DeleteItemSpec;
import com.amazonaws.services.dynamodbv2.document.spec.PutItemSpec;
import com.amazonaws.services.dynamodbv2.document.spec.QuerySpec;
import com.amazonaws.services.dynamodbv2.document.spec.UpdateItemSpec;
import com.amazonaws.services.dynamodbv2.model.ReturnConsumedCapacity;
import com.amazonaws.services.dynamodbv2.model.ReturnValue;
import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.wcy.domain.Movie;

@RestController
public class MiddleInterface {

	@Autowired
	DynamoDB dynamoDB;
	
	@PostMapping("/put-item-m")
	Object putItem(@RequestBody Movie movie){
		
		Item item=new Item().withPrimaryKey("year", movie.getYear(), "title", movie.getTitle())
				.withString("genres",movie.getGenres()).withDouble("rating", movie.getRating());
		
		Table table=dynamoDB.getTable("Movies");
		PutItemSpec spec=new PutItemSpec().withItem(item).withReturnConsumedCapacity(ReturnConsumedCapacity.TOTAL);
		PutItemOutcome outcome=table.putItem(spec);
		return outcome.getPutItemResult().getConsumedCapacity();
	}
	
	@GetMapping("/get-item-m")
	Object getItem(Integer year,String title){
		Table table=dynamoDB.getTable("Movies");
		Item item=table.getItem("year",year, "title", title);
		return item.toJSONPretty();
	}
	
	@PutMapping("/update-item-m")
	Object updateItem(@RequestBody Movie movie){
		List<AttributeUpdate> attributeUpdates=new ArrayList<AttributeUpdate>();
		if (movie.getGenres()!=null) {
			attributeUpdates.add(new AttributeUpdate("genres").put(movie.getGenres()));
		}
		if (movie.getRating()!=null) {
			attributeUpdates.add(new AttributeUpdate("rating").put(movie.getRating()));
		}
		
		Table table=dynamoDB.getTable("Movies");
		UpdateItemSpec spec=new UpdateItemSpec().withPrimaryKey("year", movie.getYear(), "title", movie.getTitle())
				.withAttributeUpdate(attributeUpdates)
				.withReturnValues(ReturnValue.ALL_NEW);
		UpdateItemOutcome outcome=table.updateItem(spec);
		return outcome.getItem().toJSONPretty();
	}
	
	@DeleteMapping("/delete-item-m/{year},{title}")
	Object deleteItem(@PathVariable Integer year,@PathVariable String title){
		Table table=dynamoDB.getTable("Movies");
		DeleteItemSpec spec=new DeleteItemSpec().withPrimaryKey("year", year, "title", title);
		table.deleteItem(spec);
		return null;
	}
	
	//使用query检索"title"小于等于"t3"的电影
	@GetMapping("/query-m")
	Object query(Integer year){
		Table table=dynamoDB.getTable("Movies");
		Map<String, String> keyMap=new HashMap<String, String>();
		keyMap.put("#Y", "year");
		Map<String, Object> valueMap=new HashMap<String, Object>();
		valueMap.put(":pkName", year);
		valueMap.put(":rkName", "t3");
		
		QuerySpec spec=new QuerySpec().withKeyConditionExpression("#Y = :pkName AND title <= :rkName")
				.withNameMap(keyMap).withValueMap(valueMap);
		ItemCollection<QueryOutcome> outcomeCollection=table.query(spec);
		
		Iterator<Item> iterator=outcomeCollection.iterator();
		
		ObjectMapper mapper=new ObjectMapper();
		List<Movie> movies=new ArrayList<Movie>();
		while (iterator.hasNext()) {
			try {
				Movie movie=mapper.readValue(iterator.next().toJSONPretty(), Movie.class);
				movies.add(movie);
			} catch (JsonParseException e) {
				e.printStackTrace();
			} catch (JsonMappingException e) {
				e.printStackTrace();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		
		return movies;
	}
	
	@GetMapping("/scan-m")
	Object scan(){
		Table table=dynamoDB.getTable("Movies");
		ItemCollection<ScanOutcome> outcomeCollection=table.scan();
		Iterator<Item> iterator=outcomeCollection.iterator();
		
		ObjectMapper mapper=new ObjectMapper();
		List<Movie> movies=new ArrayList<Movie>();
		while (iterator.hasNext()) {
			try {
				Movie movie=mapper.readValue(iterator.next().toJSONPretty(), Movie.class);
				movies.add(movie);
			} catch (JsonParseException e) {
				e.printStackTrace();
			} catch (JsonMappingException e) {
				e.printStackTrace();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		
		return movies;
	}
	
	@GetMapping("/query-index-m")
	Object queryIndex(String genres){
		Map<String, Object> valueMap=new HashMap<String, Object>();
		valueMap.put(":g",genres);
		valueMap.put(":r", 5);
		
		QuerySpec spec=new QuerySpec().withKeyConditionExpression("genres = :g AND rating <= :r")
				.withValueMap(valueMap);
		Table table=dynamoDB.getTable("Movies");
		Index index=table.getIndex("genres-rating");
		
		ItemCollection<QueryOutcome> outcomeCollection=index.query(spec);
		Iterator<Item> iterator=outcomeCollection.iterator();
		List<Movie> movies=new ArrayList<Movie>();
		
		ObjectMapper mapper=new ObjectMapper();
		while (iterator.hasNext()) {
			try {
				Movie movie=mapper.readValue(iterator.next().toJSONPretty(), Movie.class);
				movies.add(movie);
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		
		return movies;
	}
}
