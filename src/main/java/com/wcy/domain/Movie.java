package com.wcy.domain;

import java.util.HashMap;
import java.util.Map;

import com.amazonaws.services.dynamodbv2.model.AttributeValue;

public class Movie {

	private Integer year;
	
	private String title;
	
	private String genres;
	
	private Double rating;
	
	public Movie() {
	}

	public Map<String, AttributeValue> movie2item(Movie movie){
		Map<String, AttributeValue> item=new HashMap<String, AttributeValue>();
		item.put("year", new AttributeValue().withN(movie.getYear().toString()));
		item.put("title", new AttributeValue().withS(movie.getTitle()));
		item.put("genres",new AttributeValue().withS(movie.getGenres()));
		item.put("rating", new AttributeValue().withN(movie.getRating().toString()));
		return item;
		
	}
	public Integer getYear() {
		return year;
	}

	public void setYear(Integer year) {
		this.year = year;
	}

	public String getTitle() {
		return title;
	}

	public void setTitle(String title) {
		this.title = title;
	}

	public String getGenres() {
		return genres;
	}

	public void setGenres(String genres) {
		this.genres = genres;
	}

	public Double getRating() {
		return rating;
	}

	public void setRating(Double rating) {
		this.rating = rating;
	}
	
	
}
