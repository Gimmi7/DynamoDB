package com.wcy;

import org.springframework.beans.factory.annotation.Autowired;	
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import com.wcy.service.MoviesCreatTable;

@SpringBootApplication
public class DdbLowestAPIApplication {

	@Autowired
	MoviesCreatTable moviesCreatTable;
	
	public static void main(String[] args) {
		SpringApplication.run(DdbLowestAPIApplication.class, args);
	}

//	@Override
//	public void run(String... args) throws Exception {
//		moviesCreatTable.movieCreatTabel();
//	}

	
}
