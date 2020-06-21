package com.learnkafka.libraryevents.producer.controller;

import javax.validation.Valid;

import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

import com.learnkafka.libraryevents.producer.domain.LibraryEvent;
import com.learnkafka.libraryevents.producer.domain.LibraryEventType;
import com.learnkafka.libraryevents.producer.producer.LibraryEventProducer;

import lombok.extern.slf4j.Slf4j;


@Slf4j
@RestController
public class LibraryEventsController {
	
	private LibraryEventProducer libraryEventProducer;
	
	

	public LibraryEventsController(LibraryEventProducer libraryEventProducer) {
		super();
		this.libraryEventProducer = libraryEventProducer;
	}

	@PostMapping("/v1/libraryevent")
	public ResponseEntity<LibraryEvent> postLibraryEvent(@RequestBody @Valid LibraryEvent libraryEvent) throws Exception {

		libraryEvent.setLibraryEventType(LibraryEventType.NEW);
		libraryEventProducer.sendLibraryEventApproach2(libraryEvent);
		
		return ResponseEntity.status(HttpStatus.CREATED).body(libraryEvent);
	}

	@PutMapping("/v1/libraryevent")
	public ResponseEntity<?> putLibraryEvent(@RequestBody @Valid LibraryEvent libraryEvent) throws Exception {

		if( libraryEvent.getLibraryEventId()==null || libraryEvent.getLibraryEventId()<=0) {
			return ResponseEntity.badRequest().body("Please pass the libraryEventId");
		}
		libraryEvent.setLibraryEventType(LibraryEventType.UPDATE);
		libraryEventProducer.sendLibraryEventApproach2(libraryEvent);
		
		return ResponseEntity.status(HttpStatus.OK).body(libraryEvent);
	}
}
