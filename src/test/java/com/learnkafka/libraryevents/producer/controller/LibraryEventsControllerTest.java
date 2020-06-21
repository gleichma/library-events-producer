package com.learnkafka.libraryevents.producer.controller;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.contains;
import static org.mockito.ArgumentMatchers.isA;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.put;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.content;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.learnkafka.libraryevents.producer.domain.Book;
import com.learnkafka.libraryevents.producer.domain.LibraryEvent;
import com.learnkafka.libraryevents.producer.domain.LibraryEventType;
import com.learnkafka.libraryevents.producer.producer.LibraryEventProducer;

@WebMvcTest(LibraryEventsController.class)
@AutoConfigureMockMvc
class LibraryEventsControllerTest {
	@Autowired
	private MockMvc mockMvc;

	@MockBean
	private LibraryEventProducer libraryEventProducer;
	
	@Captor
	private ArgumentCaptor<LibraryEvent> libraryEventCaptor;

	private ObjectMapper jsonMapper = new ObjectMapper();

	@Test
	void postLibraryEvent() throws Exception {
		// given
		Book book = Book.builder().bookId(123).bookName("Kafka using Spring Boot").bookAuthor("Dilip").build();
		LibraryEvent libraryEvent = LibraryEvent.builder().libraryEventId(null).book(book).build();

		String libraryEventJson = jsonMapper.writeValueAsString(libraryEvent);
		when(libraryEventProducer.sendLibraryEventApproach2(libraryEventCaptor.capture())).thenReturn(null);

		// when and then
		mockMvc.perform(post("/v1/libraryevent").content(libraryEventJson).contentType(MediaType.APPLICATION_JSON))
				.andExpect(status().isCreated());
		assertThat(libraryEventCaptor.getValue().getLibraryEventType()).isEqualTo(LibraryEventType.NEW);
	}
	
	@Test
	void updateLibraryEvent() throws Exception {
		// given
		Book book = Book.builder().bookId(123).bookName("Kafka using Spring Boot").bookAuthor("Dilip").build();
		LibraryEvent libraryEvent = LibraryEvent.builder().libraryEventId(456).book(book).build();
		
		String libraryEventJson = jsonMapper.writeValueAsString(libraryEvent);
		
		when(libraryEventProducer.sendLibraryEventApproach2(libraryEventCaptor.capture())).thenReturn(null);
		
		// when and then
		mockMvc.perform(put("/v1/libraryevent").content(libraryEventJson).contentType(MediaType.APPLICATION_JSON))
			.andExpect(status().isOk());
		assertThat(libraryEventCaptor.getValue().getLibraryEventType()).isEqualTo(LibraryEventType.UPDATE);
	}

	@Test
	void updateLibraryEvent_withNullLibraryEventId() throws Exception {
		// given
		Book book = Book.builder().bookId(123).bookName("Kafka using Spring Boot").bookAuthor("Dilip").build();
		LibraryEvent libraryEvent = LibraryEvent.builder().libraryEventId(null).book(book).build();
		
		String libraryEventJson = jsonMapper.writeValueAsString(libraryEvent);
		
		when(libraryEventProducer.sendLibraryEventApproach2(isA(LibraryEvent.class))).thenReturn(null);
		
		// when and then
		mockMvc.perform(put("/v1/libraryevent").content(libraryEventJson).contentType(MediaType.APPLICATION_JSON))
			.andExpect(status().isBadRequest());
	}

	
	
	@Test
	void testPostLibraryEvent_NullBook_ValidationFails() throws Exception {
		Book book = Book.builder().bookId(123).bookName("Kafka using Spring Boot").bookAuthor("Dilip").build();
		LibraryEvent libraryEvent = LibraryEvent.builder().libraryEventId(null).book(null).build();

		String libraryEventJson = jsonMapper.writeValueAsString(libraryEvent);

		when(libraryEventProducer.sendLibraryEventApproach2(isA(LibraryEvent.class))).thenReturn(null);

		mockMvc.perform(post("/v1/libraryevent").content(libraryEventJson).contentType(MediaType.APPLICATION_JSON))
				.andExpect(status().is4xxClientError());
	}

	@Test
	void testPostLibraryEvent_BookMissingId_ValidationFails() throws Exception {
		Book book = Book.builder().bookId(null).bookName("Kafka using Spring Boot").bookAuthor("Dilip").build();
		LibraryEvent libraryEvent = LibraryEvent.builder().libraryEventId(null).book(book).build();

		String libraryEventJson = jsonMapper.writeValueAsString(libraryEvent);

		when(libraryEventProducer.sendLibraryEventApproach2(isA(LibraryEvent.class))).thenReturn(null);

		mockMvc.perform(post("/v1/libraryevent").content(libraryEventJson).contentType(MediaType.APPLICATION_JSON))
				.andExpect(status().is4xxClientError());
	}

	@Test
	void testPostLibraryEvent_BookMissingName_ValidationFails() throws Exception {
		Book book = Book.builder().bookId(123).bookName("").bookAuthor("Dilip").build();
		LibraryEvent libraryEvent = LibraryEvent.builder().libraryEventId(null).book(book).build();

		String libraryEventJson = jsonMapper.writeValueAsString(libraryEvent);

		when(libraryEventProducer.sendLibraryEventApproach2(isA(LibraryEvent.class))).thenReturn(null);

		mockMvc.perform(post("/v1/libraryevent").content(libraryEventJson).contentType(MediaType.APPLICATION_JSON))
				.andExpect(status().is4xxClientError());
	}

	@Test
	void testPostLibraryEvent_BookMissingAuthor_ValidationFails() throws Exception {
		Book book = Book.builder().bookId(123).bookName("Kafka using Spring Boot").bookAuthor("").build();
		LibraryEvent libraryEvent = LibraryEvent.builder().libraryEventId(null).book(book).build();

		String libraryEventJson = jsonMapper.writeValueAsString(libraryEvent);

		when(libraryEventProducer.sendLibraryEventApproach2(isA(LibraryEvent.class))).thenReturn(null);

		mockMvc.perform(post("/v1/libraryevent").content(libraryEventJson).contentType(MediaType.APPLICATION_JSON))
				.andExpect(status().is4xxClientError());
	}

}
