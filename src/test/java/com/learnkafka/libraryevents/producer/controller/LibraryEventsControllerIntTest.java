package com.learnkafka.libraryevents.producer.controller;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Map;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.SpringBootTest.WebEnvironment;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.springframework.test.context.TestPropertySource;

import com.learnkafka.libraryevents.producer.domain.Book;
import com.learnkafka.libraryevents.producer.domain.LibraryEvent;

@EmbeddedKafka(topics = {"library-events"}, partitions = 3)
@SpringBootTest(webEnvironment = WebEnvironment.RANDOM_PORT)
@TestPropertySource(properties = {
		"spring.kafka.producer.bootstrap-servers=${spring.embedded.kafka.brokers}",
		"spring.kafka.admin.properties.bootstrap.servers=${spring.embedded.kafka.brokers}", })
public class LibraryEventsControllerIntTest {
	private static final String GROUP = "group1";

	@Autowired
	private TestRestTemplate testRestTemplate;
	
	@Autowired
	private EmbeddedKafkaBroker embeddedKafkaBroker;
	
	private Consumer<Integer, String> consumer;

	private String TOPIC = "library-events";
	
	@BeforeEach
	private void setUp() {

		Map<String, Object> consumerConfig = KafkaTestUtils.consumerProps(GROUP,"true",embeddedKafkaBroker);
		consumer = new DefaultKafkaConsumerFactory<>(consumerConfig,new IntegerDeserializer(), new StringDeserializer()).createConsumer();
		embeddedKafkaBroker.consumeFromAllEmbeddedTopics(consumer);
	}
	
	@AfterEach
	private void tearDown() {
		consumer.close();
	}
	
	@Timeout(10)
	@Test
	public void postLibraryEvent() throws Exception {
		Book book = Book.builder().bookId(123).bookName("Kafka using Spring Boot").bookAuthor("Dilip").build();
		LibraryEvent libraryEvent = LibraryEvent.builder().libraryEventId(null).book(book).build();
		
		HttpHeaders headers = new HttpHeaders();
		headers.setContentType(MediaType.APPLICATION_JSON);
		HttpEntity<LibraryEvent> request = new HttpEntity<LibraryEvent>(libraryEvent, headers);
		
		
		ResponseEntity<LibraryEvent> response = testRestTemplate.exchange("/v1/libraryevent", HttpMethod.POST, request, LibraryEvent.class);
		
		assertThat(response.getStatusCode()).isEqualTo(HttpStatus.CREATED);
		
		ConsumerRecord<Integer, String> record = KafkaTestUtils.getSingleRecord(consumer, TOPIC);
		assertThat(record.key()).isNull();
		assertThat(record.value()).isEqualTo("{\"libraryEventId\":null,\"libraryEventType\":\"NEW\",\"book\":{\"bookId\":123,\"bookName\":\"Kafka using Spring Boot\",\"bookAuthor\":\"Dilip\"}}");
	}

	@Timeout(10)
	@Test
	public void putLibraryEvent() throws Exception {
		// given
		Book book = Book.builder().bookId(123).bookName("Kafka using Spring Boot").bookAuthor("Dilip").build();
		LibraryEvent libraryEvent = LibraryEvent.builder().libraryEventId(456).book(book).build();
		
		HttpHeaders headers = new HttpHeaders();
		headers.setContentType(MediaType.APPLICATION_JSON);
		HttpEntity<LibraryEvent> request = new HttpEntity<LibraryEvent>(libraryEvent, headers);
		
		// when
		ResponseEntity<LibraryEvent> response = testRestTemplate.exchange("/v1/libraryevent", HttpMethod.PUT, request, LibraryEvent.class);
		
		// then
		assertThat(response.getStatusCode()).isEqualTo(HttpStatus.OK);
		
		ConsumerRecord<Integer, String> record = KafkaTestUtils.getSingleRecord(consumer, TOPIC);
		assertThat(record.key()).isNull();
		assertThat(record.value()).isEqualTo("{\"libraryEventId\":456,\"libraryEventType\":\"UPDATE\",\"book\":{\"bookId\":123,\"bookName\":\"Kafka using Spring Boot\",\"bookAuthor\":\"Dilip\"}}");
	}
	
	@Timeout(10)
	@Test
	public void postLibraryEventEmptyBookName_ShouldReturnBAD_REQUEST() throws Exception {
		Book book = Book.builder().bookId(123).bookName("").bookAuthor("Dilip").build();
		LibraryEvent libraryEvent = LibraryEvent.builder().libraryEventId(null).book(book).build();
		
		HttpHeaders headers = new HttpHeaders();
		headers.setContentType(MediaType.APPLICATION_JSON);
		HttpEntity<LibraryEvent> request = new HttpEntity<LibraryEvent>(libraryEvent, headers);
		
		
		ResponseEntity<LibraryEvent> response = testRestTemplate.exchange("/v1/libraryevent", HttpMethod.POST, request, LibraryEvent.class);
		
		assertThat(response.getStatusCode()).isEqualTo(HttpStatus.BAD_REQUEST);
	}
}
