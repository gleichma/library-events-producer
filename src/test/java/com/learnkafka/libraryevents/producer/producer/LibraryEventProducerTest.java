package com.learnkafka.libraryevents.producer.producer;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.isA;
import static org.mockito.Mockito.when;

import java.util.concurrent.ExecutionException;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Spy;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.util.concurrent.ListenableFuture;
import org.springframework.util.concurrent.SettableListenableFuture;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.learnkafka.libraryevents.producer.domain.Book;
import com.learnkafka.libraryevents.producer.domain.LibraryEvent;

@ExtendWith(MockitoExtension.class)
public class LibraryEventProducerTest {

	@Mock
	private KafkaTemplate<Integer, String> kafkaTemplate;

	@Spy
	private ObjectMapper objectMapper;

	@InjectMocks
	private LibraryEventProducer out;

	@Test
	public void sendLibraryEventApproach2_OnFailure() {
		Book book = Book.builder().bookId(123).bookName("Kafka using Spring Boot").bookAuthor("Dilip").build();
		LibraryEvent libraryEvent = LibraryEvent.builder().libraryEventId(null).book(book).build();
		SettableListenableFuture listenableFuture = new SettableListenableFuture<>();
		listenableFuture.setException(new RuntimeException("errro calling kafka"));

		when(kafkaTemplate.send(isA(ProducerRecord.class))).thenReturn(listenableFuture);

		assertThrows(Exception.class, () -> out.sendLibraryEventApproach2(libraryEvent).get());
	}

	@Test
	public void sendLibraryEventApproach2_OnSuccess() throws Exception {
		// given
		Book book = Book.builder().bookId(123).bookName("Kafka using Spring Boot").bookAuthor("Dilip").build();
		LibraryEvent libraryEvent = LibraryEvent.builder().libraryEventId(null).book(book).build();

		ProducerRecord<Integer, String> producerRecord = new ProducerRecord<Integer, String>("library-events",
				libraryEvent.getLibraryEventId(), objectMapper.writeValueAsString(libraryEvent));
		RecordMetadata recordMetadata = new RecordMetadata(new TopicPartition("library-events", 1), 1, 1, 342, System.currentTimeMillis(),
				1, 2);
		SendResult<Integer, String> sendResult = new SendResult<>(producerRecord, recordMetadata);
		SettableListenableFuture<SendResult<Integer, String>> listenableFuture = new SettableListenableFuture<>();
		listenableFuture.set(sendResult);

		when(kafkaTemplate.send(isA(ProducerRecord.class))).thenReturn(listenableFuture);

		// when
		ListenableFuture<SendResult<Integer, String>> result = out.sendLibraryEventApproach2(libraryEvent);
		
		// then
		SendResult<Integer,String> sendResultReceived = result.get();
		String libraryEventReceived = sendResultReceived.getProducerRecord().value();
	}

}
