package com.learnkafka.libraryevents.producer.producer;

import java.util.List;
import java.util.concurrent.ExecutionException;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Component;
import org.springframework.util.concurrent.ListenableFuture;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.learnkafka.libraryevents.producer.domain.LibraryEvent;

import lombok.extern.slf4j.Slf4j;

@Component
@Slf4j
public class LibraryEventProducer {
	private static final String TOPIC = "library-events";
	private KafkaTemplate<Integer, String> kafkaTemplate;
	private ObjectMapper objectMapper;

	public LibraryEventProducer(KafkaTemplate<Integer, String> kafkaTemplate, ObjectMapper objectMapper) {
		super();
		this.kafkaTemplate = kafkaTemplate;
		this.objectMapper = objectMapper;
	}

	public SendResult<Integer, String> sendLibraryEventSynchronous(final LibraryEvent libraryEvent) throws Exception {
		ListenableFuture<SendResult<Integer, String>> listenableFuture = kafkaTemplate.sendDefault(libraryEvent.getLibraryEventId(),
				objectMapper.writeValueAsString(libraryEvent));
		try {
			SendResult<Integer, String> sendResult = listenableFuture.get();
			return sendResult;
		} catch (InterruptedException | ExecutionException e) {
			log.error("exception thrown", e);
			throw e;
		} catch (Exception e2) {
			log.error("exception thrown", e2);
			throw e2;
		}
	}

	public ListenableFuture<SendResult<Integer, String>> sendLibraryEventApproach2(final LibraryEvent libraryEvent)
			throws JsonProcessingException {

		String value = objectMapper.writeValueAsString(libraryEvent);
		ProducerRecord<Integer, String> producerRecord = createProducerRecord(TOPIC, null, value);
		
		ListenableFuture<SendResult<Integer, String>> listenableFuture = kafkaTemplate.send(producerRecord);
		
		listenableFuture.addCallback(this::onSuccess, this::onFailure);
		return listenableFuture;
	}

	private ProducerRecord<Integer, String> createProducerRecord(String topic, Integer key, String value) {
		List<Header> headers = List.of(new RecordHeader("event-source", "scanner".getBytes()));

		return new ProducerRecord<Integer, String>(topic, null, key, value, headers);
	}

	public void sendLibraryEvent(final LibraryEvent libraryEvent) throws JsonProcessingException {
		ListenableFuture<SendResult<Integer, String>> listenableFuture = kafkaTemplate.sendDefault(libraryEvent.getLibraryEventId(),
				objectMapper.writeValueAsString(libraryEvent));
		listenableFuture.addCallback(this::onSuccess, this::onFailure);
	}

	private void onSuccess(SendResult<Integer, String> sendResult) {
		log.info("onSuccess producerRecord={}, topic={}, partition={}", sendResult.getProducerRecord(),
				sendResult.getRecordMetadata().topic(), sendResult.getRecordMetadata().partition());
	}

	private void onFailure(Throwable ex) {
		log.error("onFailure exception=" + ex.getMessage(), ex);
	}
}
