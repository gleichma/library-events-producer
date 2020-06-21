package com.learnkafka.libraryevents.producer.domain;

import javax.validation.Valid;
import javax.validation.constraints.NotNull;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder
@AllArgsConstructor
@NoArgsConstructor
public class LibraryEvent {
	private Integer libraryEventId;
	private LibraryEventType libraryEventType;
	@NotNull
	@Valid
	private Book book;
}
