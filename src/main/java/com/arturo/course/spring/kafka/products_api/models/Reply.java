package com.arturo.course.spring.kafka.products_api.models;

import org.springframework.http.HttpStatus;

public record Reply<T>(HttpStatus status, String statusMessage, String message, T body) {
}
