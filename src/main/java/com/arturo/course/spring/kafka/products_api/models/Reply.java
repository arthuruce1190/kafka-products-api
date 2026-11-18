package com.arturo.course.spring.kafka.products_api.models;

public record Reply<T>(Integer status, String statusMessage, String message, T body) {
}
