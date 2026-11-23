package com.arturo.course.spring.kafka.products_api.models;

public record Command<T>(CommandType type, String id, Boolean isActive, T body) {
}
