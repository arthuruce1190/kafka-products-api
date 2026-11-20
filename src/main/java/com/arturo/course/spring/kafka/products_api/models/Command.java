package com.arturo.course.spring.kafka.products_api.models;

public record Command<T>(String type, String id, Boolean isActive, T body) {
}
