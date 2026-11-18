package com.arturo.course.spring.kafka.products_api.service;

import com.arturo.course.spring.kafka.products_api.models.Reply;
import com.arturo.course.spring.kafka.products_api.models.dto.ProductDto;

import java.time.Duration;

public interface ProductCommandService {
    Reply<?> sendCreateAsyncAwait(ProductDto productDto, Duration timeout);
}
