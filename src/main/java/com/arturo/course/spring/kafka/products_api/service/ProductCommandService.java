package com.arturo.course.spring.kafka.products_api.service;

import com.arturo.course.spring.kafka.products_api.models.dto.ProductDto;

public interface ProductCommandService {
    void sendCreate(ProductDto productDto);
}
