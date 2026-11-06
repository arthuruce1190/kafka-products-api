package com.arturo.course.spring.kafka.products_api.controller;

import com.arturo.course.spring.kafka.products_api.models.dto.ProductDto;
import com.arturo.course.spring.kafka.products_api.service.ProductCommandService;
import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.Map;

@RestController
@RequiredArgsConstructor
@RequestMapping("/api/products")
public class ProductController {
    private final ProductCommandService productCommandService;

    @PostMapping
    public ResponseEntity<?> create(@Valid @RequestBody ProductDto product) {
        productCommandService.sendCreate(product);
        return ResponseEntity.ok().body(Map.of("Message", "Sent success"));
    }
}
