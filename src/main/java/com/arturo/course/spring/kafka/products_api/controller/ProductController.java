package com.arturo.course.spring.kafka.products_api.controller;

import com.arturo.course.spring.kafka.products_api.models.Reply;
import com.arturo.course.spring.kafka.products_api.models.dto.ProductDto;
import com.arturo.course.spring.kafka.products_api.service.ProductCommandService;
import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.time.Duration;
import java.util.Map;

@RestController
@RequiredArgsConstructor
@RequestMapping("/api/products")
public class ProductController {
    private final ProductCommandService productCommandService;

    @PostMapping
    public ResponseEntity<?> create(@Valid @RequestBody ProductDto product) {
        Reply<?> reply = productCommandService.sendCreateAsyncAwait(product, Duration.ofSeconds(5));
        if ("SUCCESS".equalsIgnoreCase(reply.statusMessage())) {
            return ResponseEntity.ok(reply.body());
        }
        return ResponseEntity.badRequest().body(Map.of("error", reply.message()));
    }
}
