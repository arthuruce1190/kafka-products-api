package com.arturo.course.spring.kafka.products_api.controller;

import com.arturo.course.spring.kafka.products_api.models.Reply;
import com.arturo.course.spring.kafka.products_api.models.dto.ProductDto;
import com.arturo.course.spring.kafka.products_api.service.ProductCommandService;
import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import org.jspecify.annotations.NonNull;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.time.Duration;
import java.util.Map;

import static org.springframework.http.HttpStatus.BAD_REQUEST;
import static org.springframework.http.HttpStatus.OK;

@RestController
@RequiredArgsConstructor
@RequestMapping("/api/products")
public class ProductController {
    private final ProductCommandService productCommandService;

    private static @NonNull ResponseEntity<?> getResponseEntity(Reply<?> reply) {
        if (OK.getReasonPhrase().equalsIgnoreCase(reply.status().getReasonPhrase())) {
            return ResponseEntity.ok(reply.body());
        }
        return ResponseEntity
            .badRequest()
            .body(Map.of(BAD_REQUEST.getReasonPhrase(), reply.message()));
    }

    @PostMapping
    public ResponseEntity<?> create(@Valid @RequestBody ProductDto product) {
        return getResponseEntity(productCommandService.sendCreateAsyncAwait(product, Duration.ofSeconds(5)));
    }

    @PutMapping
    public ResponseEntity<?> update(@Valid @RequestBody ProductDto product) {
        return getResponseEntity(productCommandService.sendUpdateAsyncAwait(product, Duration.ofSeconds(5)));
    }

    @DeleteMapping
    public ResponseEntity<?> delete(@RequestBody ProductDto product) {
        return getResponseEntity(productCommandService.sendDeleteAsyncAwait(product, Duration.ofSeconds(5)));
    }

    @GetMapping("/all")
    public ResponseEntity<?> getAll(@RequestParam Boolean isActive) {
        return getResponseEntity(productCommandService.sendReadAsyncAwait(isActive, Duration.ofSeconds(5)));
    }

    @GetMapping("/{id}")
    public ResponseEntity<?> findById(@PathVariable("id") String id) {
        return getResponseEntity(productCommandService.sendReadByIdAsyncAwait(id, Duration.ofSeconds(5)));
    }
}
