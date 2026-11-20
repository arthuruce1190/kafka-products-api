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

@RestController
@RequiredArgsConstructor
@RequestMapping("/api/products")
public class ProductController {
    private final ProductCommandService productCommandService;

    private static @NonNull ResponseEntity<?> getResponseEntity(Reply<?> reply) {
        if ("OK".equalsIgnoreCase(reply.statusMessage())) {
            return ResponseEntity.ok(reply.body());
        }
        return ResponseEntity.badRequest().body(Map.of("error", reply.message()));
    }

    @PostMapping
    public ResponseEntity<?> create(@Valid @RequestBody ProductDto product) {
        Reply<?> reply = productCommandService.sendCreateAsyncAwait(product, Duration.ofSeconds(5));
        return getResponseEntity(reply);
    }

    @PutMapping
    public ResponseEntity<?> update(@Valid @RequestBody ProductDto product) {
        Reply<?> reply = productCommandService.sendUpdateAsyncAwait(product, Duration.ofSeconds(5));
        return getResponseEntity(reply);
    }

    @DeleteMapping
    public ResponseEntity<?> delete(@RequestBody ProductDto product) {
        Reply<?> reply = productCommandService.sendDeleteAsyncAwait(product, Duration.ofSeconds(5));
        return getResponseEntity(reply);
    }

    @GetMapping("/all")
    public ResponseEntity<?> getAll(@RequestParam Boolean isActive) {
        Reply<?> reply = productCommandService.sendReadAsyncAwait(isActive, Duration.ofSeconds(5));
        return getResponseEntity(reply);
    }

    @GetMapping("/{id}")
    public ResponseEntity<?> findById(@PathVariable("id") String id) {
        Reply<?> reply = productCommandService.sendReadByIdAsyncAwait(id, Duration.ofSeconds(5));
        return getResponseEntity(reply);
    }
}
