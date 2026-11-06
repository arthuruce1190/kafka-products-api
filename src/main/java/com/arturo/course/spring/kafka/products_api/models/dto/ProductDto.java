package com.arturo.course.spring.kafka.products_api.models.dto;

import jakarta.validation.constraints.DecimalMin;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;

public record ProductDto(@NotBlank String name, @NotNull @DecimalMin(value = "0.1") Double price) {
}
