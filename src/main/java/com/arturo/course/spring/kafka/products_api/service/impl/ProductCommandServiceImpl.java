package com.arturo.course.spring.kafka.products_api.service.impl;


import com.arturo.course.spring.kafka.products_api.models.Command;
import com.arturo.course.spring.kafka.products_api.models.dto.ProductDto;
import com.arturo.course.spring.kafka.products_api.service.ProductCommandService;
import lombok.RequiredArgsConstructor;
import org.springframework.cloud.stream.function.StreamBridge;
import org.springframework.messaging.Message;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.stereotype.Service;

import java.util.UUID;

@Service
@RequiredArgsConstructor
public class ProductCommandServiceImpl implements ProductCommandService {

    private final StreamBridge bridge;

    @Override
    public void sendCreate(ProductDto productDto) {

        Command<ProductDto> cmd = new Command<>("CREATE", null, productDto);
        String correlationId = UUID.randomUUID().toString();
        Message<Command<ProductDto>> msg = MessageBuilder
            .withPayload(cmd)
            .setHeader("correlationId", correlationId)
            .build();

        boolean sent = bridge.send("commands-out-0", msg);

        if (!sent) {
            throw new IllegalStateException("Can't send command to Kafka");
        }
    }
}
