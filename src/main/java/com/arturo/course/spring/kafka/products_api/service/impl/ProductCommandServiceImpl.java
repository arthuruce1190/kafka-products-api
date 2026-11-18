package com.arturo.course.spring.kafka.products_api.service.impl;


import com.arturo.course.spring.kafka.products_api.messaging.ReplyInbox;
import com.arturo.course.spring.kafka.products_api.models.Command;
import com.arturo.course.spring.kafka.products_api.models.Reply;
import com.arturo.course.spring.kafka.products_api.models.dto.ProductDto;
import com.arturo.course.spring.kafka.products_api.service.ProductCommandService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.cloud.stream.function.StreamBridge;
import org.springframework.messaging.Message;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.stereotype.Service;

import java.time.Duration;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

@Service
@Slf4j
@RequiredArgsConstructor
public class ProductCommandServiceImpl implements ProductCommandService {

    private final StreamBridge bridge;
    private final ReplyInbox replyInbox;

    @Override
    public Reply<?> sendCreateAsyncAwait(final ProductDto productDto, final Duration timeout) {

        Command<ProductDto> cmd = new Command<>("CREATE", null, productDto);
        String correlationId = UUID.randomUUID().toString();
        log.info("Api Products Client Creating product with correlationId: {}", correlationId);
        CompletableFuture<Reply<?>> future = replyInbox.register(correlationId);
        Message<Command<ProductDto>> msg = MessageBuilder
            .withPayload(cmd)
            .setHeader("correlationId", correlationId)
            .build();

        boolean sent = bridge.send("commands-out-0", msg);

        if (!sent) {
            throw new IllegalStateException("Can't send command to Kafka");
        }

        try {
            return future.get(timeout.toMillis(), TimeUnit.MILLISECONDS);
        } catch (InterruptedException | ExecutionException | TimeoutException e) {
            throw new RuntimeException(
                "Timeout esperando respuesta de products-commands desde kafka", e.getCause());
        }
    }
}
