package com.arturo.course.spring.kafka.products_api.messaging;

import com.arturo.course.spring.kafka.products_api.models.Reply;
import org.springframework.stereotype.Component;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;

@Component
public class ReplyInbox {
    private final ConcurrentHashMap<String, CompletableFuture<Reply<?>>> pending = new ConcurrentHashMap<>();

    public CompletableFuture<Reply<?>> register(String id) {
        CompletableFuture<Reply<?>> future = new CompletableFuture<>();
        pending.put(id, future);

        return future;
    }

    public void complete(String correlationId, Reply<?> reply) {
        if (correlationId == null) {
            throw new NullPointerException("correlationId no puede ser nulo");
        }
        CompletableFuture<Reply<?>> future = pending.remove(correlationId);
        if (future != null) {
            future.complete(reply);
        }
    }
}
