package com.arturo.course.spring.kafka.products_api.handler;

import com.arturo.course.spring.kafka.products_api.messaging.ReplyInbox;
import com.arturo.course.spring.kafka.products_api.models.Reply;
import lombok.RequiredArgsConstructor;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.messaging.Message;

import java.util.function.Consumer;

@Configuration
@RequiredArgsConstructor
public class RepliesConsumer {
    private final ReplyInbox replyInbox;

    @Bean
    public Consumer<Message<Reply<?>>> handlerReplies() {
        return message -> {
            String correlationId = message.getHeaders().get("correlationId", String.class);
            replyInbox.complete(correlationId, message.getPayload());
        };
    }
}
