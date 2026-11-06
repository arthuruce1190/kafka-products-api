package com.arturo.course.spring.kafka.products_api.service.impl;


import com.arturo.course.spring.kafka.products_api.models.Command;
import com.arturo.course.spring.kafka.products_api.models.dto.ProductDto;
import com.arturo.course.spring.kafka.products_api.service.ProductCommandService;
import lombok.RequiredArgsConstructor;
import org.springframework.cloud.stream.function.StreamBridge;
import org.springframework.stereotype.Service;

@Service
@RequiredArgsConstructor
public class ProductCommandServiceImpl implements ProductCommandService {

    private final StreamBridge bridge;

    @Override
    public void sendCreate(ProductDto productDto) {

        Command<ProductDto> cmd = new Command<>("CREATE", null, productDto);
        boolean sent = bridge.send("commands-out-0", cmd);

        if (!sent) {
            throw new IllegalStateException("Can't send command to Kafka");
        }
    }
}
