package com.alan.example.azure.deviceclient;

import com.fasterxml.jackson.core.JsonProcessingException;
import lombok.Builder;
import lombok.Data;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.Random;
import java.util.concurrent.atomic.AtomicLong;

@RestController
public class ApiRest {

    private final AtomicLong idGen = new AtomicLong();
    private final String[] types = {"type1", "type2", "type3"};
    private final Random random = new Random();
    @Autowired
    private PublisherService publisherService;

    @GetMapping("/message")
    public String message() throws JsonProcessingException {
        for (int i = 0; i < 20; i++) {
            publisherService.sendEvent(MyMessage.builder().data("text")
                    .type(types[random.nextInt(types.length - 1)])
                    .ts(System.currentTimeMillis()).id(String.valueOf(idGen.getAndIncrement())).build());
        }
        return "OK";
    }
}

@Builder
@Data
class MyMessage {
    private String id;
    private String data;
    private String type;
    private Long ts;
}