package com.alan.example.azure.deviceclient;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.microsoft.azure.sdk.iot.service.IotHubServiceClientProtocol;
import com.microsoft.azure.sdk.iot.service.Message;
import com.microsoft.azure.sdk.iot.service.ServiceClient;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.util.concurrent.atomic.AtomicLong;

@Service
@Slf4j
public class ServiceCommandClient {

    private final ServiceClient serviceClient;
    private final AtomicLong idGen = new AtomicLong();

    private final ObjectMapper mapper;

    public ServiceCommandClient(@Value("${app.service.connection-string}") String connectionString,
                                @Autowired ObjectMapper mapper) {
        this.mapper = mapper;
        this.serviceClient = new ServiceClient(connectionString, IotHubServiceClientProtocol.AMQPS);
    }

    public void sendMessage2Device(String type, String deviceId) {

        try {
            Message message = new Message(mapper.writeValueAsBytes(MyMessage.builder().deviceId(deviceId).data("SERVICE")
                    .id(String.valueOf(idGen.getAndIncrement()))
                    .type(type)
                    .ts(System.currentTimeMillis()).build()));
            this.serviceClient.sendAsync(deviceId, message);
        } catch (JsonProcessingException e) {
            e.printStackTrace();
        }
    }
}
