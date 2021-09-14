package com.alan.example.azure.deviceclient;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.microsoft.azure.sdk.iot.device.IotHubEventCallback;
import com.microsoft.azure.sdk.iot.device.IotHubStatusCode;
import com.microsoft.azure.sdk.iot.device.Message;
import lombok.Builder;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RestController;

import java.util.concurrent.atomic.AtomicLong;

@RestController
@Slf4j
public class ApiRest {

    private final AtomicLong idGen = new AtomicLong();
    @Autowired
    private ObjectMapper mapper;
    @Autowired
    private DeviceProvisioningService deviceProvisioningService;
    @Autowired
    private ServiceCommandClient serviceCommandClient;

    @GetMapping("/{type}/{deviceId}/message")
    public String message(@PathVariable("type") String type,
                          @PathVariable("deviceId") String deviceId) throws JsonProcessingException {
        Message message = new Message(mapper.writeValueAsBytes(MyMessage.builder()
                .data("DPS")
                .id(String.valueOf(idGen.getAndIncrement()))
                .deviceId(deviceId)
                .type(type)
                .ts(System.currentTimeMillis()).build()));
        message.setContentTypeFinal("application/json");
        deviceProvisioningService.getDeviceClient(deviceId)
                .sendEventAsync(message, new EventCallback(), "context");
        return "OK";
    }

    @GetMapping("/{type}/{deviceId}/c2d")
    public String cloudToDevice(@PathVariable("type") String type,
                                @PathVariable("deviceId") String deviceId) {
        serviceCommandClient.sendMessage2Device(type, deviceId);
        return "OK";
    }

    protected static class EventCallback implements IotHubEventCallback {
        public void execute(IotHubStatusCode status, Object context) {
            log.info("IoT Hub responded to message {}  with status {}", context.toString(), status.name());
            if (status == IotHubStatusCode.MESSAGE_CANCELLED_ONCLOSE) {
                log.error("Received context  {}", context);
            }
        }
    }
}

@Builder
@Data
class MyMessage {
    private String id;
    private String data;
    private String type;
    private Long ts;
    private String deviceId;
}

