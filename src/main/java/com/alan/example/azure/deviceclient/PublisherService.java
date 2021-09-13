package com.alan.example.azure.deviceclient;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.microsoft.azure.sdk.iot.device.*;
import com.microsoft.azure.sdk.iot.device.transport.IotHubConnectionStatus;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.net.URISyntaxException;

//@Service
@Slf4j
public class PublisherService {

    private final DeviceClient deviceClient;
    private final Long time = 2400L;
    @Autowired
    private ObjectMapper mapper;

    public PublisherService(@Value("${app.connection-string}") String connectionString,
                            @Value("${app.path-to-certificate}") String pathToCertificate) throws URISyntaxException, IOException {
        this.deviceClient = new DeviceClient(connectionString, IotHubClientProtocol.MQTT);
        this.deviceClient.setOption("SetCertificatePath", pathToCertificate);
        this.deviceClient.setOption("SetSASTokenExpiryTime", time);
        this.deviceClient.registerConnectionStatusChangeCallback(new IotHubConnectionStatusChangeCallbackLogger(), new Object());
        this.deviceClient.setMessageCallback(new AppMessageCallback(), null);
        this.deviceClient.open();
    }

    public void sendEvent(MyMessage body) {
        try {
            Message message = new Message(mapper.writeValueAsBytes(body));
            message.setContentEncoding("utf-8");
            message.setContentTypeFinal("application/json");
            message.setMessageId(body.getId());
            message.setConnectionDeviceId("simulator-udp-01");
            deviceClient.sendEventAsync(message, new EventCallback(), new Object());
        } catch (JsonProcessingException e) {
            e.printStackTrace();
        }
    }

    protected static class EventCallback implements IotHubEventCallback {
        public void execute(IotHubStatusCode status, Object context) {
           log.info("IoT Hub responded to message {}  with status {}", context.toString(), status.name());
            if (status == IotHubStatusCode.MESSAGE_CANCELLED_ONCLOSE) {
                System.err.println(context);
            }
        }
    }

    static class AppMessageCallback implements MessageCallback {
        public IotHubMessageResult execute(Message msg, Object context) {
            log.info("Received message from hub: {}",
                    new String(msg.getBytes(), Message.DEFAULT_IOTHUB_MESSAGE_CHARSET));
            return IotHubMessageResult.COMPLETE;
        }
    }

    protected static class IotHubConnectionStatusChangeCallbackLogger implements IotHubConnectionStatusChangeCallback {
        @Override
        public void execute(IotHubConnectionStatus status, IotHubConnectionStatusChangeReason statusChangeReason, Throwable throwable, Object callbackContext) {
            log.info("------------------------------------------");
            log.info("CONNECTION STATUS UPDATE: {}", status);
            log.info("CONNECTION STATUS REASON: {}", statusChangeReason);
            log.info("CONNECTION STATUS THROWABLE: {}", (throwable == null ? "null" : throwable.getMessage()));
            log.info("-------------------------------------------");

            if (throwable != null) {
                throwable.printStackTrace();
            }

            if (status == IotHubConnectionStatus.DISCONNECTED) {
                log.info("The connection was lost, and is not being re-established." +
                        " Look at provided exception for how to resolve this issue." +
                        " Cannot send messages until this issue is resolved, and you manually re-open the device client");
            } else if (status == IotHubConnectionStatus.DISCONNECTED_RETRYING) {
                log.info("The connection was lost, but is being re-established." +
                        " Can still send messages, but they won't be sent until the connection is re-established");
            } else if (status == IotHubConnectionStatus.CONNECTED) {
                log.info("The connection was successfully established. Can send messages.");
            }
        }
    }
}


