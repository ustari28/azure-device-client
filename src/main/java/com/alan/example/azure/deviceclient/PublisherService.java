package com.alan.example.azure.deviceclient;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.microsoft.azure.sdk.iot.device.*;
import com.microsoft.azure.sdk.iot.device.transport.IotHubConnectionStatus;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.net.URISyntaxException;

@Service
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
            System.out.println("IoT Hub responded to message " + context.toString() + " with status " + status.name());

            if (status == IotHubStatusCode.MESSAGE_CANCELLED_ONCLOSE) {
                System.err.println(context);
            }
        }
    }

    protected static class IotHubConnectionStatusChangeCallbackLogger implements IotHubConnectionStatusChangeCallback {
        @Override
        public void execute(IotHubConnectionStatus status, IotHubConnectionStatusChangeReason statusChangeReason, Throwable throwable, Object callbackContext) {
            System.out.println();
            System.out.println("CONNECTION STATUS UPDATE: " + status);
            System.out.println("CONNECTION STATUS REASON: " + statusChangeReason);
            System.out.println("CONNECTION STATUS THROWABLE: " + (throwable == null ? "null" : throwable.getMessage()));
            System.out.println();

            if (throwable != null) {
                throwable.printStackTrace();
            }

            if (status == IotHubConnectionStatus.DISCONNECTED) {
                System.out.println("The connection was lost, and is not being re-established." +
                        " Look at provided exception for how to resolve this issue." +
                        " Cannot send messages until this issue is resolved, and you manually re-open the device client");
            } else if (status == IotHubConnectionStatus.DISCONNECTED_RETRYING) {
                System.out.println("The connection was lost, but is being re-established." +
                        " Can still send messages, but they won't be sent until the connection is re-established");
            } else if (status == IotHubConnectionStatus.CONNECTED) {
                System.out.println("The connection was successfully established. Can send messages.");
            }
        }
    }
}


