package com.alan.example.azure.deviceclient;

import com.microsoft.azure.sdk.iot.device.*;
import com.microsoft.azure.sdk.iot.provisioning.device.*;
import com.microsoft.azure.sdk.iot.provisioning.device.internal.exceptions.ProvisioningDeviceClientException;
import com.microsoft.azure.sdk.iot.provisioning.security.SecurityProviderSymmetricKey;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;

@Service
@Slf4j
public class DeviceProvisioningService {
    private static final int MAX_TIME_TO_WAIT_FOR_REGISTRATION = 10000; // in milliseconds
    SecurityProviderSymmetricKey securityClientSymmetricKey;

    ProvisioningDeviceClient provisioningDeviceClient;
    Map<String, DeviceClient> deviceClientPool = new LinkedHashMap<>();
    private final String scopeId;
    private final String globalEndpoint;
    private final String symmetricKey;
    private final String registrationId;

    public DeviceProvisioningService(@Value("${app.scope-id}")
                                             String scopeId,
                                     @Value("${app.global-endpoint}")
                                             String globalEndpoint,
                                     @Value("${app.symmetric-key}")
                                             String symmetricKey,
                                     @Value("${app.registration-id}")
                                             String registrationId) {
        this.scopeId = scopeId;
        this.globalEndpoint = globalEndpoint;
        this.symmetricKey = symmetricKey;
        this.registrationId = registrationId;
    }

    public DeviceClient getDeviceClient(String deviceId) {
        return Optional.ofNullable(deviceClientPool.get(deviceId)).orElseGet(() ->createDeviceClient(deviceId));
    }

    private DeviceClient createDeviceClient(String deviceId) {
        DeviceClient deviceClient = null;
        try {

            log.info("Registering new device {}", deviceId);
            ProvisioningStatus provisioningStatus = new ProvisioningStatus();
            byte[] derivedSymmetricKey =
                    SecurityProviderSymmetricKey
                            .ComputeDerivedSymmetricKey(
                                    symmetricKey.getBytes(StandardCharsets.UTF_8),
                                    deviceId);

            securityClientSymmetricKey = new SecurityProviderSymmetricKey(derivedSymmetricKey, deviceId);
            provisioningDeviceClient = ProvisioningDeviceClient.create(globalEndpoint, scopeId,
                    ProvisioningDeviceClientTransportProtocol.HTTPS, securityClientSymmetricKey);
            provisioningDeviceClient.registerDevice(new ProvisioningDeviceClientRegistrationCallbackImpl(), provisioningStatus);
            while (provisioningStatus.provisioningDeviceClientRegistrationInfoClient.getProvisioningDeviceClientStatus() != ProvisioningDeviceClientStatus.PROVISIONING_DEVICE_STATUS_ASSIGNED) {
                if (provisioningStatus.provisioningDeviceClientRegistrationInfoClient.getProvisioningDeviceClientStatus() == ProvisioningDeviceClientStatus.PROVISIONING_DEVICE_STATUS_ERROR ||
                        provisioningStatus.provisioningDeviceClientRegistrationInfoClient.getProvisioningDeviceClientStatus() == ProvisioningDeviceClientStatus.PROVISIONING_DEVICE_STATUS_DISABLED ||
                        provisioningStatus.provisioningDeviceClientRegistrationInfoClient.getProvisioningDeviceClientStatus() == ProvisioningDeviceClientStatus.PROVISIONING_DEVICE_STATUS_FAILED) {
                    provisioningStatus.exception.printStackTrace();
                    System.out.println("Registration error, bailing out");
                    break;
                }
                System.out.println("Waiting for Provisioning Service to register");
                Thread.sleep(MAX_TIME_TO_WAIT_FOR_REGISTRATION);
            }

            if (provisioningStatus.provisioningDeviceClientRegistrationInfoClient.getProvisioningDeviceClientStatus() == ProvisioningDeviceClientStatus.PROVISIONING_DEVICE_STATUS_ASSIGNED) {
                System.out.println("IotHUb Uri : " + provisioningStatus.provisioningDeviceClientRegistrationInfoClient.getIothubUri());
                System.out.println("Device ID : " + provisioningStatus.provisioningDeviceClientRegistrationInfoClient.getDeviceId());

                // connect to iothub
                String iotHubUri = provisioningStatus.provisioningDeviceClientRegistrationInfoClient.getIothubUri();
                String receivedDeviceId = provisioningStatus.provisioningDeviceClientRegistrationInfoClient.getDeviceId();
                log.info("Device registered {}", receivedDeviceId);
                deviceClient = DeviceClient.createFromSecurityProvider(iotHubUri, deviceId, securityClientSymmetricKey, IotHubClientProtocol.MQTT);
                deviceClient.setMessageCallback(new AppMessageCallback(), null);
                deviceClient.open();
                deviceClientPool.put(deviceId, deviceClient);
//                Message messageToSendFromDeviceToHub = new Message("Whatever message you would like to send");
//
//                System.out.println("Sending message from device to IoT Hub...");
//                deviceClient.sendEventAsync(messageToSendFromDeviceToHub, new IotHubEventCallbackImpl(), null);
//                log.info("Waiting to send message");
//                Thread.sleep(5000);
//                deviceClient.closeNow();
            } else {
                log.error("We can't register the deviceId {}", deviceId);
            }

        } catch (IOException e) {
            System.out.println("Device client threw an exception: " + e.getMessage());
        } catch (URISyntaxException e) {
            e.printStackTrace();
        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
        } catch (InvalidKeyException e) {
            e.printStackTrace();
        } catch (ProvisioningDeviceClientException e) {
            e.printStackTrace();

        } catch (InterruptedException e) {
            e.printStackTrace();
        } finally {
            provisioningDeviceClient.closeNow();
        }
        return deviceClient;
    }

    static class ProvisioningStatus {
        ProvisioningDeviceClientRegistrationResult provisioningDeviceClientRegistrationInfoClient = new ProvisioningDeviceClientRegistrationResult();
        Exception exception;
    }

    static class ProvisioningDeviceClientRegistrationCallbackImpl implements ProvisioningDeviceClientRegistrationCallback {
        @Override
        public void run(ProvisioningDeviceClientRegistrationResult provisioningDeviceClientRegistrationResult, Exception exception, Object context) {
            if (context instanceof ProvisioningStatus) {
                ProvisioningStatus status = (ProvisioningStatus) context;
                status.provisioningDeviceClientRegistrationInfoClient = provisioningDeviceClientRegistrationResult;
                status.exception = exception;
            } else {
                System.out.println("Received unknown context");
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
}
