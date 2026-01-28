package io.homeassistant.devi.mqtt.service;

import org.eclipse.paho.client.mqttv3.*;
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence;

public class MQTTService {

    private String broker;
    private String commandsTopic = "devi/command/#"; // Subscribing topic
    private String username;
    private String password;
    private Mediator inputCommandMediator;
    private MqttClient client;
    private String statePublishPrefix =  "devi/state/%s/";

    private MqttConnectOptions options;

    public MQTTService(String broker, String port, String username, String password) {
        this.broker = "tcp://" + broker + ":" + port;
        this.username = username;
        this.password = password;
    }

    private InputCommand getInputCommand(String topicFullPath, String payload) {
        String[] parts = topicFullPath.split("/");
        int length = parts.length;

        if (length != 4) { // devi/command/10159384/sensor_heating_state
            throw new IllegalArgumentException("Input string does not have enough parts");
        }

        InputCommand inputCommand = new InputCommand();

        inputCommand.deviceId = parts[length - 2];
        inputCommand.sensorName = parts[length - 1];
        inputCommand.topicFullPath = topicFullPath;
        inputCommand.payload = payload;

        return inputCommand;
    }

    public void registerMediator(Mediator inputCommandMediator) {
        this.inputCommandMediator = inputCommandMediator;
    }

    public void start() {
        try {
            client = new MqttClient(broker, MqttClient.generateClientId(), new MemoryPersistence());
            options = new MqttConnectOptions();
            options.setCleanSession(true);
            options.setAutomaticReconnect(true);
            options.setUserName(username);
            options.setPassword(password.toCharArray());

            // Set a connection lost callback
            client.setCallback(new MqttCallbackWithCommand());

            // Initial connection
            connect();

        } catch (MqttException e) {
            e.printStackTrace();
        }
    }

    public void SendSensorData(String sensorId, String sensorName, String sensorValue, boolean setRetain) {

        if (client.isConnected()) {
            try {
                String topic = String.format(statePublishPrefix, sensorId) + sensorName;
                MqttMessage message = new MqttMessage(sensorValue.getBytes());
                message.setQos(1);
				if (setRetain)
					message.setRetained(true);

                // Publish message
                client.publish(topic, message);
                System.out.println("Message published to topic " + topic + ": " + sensorValue);
            } catch (MqttException e) {
                e.printStackTrace();
            }
        }
    }

    public void SendDiscoveryMessage(String topic, String payload) {

        if (client.isConnected()) {
            try {
                MqttMessage message = new MqttMessage(payload.getBytes());
                message.setQos(1);
                message.setRetained(true);

                // Publish message
                client.publish(topic, message);
            } catch (MqttException e) {
                e.printStackTrace();
            }
        }
    }

    private void connect() {
        final int maxAttempts = 12;
        int attempts = 0;
        while (attempts < maxAttempts) {
            try {
                client.connect(options);
                System.out.println("Connected to broker");
                return;
            } catch (MqttException e) {
                e.printStackTrace();
                attempts++;
                // Retry connection after a delay
                try {
                    Thread.sleep(5000);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    return;
                }
            }
        }
        System.out.println("Failed to connect to broker after " + maxAttempts + " attempts");
    }

    private void subscribeCommands() {
        if (client == null || !client.isConnected()) {
            return;
        }
        try {
            client.subscribe(commandsTopic);
        } catch (MqttException e) {
            e.printStackTrace();
        }
    }

    public class MqttCallbackWithCommand implements MqttCallbackExtended {
        @Override
        public void connectComplete(boolean reconnect, String serverURI) {
            subscribeCommands();
        }

        @Override
        public void connectionLost(Throwable cause) {
            System.out.println("Connection lost. Reconnecting...");
        }

        @Override
        public void messageArrived(String topic, MqttMessage message) {
            String payload = new String(message.getPayload());

            InputCommand inputCmd = getInputCommand(topic, payload);

            System.out.println(inputCmd.toString());

            try {
                if(inputCommandMediator != null)
                    inputCommandMediator.notify(this, inputCmd);
            } catch (Exception e) {
                e.printStackTrace();
            }

            //System.out.println("Message received on topic " + topic + ": " + payload);
        }

        @Override
        public void deliveryComplete(IMqttDeliveryToken token) {
            // No action needed for this example
        }
    }
}
