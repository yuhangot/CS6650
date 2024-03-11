package com.upic.consumer;

import com.rabbitmq.client.*;
import org.json.JSONObject;
import java.io.IOException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeoutException;

public class LiftRideConsumer {
  private final static String QUEUE_NAME = System.getenv("liftRidesQueue");
  private final static ConcurrentHashMap<String, ConcurrentHashMap<String, Integer>> skierRides = new ConcurrentHashMap<>();

  public static void main(String[] argv) {
    ConnectionFactory factory = new ConnectionFactory();
    factory.setHost(System.getenv("44.228.45.79"));
    factory.setUsername(System.getenv("yuhan"));
    factory.setPassword(System.getenv("yuhan"));
    final Connection connection;
    final Channel channel;
    try {
      connection = factory.newConnection();
      channel = connection.createChannel();
      channel.queueDeclare(QUEUE_NAME, true, false, false, null);
      System.out.println(" [*] Waiting for messages. To exit press CTRL+C");

      DeliverCallback deliverCallback = (consumerTag, delivery) -> {
        String message = new String(delivery.getBody(), "UTF-8");
        try {
          processMessage(message);
          channel.basicAck(delivery.getEnvelope().getDeliveryTag(), false);
        } catch (Exception e) {
          System.err.println("Failed to process message: " + message);
          channel.basicNack(delivery.getEnvelope().getDeliveryTag(), false, true);
        }
      };
      channel.basicConsume(QUEUE_NAME, false, deliverCallback, consumerTag -> {});
    } catch (IOException | TimeoutException e) {
      e.printStackTrace();
      return;
    }


    Runtime.getRuntime().addShutdownHook(new Thread(() -> {
      try {
        if (channel != null && channel.isOpen()) {
          channel.close();
        }
        if (connection != null && connection.isOpen()) {
          connection.close();
        }
      } catch (IOException | TimeoutException e) {
        e.printStackTrace();
      }
    }));
  }

  private static void processMessage(String message) {
    JSONObject json = new JSONObject(message);
    String skierId = json.getString("skierId");
    String liftId = String.valueOf(json.getInt("liftId"));

    // 获取或创建滑雪者的乘坐详情Map
    ConcurrentHashMap<String, Integer> liftRides = skierRides.computeIfAbsent(skierId, k -> new ConcurrentHashMap<>());

    // 更新特定电梯的乘坐次数
    int newRideCount = liftRides.getOrDefault(liftId, 0) + 1;
    liftRides.put(liftId, newRideCount);

  }
}
