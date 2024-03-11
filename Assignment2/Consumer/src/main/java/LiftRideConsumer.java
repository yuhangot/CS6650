import com.rabbitmq.client.*;
import java.util.List;
import org.json.JSONObject;
import java.io.IOException;
import java.util.concurrent.*;

public class LiftRideConsumer {

  private final static String QUEUE_NAME = "liftRidesQueue";
  private static final int NUM_CONSUMERS = 10;
  private final static ConcurrentHashMap<String, ConcurrentHashMap<String, Integer>> skierRides = new ConcurrentHashMap<>();


  public static void main(String[] argv) throws Exception {
    ExecutorService executor = Executors.newFixedThreadPool(NUM_CONSUMERS);
    ConnectionFactory factory = new ConnectionFactory();
    factory.setHost("44.228.45.79");
    factory.setUsername("yuhan");
    factory.setPassword("yuhan");

    final Connection connection = factory.newConnection();
    MessageProcessor processor = new MessageProcessor(skierRides);

    for (int i = 0; i < NUM_CONSUMERS; i++) {
      executor.submit(() -> {
        try {
          final Channel channel = connection.createChannel();
          channel.queueDeclare(QUEUE_NAME, true, false, false, null);
          System.out.println(" [*] Waiting for messages. To exit press CTRL+C");

          DeliverCallback deliverCallback = (consumerTag, delivery) -> {
            String message = new String(delivery.getBody(), "UTF-8");
            try {
              processor.processMessage(message);
              channel.basicAck(delivery.getEnvelope().getDeliveryTag(), false);
            } catch (Exception e) {
              System.err.println("Failed to process message: " + message);
              channel.basicNack(delivery.getEnvelope().getDeliveryTag(), false,
                  true);
            }
          };

          channel.basicConsume(QUEUE_NAME, false, deliverCallback, consumerTag -> {
          });
        } catch (IOException e) {
          e.printStackTrace();
        }
      });
    }

//关闭钩子
    Runtime.getRuntime().addShutdownHook(new Thread(() -> {
      try {
        executor.shutdown();
        if (!executor.awaitTermination(800, TimeUnit.MILLISECONDS)) {
          System.err.println("Executor did not terminate in the specified time.");
          List<Runnable> droppedTasks = executor.shutdownNow();
          System.err.println("Executor was abruptly shut down. " + droppedTasks.size()
              + " tasks will not be executed.");
        }
        if (connection != null && connection.isOpen()) {
          connection.close();
        }
      } catch (IOException | InterruptedException e) {
        e.printStackTrace();
      }
    }));
  }
}


