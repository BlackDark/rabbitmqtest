import com.rabbitmq.client.*;

import java.io.IOException;
import java.util.Calendar;
import java.util.Date;
import java.util.concurrent.TimeoutException;

/**
 *
 */
public class Recv {
	private final static String QUEUE_NAME = "hello";
	private static int counter = 0;

	public static void main(String[] argv) throws IOException, InterruptedException, TimeoutException {
		ConnectionFactory factory = new ConnectionFactory();
		factory.setHost("ec2-54-175-246-160.compute-1.amazonaws.com");
		factory.setPort(5672);
		Connection connection = factory.newConnection();
		Channel channel = connection.createChannel();

		channel.queueDeclare(QUEUE_NAME, false, false, false, null);
		System.out.println(" [*] Waiting for messages. To exit press CTRL+C");

		Consumer consumer = new DefaultConsumer(channel) {
			@Override
			public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body)
					throws IOException {
				String message = new String(body, "UTF-8");
				if (message.equals("END")) {
					System.out.println("Received all messages - " + Send.sdf.format(new Date(System.currentTimeMillis())));
				}
			}
		};
		channel.basicConsume(QUEUE_NAME, true, consumer);
	}
}
