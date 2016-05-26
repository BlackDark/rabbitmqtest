import com.rabbitmq.client.*;
import org.dom4j.*;
import org.dom4j.io.SAXReader;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.concurrent.TimeoutException;

public class CheckTemperature {
	private final static String QUEUE_FROM = "mean_temperature";
	private final static String HOST_FROM = "ec2-54-175-246-160.compute-1.amazonaws.com";

	public static void main(String[] argv) throws IOException, TimeoutException {
		new CheckTemperature().startConsume();
	}

	protected void startConsume() throws IOException, TimeoutException {
		ConnectionFactory factory = new ConnectionFactory();
		factory.setHost(HOST_FROM);
		factory.setPort(5672);
		Connection connection = factory.newConnection();
		Channel channel = connection.createChannel();

		channel.queueDeclare(QUEUE_FROM, false, false, false, null);

		Consumer consumer = new DefaultConsumer(channel) {
			@Override
			public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body)
					throws IOException {
				Document document = parseBytes(body);

				if (document == null) {
					// Do not continue and stop.
					return;
				}

				processMessage(document);
			}
		};

		channel.basicConsume(QUEUE_FROM, true, consumer);
	}

	private void processMessage(Document document) {
		Node node = document.selectSingleNode("//temperature");

		if (!node.hasContent()) {
			// No content
		}

		int temperatureValue = Integer.parseInt(node.getText());

		if (temperatureValue > 90) {
			System.out.println("Attention: Temperature exceeded value of 90. Current temperature: " + temperatureValue);
		} else {
			System.out.println("Current temperature: " + temperatureValue);
		}
	}

	public Document parseBytes(byte[] bytes) {
		SAXReader reader = new SAXReader();
		Document doc = null;
		try {
			doc = reader.read(new ByteArrayInputStream(bytes));
		} catch (DocumentException e) {
			e.printStackTrace();
		}

		return doc;
	}
}
