import com.rabbitmq.client.*;
import org.dom4j.*;
import org.dom4j.io.SAXReader;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.concurrent.TimeoutException;

public class CalculateMeanTemperatur {
	private final static String QUEUE_FROM = "temperatures";
	private final static String QUEUE_TO = "mean_temperature";
	private final static String HOST_FROM = "ec2-54-175-246-160.compute-1.amazonaws.com";
	private final static String HOST_TO = "ec2-54-175-246-160.compute-1.amazonaws.com";

	public static void main(String[] argv) throws IOException, TimeoutException {
		new CalculateMeanTemperatur().startConsume();
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
		Node temp1 = document.selectSingleNode("//temperature1");
		Node temp2 = document.selectSingleNode("//temperature2");

		if (temp1 == null || temp2 == null || !temp1.hasContent() || !temp2.hasContent()) {
			// No content
			return;
		}

		int temperatureValue1 = Integer.parseInt(temp1.getText());
		int temperatureValue2 = Integer.parseInt(temp2.getText());

		Document document1 = createDocument();
		document1.addElement("temperature").addText("" + (temperatureValue1 + temperatureValue2) / 2);

		if (true == true) {
			System.out.println(document1.asXML());
			return;
		}

		try {
			startPublish(document1);
		} catch (IOException e) {
			e.printStackTrace();
		} catch (TimeoutException e) {
			e.printStackTrace();
		}
	}

	protected void startPublish(Document document) throws IOException, TimeoutException {
		ConnectionFactory factory = new ConnectionFactory();
		factory.setHost(HOST_TO);
		factory.setPort(5672);
		Connection connection = factory.newConnection();
		Channel channel = connection.createChannel();

		channel.queueDeclare(QUEUE_TO, false, false, false, null);

		channel.basicPublish("", QUEUE_TO, null, document.asXML().getBytes("UTF-8"));

		channel.close();
		connection.close();
	}

	protected Document parseBytes(byte[] bytes) {
		SAXReader reader = new SAXReader();
		Document doc = null;
		try {
			doc = reader.read(new ByteArrayInputStream(bytes));
		} catch (DocumentException e) {
			e.printStackTrace();
		}

		return doc;
	}

	protected Document createDocument() {
		Document document = DocumentHelper.createDocument();
		return document;
	}
}
