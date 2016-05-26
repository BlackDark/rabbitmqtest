import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import org.dom4j.Document;
import org.dom4j.DocumentHelper;
import org.dom4j.Element;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.concurrent.TimeoutException;

public class Send {
	private final static String QUEUE_TO = "temperatures";
	private static final int numMessage = 100000;

	public static SimpleDateFormat sdf = new SimpleDateFormat("MMM dd,yyyy HH:mm");

	public static void main(String[] argv) throws IOException, TimeoutException {
		new Send().testing();
	}

	protected void working() throws IOException, TimeoutException {
		ConnectionFactory factory = new ConnectionFactory();
		factory.setHost("ec2-54-175-246-160.compute-1.amazonaws.com");
		factory.setPort(5672);
		Connection connection = factory.newConnection();
		Channel channel = connection.createChannel();

		channel.queueDeclare(QUEUE_TO, false, false, false, null);
		String message = "Hello World from RabbitMQ on AWS :) !";


		System.out.println("Start sending " + numMessage + " messages - " + sdf.format(new Date(System.currentTimeMillis())));
		for (int i = 0; i < numMessage; i++) {
			channel.basicPublish("", QUEUE_TO, null, message.getBytes());
		}

		String endMessage = "END";
		channel.basicPublish("", QUEUE_TO, null, endMessage.getBytes());


		System.out.println("Finished sending " + numMessage + " messages - " + sdf.format(new Date(System.currentTimeMillis())));

		channel.close();
		connection.close();
	}

	protected void testing() throws IOException, TimeoutException {
		ConnectionFactory factory = new ConnectionFactory();
		factory.setHost("ec2-54-175-246-160.compute-1.amazonaws.com");
		factory.setPort(5672);
		Connection connection = factory.newConnection();
		Channel channel = connection.createChannel();

		channel.queueDeclare(QUEUE_TO, false, false, false, null);

		channel.basicPublish("", QUEUE_TO, null, createDocument().asXML().getBytes());

		channel.close();
		connection.close();
	}


	protected Document createDocument() {
		Document document = DocumentHelper.createDocument();
		Element root = document.addElement("achse");

		Element author1 = root.addElement("temperature1")
				.addText("40");

		Element author2 = root.addElement("temperature2")
				.addText("70");

		return document;
	}
}
