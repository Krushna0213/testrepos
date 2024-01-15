package uscrn.acquisition.cmd;

import static java.nio.charset.StandardCharsets.UTF_8;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.Arrays;
import java.util.List;
import java.io.File;

import javax.jms.BytesMessage;
import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.Queue;
import javax.jms.Session;
import javax.jms.TextMessage;

import org.apache.activemq.ActiveMQConnectionFactory;

import uscrn.acquisition.routing.MessageProperties;

/**
 * A utility to connect to a queue and extract all of its messages.
 * The act of dumping WILL remove the messages from the queue. A
 * message corresponds to one file in the output directory.
 *
 * Requires three arguments:
 *   1.  The URI of the broker.
 *   2.  The name of the queue.
 *   3.  The directory to dump messages into.
 */
public class Dump {

	public static void main(String[] args) throws Exception {
		List<String> options = Arrays.asList(args);

		if(options.isEmpty() || options.contains("--help")) {
			System.out.println(
					"A utility to connect to a queue and extract all of its messages.\n" +
							"The act of dumping WILL remove the messages from the queue. A \n" +
							"message corresponds to one file in the output directory.\n" +
							"\n" +
							"Requires three arguments:\n" +
							"  1.  The URI of the broker.\n" +
							"  2.  The name of the queue.\n" +
							"  3.  The directory to dump messages into.");
			return;
		}

		String brokerUri = options.get(0);
		String queueName = options.get(1);
		Path outputDirectory = Paths.get(options.get(2));

		System.out.println("Output Directory=" + outputDirectory);

		Files.createDirectories(outputDirectory);

		ConnectionFactory factory = new ActiveMQConnectionFactory(brokerUri);
		Connection connection = factory.createConnection();

		try {
			connection.start();

			Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
			Queue queue = session.createQueue(queueName);
			MessageConsumer consumer = session.createConsumer(queue);

			Message message = null;
			while ((message = consumer.receive(1000)) != null) {
				String dataSource = message.getStringProperty(MessageProperties.DATA_SOURCE);
				String fileName = message.getStringProperty(MessageProperties.FILE_NAME);

				if (dataSource != null && fileName != null) {
					System.out.println(dataSource + ": " + fileName);

					Path sourceDirectory = outputDirectory.resolve(dataSource);
					if (!Files.exists(sourceDirectory)) {
						Files.createDirectory(sourceDirectory);
					}


					String normalizedFileName = Paths.get(fileName).normalize().toString();

					File file = new File(sourceDirectory.toString(), normalizedFileName);

					if (file.getCanonicalPath().startsWith(sourceDirectory.toString())) {
						dump(message, file.toPath());
					} else {
					    throw new SecurityException("Invalid Directory Invocation" + sourceDirectory.toString());
					}
				}
				message.acknowledge();
			}
		} finally {
			connection.close();
		}
	}

	static final void dump(Message message, Path path) throws IOException, JMSException {
		Files.copy(stream(message), path, StandardCopyOption.REPLACE_EXISTING);
	}

	private static final InputStream stream(Message message) throws JMSException {
		if(message instanceof TextMessage) {
			byte[] bytes = ((TextMessage)message).getText().getBytes(UTF_8);
			return new ByteArrayInputStream(bytes);
		}else if(message instanceof BytesMessage) {
			BytesMessage bytes = (BytesMessage)message;
			return new InputStream() {
				long remaining = bytes.getBodyLength();
				@Override
				public int read() throws IOException {
					if(remaining > 0) {
						remaining--;
						try {
							return bytes.readUnsignedByte();
						} catch (JMSException e) {
							throw new IOException(e);
						}
					}
					return -1;
				}
			};
		}
		return null;
	}
}
