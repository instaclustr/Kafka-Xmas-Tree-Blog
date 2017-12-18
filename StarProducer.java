package kafkaxmas;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

// Producer which drops balls onto the top of the tree.
// Drop one "ball" onto top of tree every DELAY period, stops when number of balls dropped == BALLS.
// Message is of format: key=row, value=col
// Changed to send (-1, -1) rather than the original (0, 0), to indicate that the ball needs to be processed specially by Twinkle.

public class StarProducer extends Thread {
	private final KafkaProducer<Integer, Integer> producer;
    private final String topic;
    private final Boolean isAsync;
    private final Boolean debug = KafkaProperties.DEBUG;
    private final long zeroTime = System.currentTimeMillis();

    public StarProducer(String topic, Boolean isAsync) {
        Properties props = new Properties();        
        props.put("bootstrap.servers", KafkaProperties.KAFKA_SERVER_URL + ":" + KafkaProperties.KAFKA_SERVER_PORT);
        props.put("client.id", "StarProducer");
        props.put("key.serializer", "org.apache.kafka.common.serialization.IntegerSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.IntegerSerializer");
        producer = new KafkaProducer<>(props);
        this.topic = topic;
        this.isAsync = isAsync;
    }

    public void run() {
        int messageNo = 1;
        int maxMessages = KafkaProperties.BALLS;
        while (messageNo <= maxMessages)
        {
        		int row = -1;
        		int col = -1;
            long startTime = System.currentTimeMillis();
            if (isAsync) { // Send asynchronously
                producer.send(new ProducerRecord<>(topic,
                    row,
                    col), new DemoCallBack(startTime, row, col));
            } else { // Send synchronously
                try {
                    producer.send(new ProducerRecord<>(topic,
                        row,
                        col)).get();
                    if (debug) System.out.println("Sent message: (" + row + ", " + col + ")");
                } catch (InterruptedException | ExecutionException e) {
                    e.printStackTrace();
                }
            }
            ++messageNo;
            long nowTime = System.currentTimeMillis();
            if (debug) System.out.println("Star *** " + (nowTime - zeroTime) + ": " + messageNo);
            
            try {
				Thread.sleep(KafkaProperties.DELAY);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
			
        }
    }
}

class DemoCallBack implements Callback {

    private final long startTime;
    private final int key;
    private final int message;

    public DemoCallBack(long startTime, int key, int message) {
        this.startTime = startTime;
        this.key = key;
        this.message = message;
    }

    public void onCompletion(RecordMetadata metadata, Exception exception) {
        long elapsedTime = System.currentTimeMillis() - startTime;
        if (metadata != null) {
            System.out.println(
                "message(" + key + ", " + message + ") sent to partition(" + metadata.partition() +
                    "), " +
                    "offset(" + metadata.offset() + ") in " + elapsedTime + " ms");
        } else {
            exception.printStackTrace();
        }
    }
}
