package kafkaxmas;

import kafka.utils.ShutdownableThread;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Collections;
import java.util.Properties;
import java.util.Random;


/*
 * Consumer/producer Pair. 
 * Receives from tree topic, sends to count and On/Off state change topics.
 * Simulated gravity by reading event from tree topic, transforming it the next row and randomly left or right cols.
 * If event is special (-1, -1) value then a new ball has arrived, send new event (0,0) to tree topic and a Light ON state change.
 * Else
 * 	Send current position Light OFF state change
 * 	If on bottom row, then send event to count topic.
 * 	Else transform to next row and randomly left or right and send new event to tree topic and light ON state change.
 * 
 *  What are valid producer configs? http://kafka.apache.org/documentation.html#producerconfigs
 */


public class Twinkle extends ShutdownableThread {
	private final KafkaProducer<Integer, Integer> producer;
	private final KafkaConsumer<Integer, Integer> consumer;
    private final String topic;
    Boolean debug = KafkaProperties.DEBUG;
    int maxRow = KafkaProperties.TREE_ROWS;	// size of tree
    static Random rand = new Random();
    
    boolean display = KafkaProperties.DISPLAY; // display ASCII tree or not

    public Twinkle(String topic) {
        super("Twinkle", false);
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KafkaProperties.KAFKA_SERVER_URL + ":" + KafkaProperties.KAFKA_SERVER_PORT);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "Twinkle");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
        props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000");
        props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "30000");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.IntegerDeserializer");
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.IntegerDeserializer");

        consumer = new KafkaConsumer<>(props);
        this.topic = topic;
        consumer.subscribe(Collections.singletonList(this.topic));
        
        // producer, shared across all output topics
        Properties pprops = new Properties();        
        pprops.put("bootstrap.servers", KafkaProperties.KAFKA_SERVER_URL + ":" + KafkaProperties.KAFKA_SERVER_PORT);
        pprops.put("client.id", "TwinkleProducer");
        pprops.put("key.serializer", "org.apache.kafka.common.serialization.IntegerSerializer");
        pprops.put("value.serializer", "org.apache.kafka.common.serialization.IntegerSerializer");
        producer = new KafkaProducer<>(pprops);
    }

    @Override
    public void doWork() {
    	 	int row;
        int col;
        long pollInterval = 1000;
        
        ConsumerRecords<Integer, Integer> records = consumer.poll(pollInterval);
			
        for (ConsumerRecord<Integer, Integer> record : records)
        {
        	   	if (debug) System.out.println("Twinkle got records = " + records.count());
        	   	if (debug) System.out.println("Twinkle: processing record = (" + record.key() + ", " + record.value() + ") at offset " + record.offset());
        	   	row = record.key();
        	   	col = record.value();
        		if (row == -1)
        		// ball dropped in from the top, put it in Star location (0,0) and turn it ON for a second
        		{
        				row = 0;
        				col = 0;
        				// put (0,0) into topic and send ON event        				
        				producer.send(new ProducerRecord<Integer, Integer>(this.topic, row, col));
                		if (debug) System.out.println("Twinkle STAR ON + (" + row + ", " + col  + ") ON");
                		producer.send(new ProducerRecord<Integer, Integer>(KafkaProperties.TOPICON, row, col));
        		}
        		else 
        		{
        			// turn light OFF in current position
        			if (debug) System.out.println("Twinkle + (" + row + ", " + col + ") OFF");
        			producer.send(new ProducerRecord<Integer, Integer>(KafkaProperties.TOPICOFF, row, col));
        			// increment row (gravity!), if row >= maxRow then don't publish back to tree topic, send to count topic instead
        			int nextRow = row + 1;
        			if (nextRow >= maxRow)
        			{
        				if (debug) System.out.println("Twinkle, ball on maxRow!");
        				// ball drops off bottom, so send event to TOPIC2 for counting
        				producer.send(new ProducerRecord<Integer, Integer>(KafkaProperties.TOPIC2, row, col));
        			}
        			else // random pick left or right direction and send new location back to tree topic and ON state change
        			// 
        			{
        				int nextCol = col;
        				// choose left or right bulb
        				if (rand.nextBoolean())
        					nextCol += 1;
        				if (debug) System.out.println("Twinkle: next " + nextRow + ", " + nextCol);
        				producer.send(new ProducerRecord<Integer, Integer>(this.topic, nextRow, nextCol));
        				if (debug) System.out.println("Twinkle + (" + nextRow+ ", " + nextCol + ") ON");
        				producer.send(new ProducerRecord<Integer, Integer>(KafkaProperties.TOPICON, nextRow, nextCol));
        			} 
        		}
        }
        // processed all records obtained in poll above, now sleep for some time so that lights will stay on for a while.
        try {
			Thread.sleep(KafkaProperties.DELAY);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
    }

    @Override
    public String name() {
        return null;
    }

    @Override
    public boolean isInterruptible() {
        return false;
    }
}

