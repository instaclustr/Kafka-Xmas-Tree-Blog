package kafkaxmas;

import kafka.utils.ShutdownableThread;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.Collections;
import java.util.Properties;

// consumer to count the number of balls in each bin when they drop off the bottom

public class CountConsumer extends ShutdownableThread {
	private final KafkaConsumer<Integer, Integer> consumer;
    private final String topic;
    private final int maxCols = KafkaProperties.TREE_ROWS;
    private long counts[] = new long[maxCols];
    int balls = KafkaProperties.BALLS; // number of balls expected
    Boolean debug = KafkaProperties.DEBUG;
    int runningTotal = 0;
    long startTime = System.currentTimeMillis();

    public CountConsumer(String topic) {
        super("XmasTreeCountConsumer", false);
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KafkaProperties.KAFKA_SERVER_URL + ":" + KafkaProperties.KAFKA_SERVER_PORT);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "XmasTreeCountConsumer");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
        props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000");
        props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "30000");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.IntegerDeserializer");
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.IntegerDeserializer");

        consumer = new KafkaConsumer<>(props);
        this.topic = topic;
        consumer.subscribe(Collections.singletonList(this.topic));
    }

    @Override
    public void doWork() {
        long pollInterval = 1000;
        ConsumerRecords<Integer, Integer> records = consumer.poll(pollInterval);
        
        for (ConsumerRecord<Integer, Integer> record : records) {
        	  	if (debug) System.out.println("Count Consumer records = " + records.count());
        	  	if (debug) System.out.println("Count Consumer, Received message: (" + record.key() + ", " + record.value() + ") at offset " + record.offset());
        	  	if (record.value() < maxCols)
        	  	{
        	  		counts[record.value()]++;
        	  		runningTotal++;
        	  	}
        }
        
        	if (debug) System.out.println("Count = " + runningTotal);
        // only display counts at end
        long sum = 0;
        for (int i=0; i < maxCols; i++)
        		sum += counts[i];
        if (sum >= balls)
        {
        		System.out.println("A Happy Normally Distributed Xmas! Counts:");
        		for (int i=0; i < maxCols; i++)
        			System.out.println("col " + i + " = " + counts[i]);
        		System.out.println("Total events counted = " + sum);
        		long endTime = System.currentTimeMillis();
        		System.out.println("Total time = " + (endTime - startTime));
        }
        
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

