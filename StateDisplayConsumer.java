package kafkaxmas;

import kafka.utils.ShutdownableThread;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.Arrays;
import java.util.Collections;
import java.util.Properties;

/*
 * Consumer for the tree display, receives events from ON and OFF topics and displays ASCII tree.
 * Modified version, single consumer subscribes to 2 topics and uses records method to return events for each topic in turn.
 * Consumer docs: https://kafka.apache.org/0100/javadoc/index.html?org/apache/kafka/clients/consumer/KafkaConsumer.html
 * Consumer config docs: http://kafka.apache.org/documentation.html#consumerconfigs
 */

public class StateDisplayConsumer extends ShutdownableThread {
	private final KafkaConsumer<Integer, Integer> consumer;
    private final Boolean debug = KafkaProperties.DEBUG; 
    private final String topic1;
    private final String topic2;
    // all lights off by default (false)
    private final int maxRows = KafkaProperties.TREE_ROWS;
    private final int maxCols = maxRows;
    private final boolean[][] tree = new boolean[maxRows][maxCols];	

    public StateDisplayConsumer(String topic1, String topic2) {
        super("XmasTreeStateDisplayConsumer", false);
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KafkaProperties.KAFKA_SERVER_URL + ":" + KafkaProperties.KAFKA_SERVER_PORT);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "XmasTreeStateDisplayConsumer");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
        props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000");
        props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "30000");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.IntegerDeserializer");
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.IntegerDeserializer");

        consumer = new KafkaConsumer<>(props);
        this.topic1 = topic1;
        this.topic2 = topic2;
        consumer.subscribe(Arrays.asList(topic1, topic2));
    }

    @Override
    public void doWork() {
        // compute current state of lights from OFF and ON messages

    		// Process events in OFF Topic first so if a light changes from OFF to ON instantaneously it will stay on.
        long pollInterval = 1000;
        
        // get records for all topics
        ConsumerRecords<Integer, Integer> recordsAll = consumer.poll(pollInterval);   
        
        // get records for OFF topic only
        //ConsumerRecords<Integer, Integer> recordsOFF = (ConsumerRecords<Integer, Integer>) recordsAll.records(KafkaProperties.TOPICOFF);
        
        Iterable<ConsumerRecord<Integer, Integer>> recordsOFF = recordsAll.records(topic1);
        
        for (ConsumerRecord<Integer, Integer> record : recordsOFF)
        {
        	  	//if (debug) System.out.println("Display Consumer OFF records = " + recordsOFF.count());
        	  	if (debug) System.out.println("Display Consumer, OFF Received message: (" + record.key() + ", " + record.value() + ") at offset " + record.offset());        	  	
        	    // paranoid check in case we had a bigger tree in a previous run and some messages are still hanging around unprocessed.
        	  	if (record.key() < maxRows && record.value() < maxCols)
            		tree[record.key()][record.value()] = false;    
        }
        
        // Now process ON topic messages
        Iterable<ConsumerRecord<Integer, Integer>> recordsON = recordsAll.records(topic2);
        //consumer.subscribe(Collections.singletonList(KafkaProperties.TOPICON));
        //	ConsumerRecords<Integer, Integer> records2 = consumer.poll(pollInterval);  
        	for (ConsumerRecord<Integer, Integer> record : recordsON)
        	{
        		//if (debug) System.out.println("Display Consumer ON records = " + recordsON.count());
         	if (debug) System.out.println("Display Consumer, ON Received message: (" + record.key() + ", " + record.value() + ") at offset " + record.offset());         	  	
         	// paranoid check in case we had a bigger tree in a previous run and some messages are still hanging around unprocessed.
         	if (record.key() < maxRows && record.value() < maxCols)
         		tree[record.key()][record.value()] = true;
         }
            
        	// display tree as ASCII
        for (int i=0; i < maxRows; i++)
        {
        		int indent = (maxRows/2) - (i/2);
        		for (int a=0; a < indent; a++)
        			System.out.print(" ");
        		for (int j=0; j <= i; j++)
        		{
        			if (tree[i][j])
        				System.out.print("*");
        			else System.out.print(".");	
        		}
        		System.out.println();
        }
        
        // only display the tree every second
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