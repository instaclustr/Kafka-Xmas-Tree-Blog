package kafkaxmas;

/*
 * Top level main program to run Xmas Tree Simulation.
 * start star producer, twinkle consumer/producer, and consumers for count and display.
 */


public class XmasTree {
 public static void main(String[] args) {
     boolean isAsync = args.length == 0 || !args[0].trim().equalsIgnoreCase("sync");
     
     System.out.println("Welcome to the Instaclustr XMAS Tree Simulator!!!!!");
     
     // Start balls dropping onto top of tree.
    	 StarProducer producerThread = new StarProducer(KafkaProperties.TOPIC, isAsync);
    	 producerThread.start();
     
     // Start state display consumer. Note that the TOPIC arg is ignored as it subscribes to 2 topics which are hardcoded.
     StateDisplayConsumer displayTree = new StateDisplayConsumer(KafkaProperties.TOPIC);
     displayTree.start();
     
     // start count consumer, subscribes to TOPIC2
     CountConsumer counts = new CountConsumer(KafkaProperties.TOPIC2);
     counts.start();
     
     // start twinkle consumer/producer application, subscribe to same topic as the star producer.
     Twinkle twinkleThread = new Twinkle(KafkaProperties.TOPIC);
     twinkleThread.start();
     
     // Note that even though the star producer eventually stops, the other threads keep running for ever.
 }
}