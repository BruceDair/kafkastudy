package kafkastudy.kafka.demo;


import java.util.Properties;
import java.util.UUID;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;



public class KafkaProducerCase {
	/**
	 * 生成者
	 */
	private static Producer<String, String> producer;
	
	/**
	 * topic
	 */
	private final static String topic = "test4";
	
	/**
	 * 并发数量
	 */
	private final static int num = 50000;
	
	/**
	 * ip地址和端口
	 */
	private final static String ipAndPort = "192.168.95.73:9092";
	
	public static void main(String[] args) {
		 Properties props = new Properties();
		 props.put("bootstrap.servers", ipAndPort);
         props.put("acks", "all");
         props.put("retries", 0);
         props.put("batch.size", 16384);
         props.put("buffer.memory", 33554432);
         props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
         props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		 
         producer = new KafkaProducer<>(props);
         
         KafkaProducerCase a = new KafkaProducerCase();
		 int i=0;
		 while(i<num){
//			 Thread t = new Thread(a.new ProducerThread(i));
//			 t.start();
			 a.new ProducerThread(i).run();
			 i++;
		 }
		 producer.close();
		 
	}
	
	/**
	 * 生成者线程
	 * @author daihongchang
	 *
	 */
	class ProducerThread implements Runnable{
		
		private int i;
		
		public ProducerThread(int arg){
			i = arg;
		}
		
		@Override
		public void run() {
			 String arg = UUID.randomUUID().toString();
			 producer.send(new ProducerRecord(topic, arg, "ccddee"));
		}
		
	}
}
