package DisaggregatedMonitoringFramework.Query;

import static com.google.common.base.Preconditions.checkNotNull;

import java.io.BufferedWriter;
import java.io.IOException;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.ConsumerBuilder;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageListener;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.ProducerBuilder;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.SubscriptionType;
import org.apache.pulsar.client.impl.schema.JSONSchema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Queues;

import DisaggregatedMonitoringFramework.Sketching.LSSFingerprintAtomic;
import DisaggregatedMonitoringFramework.Sketching.Sub4SketchingKVStream;
import DisaggregatedMonitoringFramework.control.LogicController;
import util.bloom.Apache.Key;

public class Consumer4sketch {

	private final static Logger log = LoggerFactory.getLogger(Consumer4sketch.class);

	/**
	 * query on the lss atomic
	 */
	QueryLSSFingerFlowApps queryInstance;
	
	/**
	 * client
	 */
	PulsarClient client = null;
	/**
	 * consumer
	 */
	String consumerSubscriber = "Consumer4sketch";
	 
	/**
	 * topic, name, id
	 */
	Map<String,Integer> TopicMap;
	
	/**
	 * consumer map
	 */
	Map<String,Consumer<byte[]>> consumerMap;
	
	/**
	 * lock for heavy change test
	 */
	//ReentrantLock lock = new ReentrantLock();
	
	/**
	 * 
	 */
	//TestPubSubPacketStreamConsumer test;
	
	ExecutorService pool = Executors.newFixedThreadPool(10); 
	
	/**
	 * 
	 */
	public Consumer4sketch(String QueryResultWriteName){
		
	queryInstance =  new QueryLSSFingerFlowApps(QueryResultWriteName);
		
		try {
			
									 
			TopicMap = Maps.newConcurrentMap();
			consumerMap = Maps.newConcurrentMap();
				
				client = PulsarClient.builder()
				        .serviceUrl(LogicController.serviceURL)
				        .ioThreads(Runtime.getRuntime().availableProcessors()) //
				        .build();
							
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		
	}
	
	
	/**
	 * consumer builder
	 * @return
	 */
	private ConsumerBuilder<byte[]> getClientBuilder(){
		
//		/**
//		 * message listen
//		 */
//		 MessageListener<LSSFingerprintAtomic> listener = (consumer, msg) -> {
//			    //pool
//			    pool.execute(new Handler(lssa,msg.getValue()));
//			    //ack
//	            consumer.acknowledgeAsync(msg);
//	        };
		
		ConsumerBuilder<byte[]> clientBuilder = client.newConsumer()
				.receiverQueueSize(LogicController.receiverQueueSize)
				.acknowledgmentGroupTime(LogicController.acknowledgmentsGroupingDelayMillis, TimeUnit.MILLISECONDS)
				.subscriptionType(LogicController.subscriptionType);
		return clientBuilder;
	}
	
	/**
	 * put topic
	 * @param topic
	 */
	private void registerTopic(String topic){
		this.TopicMap.put(topic, 1);
	}
	
	
	/**
	 * prepare the topics
	 * @return
	 */
	public List<String> prepareQueryListenTopics(){
		List<String>   SketchProducerTopics = Lists.newArrayList();
		 
		for(int i=0;i<LogicController.kvTopics.length;i++){
			if(LogicController.kvTopics[i].length()>0){
				//create sketch producer topics
				SketchProducerTopics.add(Sub4SketchingKVStream.getTopic4SketchPublish(LogicController.kvTopics[i]));				
			}
		}
		return SketchProducerTopics;
	}
	
/**
 * batch create	
 * @param topics
 * @throws InterruptedException
 * @throws ExecutionException
 */
public void batchCreateConsumer(List<String> topics) throws InterruptedException, ExecutionException{
		
		ConsumerBuilder<byte[]> pb = getClientBuilder();
		
		 List<Future<Consumer<byte[]>>> futures = Lists.newArrayList();
		 Iterator<String> ier = topics.iterator();
		 while(ier.hasNext()){
			 String topic = ier.next();
			 
			 
			 /**
				 * message listen
				 */
				 MessageListener<byte[]> listener = (consumer, msg) -> {
					    //pool
					 					 
					    pool.execute(new ConsumerHandler(consumer,topic,msg));
					   
			        };
			 
			 
			 futures.add(pb.clone()
					 .subscriptionName(topic)
					 .messageListener(listener)
					 .topic(topic)					 
					 .subscribeAsync());
			 registerTopic(topic);
		 }
		 
		 //final List<Producer<byte[]>> producers = Lists.newArrayListWithCapacity(futures.size());
	        for (int i=0;i<futures.size();i++ ) {
	        	consumerMap.put(topics.get(i),futures.get(i).get());
	        }
	        
	        log.info("Created {} producers", consumerMap.size());        
	         
	}

	
	
	
	class ConsumerHandler implements Runnable{

		 
		LSSFingerprintAtomic obtainedLSSAtomic;
		Message<byte[]> bytes;
		String topic;
		Consumer<byte[]> consumer;
		/**
		 * 
		 * @param consumer 
		 * @param lssa
		 * @param _maxCount
		 * @param _byteArraySize
		 */
		public ConsumerHandler(Consumer<byte[]> _consumer, String _topic,Message<byte[]> msg){
			/**
			 * decode 
			 */
			bytes=msg;
			topic = _topic;
			consumer = _consumer;
		}
		
		@Override
		public void run() {
			// TODO Auto-generated method stub
			try{
//				 boolean isLockAcquired = lock.tryLock(100, TimeUnit.MILLISECONDS);
//				 
//				 if(isLockAcquired ){			 
//					 try{    
						 
						 
						 obtainedLSSAtomic = LSSFingerprintAtomic.Bytes2LSS(bytes.getData());
						 queryOnLSSAtomic(topic,obtainedLSSAtomic);
						 obtainedLSSAtomic.clear();
					 //}finally{
						// lock.unlock();
						 //ack
				         consumer.acknowledgeAsync(bytes);
					// }
				// }
			}catch(Exception e){
				e.printStackTrace();
			}
		}

		/**
		 * query on the sketch
		 * @param topic2
		 * @param obtainedLSSAtomic2
		 */
		private void queryOnLSSAtomic(String topic2, LSSFingerprintAtomic obtainedLSSAtomic2) {
			// TODO Auto-generated method stub
			try {
				log.info("begin query: "+topic2+" "+obtainedLSSAtomic2.toString());
				queryInstance.bufferedWriter.write(System.currentTimeMillis()+", "+topic2+
						" "+obtainedLSSAtomic2.sketchsize()+" "+obtainedLSSAtomic2.cuckSize()+"\n");
				queryInstance.process(obtainedLSSAtomic2);
				queryInstance.bufferedWriter.flush();
				log.info("end query: "+topic2+" "+obtainedLSSAtomic2.toString());
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			
		}
				
	}

	 
	/**
	 * 
	 * @param args
	 */
	 public static void main(String[] args){
		 
		 String queryName = LogicController.myDNSAddress+"QueryResult.log";
		 
		 Consumer4sketch one = new Consumer4sketch(queryName);
		 
		 List<String> ts = one.prepareQueryListenTopics();
		 
		 try {
			one.batchCreateConsumer(ts);
		} catch (InterruptedException | ExecutionException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	 }
	  
	
}
