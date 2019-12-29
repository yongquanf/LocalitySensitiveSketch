package DisaggregatedMonitoringFramework.Sketching;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.LongAdder;

import org.apache.pulsar.client.api.MessageRoutingMode;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.ProducerBuilder;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.impl.schema.JSONSchema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import DisaggregatedMonitoringFramework.control.LogicController;
import edu.harvard.syrah.sbon.async.Config;
import io.netty.util.concurrent.DefaultThreadFactory;


public class Pub4sketchingKVStreaming {
	//logger
	private static final Logger log = LoggerFactory.getLogger(Pub4sketchingKVStreaming.class);
	
	//thread
	  private static final ExecutorService executor = Executors
	            .newCachedThreadPool(new DefaultThreadFactory("PubSub4PacketStream"));	
			
	/**
	 * client	
	 */
	PulsarClient client = null;
	/**
	 * atomic
	 */
	private static final LongAdder messagesSent = new LongAdder();
	private static final LongAdder bytesSent = new LongAdder();
	/**
	 * producer
	 * @param batchMessagesMaxMessagesPerBatch
	 */
	private static int batchMessagesMaxMessagesPerBatch=10000;

	
	
	/**
	 * topic, name, id
	 */
	Map<String,Integer> TopicMap;

	private ConcurrentMap<String,Producer<byte[]>> producerMap;
	

	
	
	 
	/**
	 * constructor
	 * @param serviceURL
	 * @param _TOPIC_NAME
	 * @param _subscribeName
	 * @param _batchMessagesMaxMessagesPerBatch
	 */
	public Pub4sketchingKVStreaming(PulsarClient _client){
		
		batchMessagesMaxMessagesPerBatch = LogicController.batchMessagesMaxMessagesPerBatch;
			
		try {
			client = _client;
			
		  
						
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		
		TopicMap = Maps.newConcurrentMap();
		producerMap = Maps.newConcurrentMap();
		
	}
	 
    
	
	/**
	 * return producer builder
	 * @return
	 */
	private ProducerBuilder<byte[]> getProducerBuilder(int batchTimeMillis){
		
		 ProducerBuilder<byte[]> producerBuilder = client.newProducer() //
	                .sendTimeout(0, TimeUnit.SECONDS) //              
	                .maxPendingMessages(batchMessagesMaxMessagesPerBatch)
	                .messageRoutingMode(MessageRoutingMode.RoundRobinPartition);
		 
		 if(batchTimeMillis>0){
		 long batchTimeUsec = (long) (batchTimeMillis * 1000);
         producerBuilder.batchingMaxPublishDelay(batchTimeUsec, TimeUnit.MICROSECONDS)
                 .enableBatching(true);
		 }else{
			 producerBuilder.enableBatching(false);
		 }
		 return producerBuilder;
	}
	
	/**
	 * put topic
	 * @param topic
	 */
	private void registerTopic(String topic){
		this.TopicMap.put(topic, 0);
	}
	
	/**
	 * 
	 * @param topic
	 * @param packet
	 */
	public void publishEntry(String topic,LSSFingerprintAtomic rec){
		
		
		AsyncPush(topic,rec);
	}
	
	
	
	
	 
	
	 
 
	
	
	/**
	 * push to the message bus
	 */
	private void AsyncPush(String topic,LSSFingerprintAtomic out){
			
		 //executor.submit(() -> {
	            //try {
	            	//not in, create, delay
	            	if(!producerMap.containsKey(topic)){
	            		try {
							CreateProducer(topic,batchMessagesMaxMessagesPerBatch);
						} catch (InterruptedException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						} catch (ExecutionException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						}
	            	}
	            	
	            	byte[] lssBytes=out.LSSTransform2Bytes();
	            	if(lssBytes==null){
	            		return;
	            	}else{
	            	//get producer
	    			 Producer<byte[]> producer = producerMap.get(topic);
                        producer.sendAsync(lssBytes).thenRun(() -> {
	                            messagesSent.increment();
	                            bytesSent.add(lssBytes.length);
	                            
	                            //clear
	                            //out.clear();
	                            //lssBytes = null;
	                         
	                        }).exceptionally(ex -> {
	                            log.warn("Write error on message", ex);
	                            System.exit(-1);
	                            return null;
	                        });
	            	}
	                
	            //} catch (Throwable t) {
	            //    log.error("Got error", t);
	           // }
	       // });
		 
		
	}
	
	

	/**
	 * generate producer interfaces
	 * @param topics
	 * @param pb
	 * @return
	 * @throws InterruptedException
	 * @throws ExecutionException
	 */
	public void batchCreateProducer(List<String> topics,int batchTimeMillis) throws InterruptedException, ExecutionException{
		
		ProducerBuilder<byte[]> pb = getProducerBuilder(batchTimeMillis);
		
		 List<Future<Producer<byte[]>>> futures = Lists.newArrayList();
		 Iterator<String> ier = topics.iterator();
		 while(ier.hasNext()){
			 String t = ier.next();
			 futures.add(pb.clone().topic(t).createAsync());
			 registerTopic(t);
		 }
		 
		 //final List<Producer<byte[]>> producers = Lists.newArrayListWithCapacity(futures.size());
	        for (int i=0;i<futures.size();i++ ) {
	        	Producer<byte[]> x = futures.get(i).get();
	        	producerMap.put(topics.get(i),x);
	        	
	        }
	        
	        log.info("Created {} producers", producerMap.size());
	        
        				 
				    
	}
	
	public void CreateProducer(String topic,int batchTimeMillis) throws InterruptedException, ExecutionException{
		
		ProducerBuilder<byte[]> pb = getProducerBuilder(batchTimeMillis);
		
		 //List<Future<> futures = Lists.newArrayList();
		 
		 	registerTopic(topic);
			 //futures.add(pb.clone().topic(topic).createAsync());
		   CompletableFuture<Producer<byte[]>> pbproducer = pb.clone().topic(topic).createAsync();
		 		 
		 //final List<Producer<byte[]>> producers = Lists.newArrayListWithCapacity(futures.size());	         
	        	producerMap.put(topic,pbproducer.get());	        
	        
	        log.info("Created {} producers", producerMap.size());        
	         
	}
	
		
}
