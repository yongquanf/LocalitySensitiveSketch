package DisaggregatedMonitoringFramework.Sketching;

import static com.google.common.base.Preconditions.checkNotNull;

import java.io.BufferedWriter;
import java.io.FileWriter;
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

import DisaggregatedMonitoringFramework.control.LogicController;
import ECS.TestECS.TestSketchPackets;
import edu.harvard.syrah.sbon.async.Config;
import util.bloom.Apache.Key;

public class Sub4SketchingKVStream {

	private final static Logger log = LoggerFactory.getLogger(Sub4SketchingKVStream.class);
	/**
	 * 
	 */
	private Map<String,LSSFingerprintAtomic> lssaMap;
	
	/**
	 * singleton
	 */
	private static LSSFingerprintAtomic lssaSingleton = null;
	
	

	
	
	
	
	/**
	 * client
	 */
	PulsarClient client = null;
	/**
	 * consumer
	 */
	String consumerSubscriber = "LSSAtomicMultithreaded";
	 
	/**
	 * topic, name, id
	 */
	Map<String,Integer> TopicMap;
	
	/**
	 * consumer map
	 */
	Map<String,Consumer<byte[]>> consumerMap;
	 

	/**
	 * producer of sketch
	 */
	Pub4sketchingKVStreaming producerSketch;
	
	public static final String postfix4SketchProducerTopics = "sketch";
	
	
	
	/**
	 * 
	 */
	//TestPubSubPacketStreamConsumer test;
	
	ExecutorService pool = Executors.newFixedThreadPool(10); 
	
	/**
	 * 
	 */
	public Sub4SketchingKVStream(){
		
		/**
		 * number of flows per sketch
		 */
		int _n = (int)  LogicController.NumberFlowsPerPeriod;
		/**
		 * cluster num
		 */
		int _c = LogicController.clusterCount;
		/**
		 * bucket num
		 */
		int _b = LogicController.bucketCount;
		/**
		 * fp for bf test
		 */
		float _fp = LogicController.expectedFP ;
		
		BufferedWriter bufferedWriter;
		try {
			bufferedWriter = new BufferedWriter(new FileWriter(LogicController.WriteLogFileName,
					true));
			/**
			 * init
			 */
			 getOneInstanceLSSAtomicMultithreaded(_n, _c, _b, _fp, LogicController.traceName, LogicController.FingerLen, bufferedWriter);
			
			 
		} catch (IOException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		 
		
		
		try {	
			lssaMap = Maps.newConcurrentMap();
			TopicMap = Maps.newConcurrentMap();
			consumerMap = Maps.newConcurrentMap();
				
				client = PulsarClient.builder()
				        .serviceUrl(LogicController.serviceURL)
				        .ioThreads(Runtime.getRuntime().availableProcessors()) //
				        .build();
				
				//
				producerSketch = new Pub4sketchingKVStreaming(client);
							
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		
	}
	
	/**
	 * build a new instance
	 * @param _n
	 * @param _c
	 * @param _b
	 * @param _fp
	 * @param traces
	 * @param _useEqual
	 * @param numEntries
	 * @param FingerLen
	 * @param bufferedWriter
	 * @return
	 */
	public static LSSFingerprintAtomic getOneInstanceLSSAtomicMultithreaded(int _n, int _c, 
			int _b,float _fp,String traceName,
			 int FingerLen, 
			BufferedWriter bufferedWriter){
				
		if(lssaSingleton==null){
			
			
			
			try {
				Map<Key, Integer> traces;
				traces = TestSketchPackets.openStream4Map(traceName, -1);
				lssaSingleton = LSSFingerprintAtomic.build(_n,
						_c, _b, _fp, traces.values(),
						FingerLen, 
						bufferedWriter);
					traces.clear();
					
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			
			
		}
		
		return lssaSingleton;
		
		//test = new TestPubSubPacketStreamConsumer(serviceURL, _TOPIC_NAME,_subscribeName,_maxBatchMessages);
	

	}
	
	/**
	 * consumer builder
	 * @return
	 */
	private ConsumerBuilder<byte[]> getClientBuilder(){
		
//		/**
//		 * message listen
//		 */
//		 MessageListener<byte[]> listener = (consumer, msg) -> {
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
		this.TopicMap.put(topic, 0);
	}
	
/**
 * batch create	
 * @param kvTopics
 * @throws InterruptedException
 * @throws ExecutionException
 */
public void batchCreateConsumer(List<String> kvTopics) throws InterruptedException, ExecutionException{
		
		ConsumerBuilder<byte[]> pb = getClientBuilder();
		
		 List<Future<Consumer<byte[]>>> futures = Lists.newArrayList();
		 Iterator<String> ier = kvTopics.iterator();
		 while(ier.hasNext()){
			 String topic = ier.next();
			 
			 /**
			  * init the lss
			  */
			 if(!lssaMap.containsKey(topic)){
				 lssaMap.put(topic, lssaSingleton.clone());
			 }
			 
			 /**
				 * message listen
				 */
				 MessageListener<byte[]> listener = (consumer, msg) -> {
					    //pool
					 					  					 					 
					    pool.execute(new ConsumerSketchHandler(msg,consumer,topic,msg.getValue()));
//					 if(msg!=null&&msg.getData()!=null&&msg.getValue()!=null){
//					 byte[] bytes = msg.getValue();
//					 int state = isStartOrEndInterval(topic);
//						//start
//						if(state ==1){
//							log.info("interval end!");
//							removeAndPublishSketch(topic,bytes);
//							
//						//end	
//						}else{
//						//middle, end signal
//							//if(isStartOrEndInterval(bytes.key, bytes.counter)==1){
//							//	removeAndPublishSketch(topic,bytes);
//							//}else{
//							log.info("insert! "+topic);
//							lssaMap.get(topic).insert(bytes);
//							//}
//						}   
//						
//					 
//					 //ack
//			            consumer.acknowledgeAsync(msg);
			            
					 //}
			        };
			 
			 
			 futures.add(pb.clone()
					 .subscriptionName(topic)
					 .messageListener(listener)
					 .topic(topic)					 
					 .subscribeAsync());
			 //register
			 registerTopic(topic);
		 }
		 
		 //final List<Producer<byte[]>> producers = Lists.newArrayListWithCapacity(futures.size());
	        for (int i=0;i<futures.size();i++ ) {
	        	consumerMap.put(kvTopics.get(i),futures.get(i).get());
	        }
	        
	        log.info("Created {} producers", consumerMap.size());        
	         
	}

/**
 * test the number of flows.
 * @param topic2
 * @param key
 * @param counter
 * @return
 */
private int isStartOrEndInterval(String topic2) {
	// TODO Auto-generated method stub
	LSSFingerprintAtomic sk = lssaMap.get(topic2);
	
	//collect enough flows
	if(sk.distinctFlows()>LogicController.NumberFlowsPerPeriod){
		return 1;
	}else{
		return 2;
	}
}

	
	class ConsumerSketchHandler implements Runnable{

		//private LSSFingerprintAtomic instance;

		byte[] bytes;

		String topic;
		
		Consumer<byte[]> me;
		
		Message<byte[]> msg;
		
		/**
		 * 
		 * @param _msg 
		 * @param consumer 
		 * @param lssa
		 * @param _maxCount
		 * @param _byteArraySize
		 */
		public ConsumerSketchHandler(Message<byte[]> _msg, Consumer<byte[]> consumer, String _topic,byte[] _bytes){
			//instance = lssa;
			bytes = _bytes;
			topic = _topic;
			me = consumer;
			msg = _msg;
		}
		
		@Override
		public void run() {
			// TODO Auto-generated method stub
			try{
			
				//concurrent access
				 if(lssaMap.containsKey(topic)){			 
			//start
				if(overflow(topic)){
					//log.info("interval end!");
					removeAndPublishSketch(topic,bytes);
					
				//end	
				}else{
					LSSFingerprintAtomic xx = lssaMap.get(topic);
					//log.info("flow counte: "+xx.distinctFlows());
				//middle, end signal
					//if(isStartOrEndInterval(bytes.key, bytes.counter)==1){
					//	removeAndPublishSketch(topic,bytes);
					//}else{
					xx.insert(bytes);
					//}
				}
				 }
				//ack
	            me.acknowledgeAsync(msg);
				
				
			}catch(Exception e){
				e.printStackTrace();
			}
		}
		private  boolean overflow(String topic2) {
			// TODO Auto-generated method stub
			LSSFingerprintAtomic sk = lssaMap.get(topic2);
			
			//collect enough flows
			if(sk.distinctFlows()<LogicController.NumberFlowsPerPeriod){
				return false;
			}else{
				return true;
			}
		}
		
		
		/**
		 * 
		 * @param key
		 * @param counter
		 * @return 1, end, 2, others
		 */
		private int isStartOrEndInterval(Key key, int counter){
			 if(counter == endSignal){
				return 1;
			}else{
				return 2;
			}
			
		}
	}

	/**
	 * packet state
	 */
	//public final static int startSignal = -1;
	public final static int endSignal = -2;
	
	private void removeAndPublishSketch(String topic, byte[] kv){
		
		//update
		lssaMap.get(topic).insert(kv);
		
		//publish, sketch topic
		publishSketch(getTopic4SketchPublish(topic),lssaMap.get(topic));
		
		reset(lssaMap.get(topic));
		//lssaMap.remove(topic);
		
		//insert a new 
		 /**
		  * init the lss
		  */
		 //if(!lssaMap.containsKey(topic)){
		//	 LSSFingerprintAtomic xx = lssaSingleton.clone();
			 //xx.insert(kv);
		//	 lssaMap.put(topic, xx);
		 //}
	}
	
	/**
	 * reset the entries
	 * @param lssFingerprintAtomic
	 */
	private void reset(LSSFingerprintAtomic lssFingerprintAtomic) {
		// TODO Auto-generated method stub
		
		 
		lssFingerprintAtomic.resetShadow();						
		lssFingerprintAtomic.resetCBF();
		lssFingerprintAtomic.resetBucketArray();
	}

	/**
	 * get topic
	 * @param topic
	 * @param counter 
	 * @param key 
	 */
	private void removeAndPublishSketch(String topic, Key key, int counter){
		LSSFingerprintAtomic sketch = lssaMap.remove(topic);
		publishSketch(topic,sketch);
		//insert a new 
		 /**
		  * init the lss
		  */
		 if(!lssaMap.containsKey(topic)){
			 LSSFingerprintAtomic xx = lssaSingleton.clone();
			 xx.insert(key, counter);
			 lssaMap.put(topic, xx);
		 }
	}

	/**
	 * publish to the bus 
	 * @param topic
	 * @param sketch
	 */
	private void publishSketch(String topic, LSSFingerprintAtomic sketch) {
		// TODO Auto-generated method stub
		producerSketch.publishEntry(topic, sketch);
	}
	
	/**
	 * get topic list
	 * each host is a topic
	 * @return
	 */
	public List<String> prepareTopics(){
		
		List<String>   SketchProducerTopics = Lists.newArrayList();
		//sketch
		List<String> ts0 = Lists.newArrayList();
		for(int i=0;i<LogicController.kvTopics.length;i++){
			if(LogicController.kvTopics[i].length()>0){
				log.info("topic: "+LogicController.kvTopics[i]);
				ts0.add(LogicController.kvTopics[i]);
				
				//create sketch producer topics
				SketchProducerTopics.add(getTopic4SketchPublish(LogicController.kvTopics[i]));
				
			}
		}
		//create producer topics
		try {
			producerSketch.batchCreateProducer(SketchProducerTopics, LogicController.batchingMaxPublishDelay);
		
			SketchProducerTopics.clear();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (ExecutionException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return ts0;
	}
	
	/**
	 * get topic for sketch producer
	 * @param string
	 * @return
	 */
	public static String getTopic4SketchPublish(String string) {
		// TODO Auto-generated method stub
		return string+postfix4SketchProducerTopics;
	}
	
	/**
	 * match against prefix
	 * @param topic
	 */
	public boolean matchSketchPublishTopic(String topic){
		return producerSketch.TopicMap.containsKey(topic)||producerSketch.TopicMap.containsKey(getTopic4SketchPublish(topic));
	}

	/**
	 * entry
	 * @param args
	 */
	public static void main(String[] args){
		
		
		Sub4SketchingKVStream one = new Sub4SketchingKVStream();
		//prepare topics to listen, and make sketch producer
		List<String> ts = one.prepareTopics();
		try {
			one.batchCreateConsumer(ts);
		} catch (InterruptedException | ExecutionException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}
}
