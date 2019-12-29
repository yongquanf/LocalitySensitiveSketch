package DisaggregatedMonitoringFramework.Ingest;

import java.io.BufferedInputStream;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.LongAdder;

import org.apache.pulsar.client.api.MessageRoutingMode;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.ProducerBuilder;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.Schema;
import org.pcap4j.core.BpfProgram;
import org.pcap4j.core.NotOpenException;
import org.pcap4j.core.PacketListener;
import org.pcap4j.core.PcapAddress;
import org.pcap4j.core.PcapHandle;
import org.pcap4j.core.PcapNativeException;
import org.pcap4j.core.PcapNetworkInterface;
import org.pcap4j.core.Pcaps;
import org.pcap4j.core.PcapNetworkInterface.PromiscuousMode;
import org.pcap4j.packet.IcmpV4CommonPacket;
import org.pcap4j.packet.IcmpV4CommonPacket.IcmpV4CommonHeader;
import org.pcap4j.packet.IpPacket;
import org.pcap4j.packet.IpV4Packet;
import org.pcap4j.packet.Packet;
import org.pcap4j.packet.TcpPacket;
import org.pcap4j.packet.TcpPacket.TcpHeader;
import org.pcap4j.packet.UdpPacket;
import org.pcap4j.packet.UdpPacket.UdpHeader;
import org.pcap4j.util.ByteArrays;
import org.pcap4j.packet.IpPacket.IpHeader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.io.ByteStreams;
import DisaggregatedMonitoringFramework.control.LogicController;
import edu.harvard.syrah.prp.NetUtil;
import edu.harvard.syrah.prp.POut;
import io.netty.util.concurrent.DefaultThreadFactory;
import util.bloom.Apache.Key;
import io.netty.util.HashedWheelTimer;
import io.netty.util.Timeout;
import io.netty.util.Timer;
import io.netty.util.TimerTask;


public class Pub4PCapStreamKVTable {
	//logger
	private static final Logger log = LoggerFactory.getLogger(Pub4PCapStreamKVTable.class);
	
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
	 * batch process
	 */
	private static boolean useBatch4Pcap = true;
	

	
	/**
	 * topic, name, id
	 */
	Map<String,Long> TopicMap;
	
	Map<String,Producer<byte[]>> producerMap;
 
	/**
	 * ring buffer
	 */
	//simpleRingBuffer<byte[]> buff;
	
	public Map<Key, Integer> buffMap; 
	
	/**
	 * pcap listen
	 */
	private PacketListener listener;
	
	/**
	 * handler to pcap
	 */
    PcapHandle dumpHandler;
    
    BpfProgram prog = null;
	//public final static String filterProg=Config.getConfigProps()
			//.getProperty("filterProg", "ip host 10.107.20.2 and 10.107.20.3");
	
	/**
	 * denote the timer
	 */
	 //private TimerTask IntervalEndTimeoutTask;
	 private Timeout IntervalEndTimeout;
	
	
	 /**
	  * timer
	  */
	 private final Timer timer;
	
	
	 
	/**
	 * constructor
	 * @param serviceURL
	 * @param _TOPIC_NAME
	 * @param _subscribeName
	 * @param _batchMessagesMaxMessagesPerBatch
	 */
	public Pub4PCapStreamKVTable(){
			 
		
		//buff = new simpleRingBuffer<byte[]>(batchMessagesMaxMessagesPerBatch);
		buffMap = Maps.newConcurrentMap();
		
		/**
		 * timer
		 */
		timer = new HashedWheelTimer(getThreadFactory("pulsar-ingest-timer"), 1, TimeUnit.MILLISECONDS);
		
		try {
			
			client = PulsarClient.builder()
			        .serviceUrl(LogicController.serviceURL)
			        .ioThreads(Runtime.getRuntime().availableProcessors()) //
			        .build();
			
		  
						
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		
		TopicMap = Maps.newConcurrentMap();
		producerMap = Maps.newConcurrentMap();
		
	}
	
	public Pub4PCapStreamKVTable(int choice){
			 
		
		//buff = new simpleRingBuffer<byte[]>(batchMessagesMaxMessagesPerBatch);
		buffMap = Maps.newConcurrentMap();
			
		TopicMap = Maps.newConcurrentMap();
		producerMap = Maps.newConcurrentMap();
		
		timer = null;
	}
	
		/**
		 * thread
		 * @param poolName
		 * @return
		 */
	   private static ThreadFactory getThreadFactory(String poolName) {
	        return new DefaultThreadFactory(poolName, Thread.currentThread().isDaemon());
	    }
	   
	   /**
	    * timer
	    */
	   TimerTask IntervalEndAndSendTask0 = new TimerTask() {

		  int  intervalPeriod=1000000; 
		   
	        @Override
	        public void run(Timeout timeout) throws Exception {
	            if (timeout.isCancelled()) {
	                return;
	            }
	            if (log.isDebugEnabled()) {
	                log.debug("[{}] [{}] Batching the messages from the batch container from timer thread", 
	                        "Interval end");
	            }
	            // semaphore acquired when message was enqueued to container
	            synchronized (Pub4PCapStreamKVTable.this) {
	                // If it's closing/closed we need to ignore the send batch timer and not schedule next timeout.
	                 
	                IntervalEndMessageAndSend0();
	                // schedule the next batch message task
	                IntervalEndTimeout = timer
	                    .newTimeout(this, intervalPeriod, TimeUnit.MILLISECONDS);
	            }
	        }

	    };

	/**
	 * constant    
	 */
	public final static float publishThreshold = LogicController.scaleBuff*LogicController.NumberFlowsPerPeriod;

	    /**
	     * send an interval end to all registered topics
	     */
		private void IntervalEndMessageAndSend0() {
			// TODO Auto-generated method stub
			
			String topic;
			Iterator<String> ier = TopicMap.keySet().iterator();
			while(ier.hasNext()){
				topic = ier.next();
				//AsyncPushOneFlow(topic, new byte[](new Key(new byte[]{0,0,0}),Sub4SketchingKVStream.endSignal));
			}
			}	    
	
		/**
		 * send a complete signal
		 * @param topic
		 */
		private void CheckNumberPacketsEnoughEndMessageAndSend0(String topic) {
			// TODO Auto-generated method stub

			if(TopicMap.get(topic)>LogicController.NumberFlowsPerPeriod){
				//AsyncPushOneFlow(topic, new byte[](new Key(new byte[]{0,0,0}),Sub4SketchingKVStream.endSignal));
				//reset
				TopicMap.replace(topic,0L);
			}
		}
		
	/**
	 * return producer builder
	 * @return
	 */
	private ProducerBuilder<byte[]> getProducerBuilder(int batchTimeMillis){
		
		 ProducerBuilder<byte[]> producerBuilder = client.newProducer() //
	                .sendTimeout(0, TimeUnit.SECONDS) //              
	                .maxPendingMessages(LogicController.batchMessagesMaxMessagesPerBatch)
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
		this.TopicMap.put(topic, 0L);
	}
	
	
	

	 
	
	
	/**
	 * topic
	 * @param topic
	 */
	public void startPublishPcap(String topic){
		
		openPcap();
		//listener
		listener = new PacketListener(){
		@Override
		public void gotPacket(Packet packet) {
			// TODO Auto-generated method stub
			//log.info(packet.toString());
			//&&packet.contains(TcpPacket.class)&&packet.contains(TcpPacket.class)
		
			
			//scale
			if(packet.contains(IpV4Packet.class)){	
	
				byte[] rec = obtainPackets4Source(packet);
				
				//if(!useBatch4Pcap){
				//	publishEntry(topic,rec);
				//}else{
				   if(rec!=null){
					cacheUpdate(topic,rec);
				   }else{
					   log.warn("packet is null!");
				   }
				//}
			}
			
		};
    };
	}
		
	/**
	 * update cache
	 * @param rec: key header
	 */
	protected void cacheUpdate(String topic, byte[] rec) {
		// TODO Auto-generated method stub
		try{
			log.debug("old: "+buffMap.size());
			//tobeOverflow
			if(BuffMapisOverflowing()){
				//publish old
				put2BuffMap(rec);
				publishEntry(topic);
				//add new 
				
			}else{
				put2BuffMap(rec);
			 }
			log.debug("new: "+buffMap.size());
					
		}catch(Exception e){
			//full
			publishEntry(topic);
			//then push
			put2BuffMap(rec);
		}
		 
	}

	/**
	 * put to buff map
	 * @param rec
	 * @return
	 */
	public boolean put2BuffMap( byte[] rec){
		/**
		 * update
		 */
		
		Key key = new Key(rec);
		
		if(buffMap.containsKey(key)){
			int val = buffMap.get(key)+1;
			buffMap.replace(key, val);
		}else{
			buffMap.put(key, 1);
		}
		return true;
	}
	
	/**
	 * batch flow records
	 * @return
	 */
	private boolean BuffMapisOverflowing() {
		// TODO Auto-generated method stub
		if(buffMap.size()<=publishThreshold ){
			return false;
		}else{
			return true;
		}
	}



	/**
	 * publish
	 * @param buff2
	 */
	private void publishEntry(String topic) {
		// TODO Auto-generated method stub			
		AsyncPush(topic);
				
	}

	/**
	 * return a cache of received packets
	 * @return
	 */
	private byte[] obtainPackets4Source(Packet packet){
		//filter IPV6
		if(packet.contains(IpPacket.class)){
			
			IpPacket ipPkt = packet.get(IpPacket.class);
			IpHeader ipHeader = ipPkt.getHeader();
		
			byte[] src=ipHeader.getSrcAddr().getAddress();
			byte[] dest = ipHeader.getDstAddr().getAddress();
			//source
		if(LogicController.headerChoice==1){
			return src;
			
			
		}else if(LogicController.headerChoice==2){
		//source dest
			return ByteArrays.concatenate(src, dest);
			
		}else if(LogicController.headerChoice==3){
		//3: source source port
			if(packet.contains(TcpPacket.class)){
				TcpPacket tcpPkt = packet.get(TcpPacket.class);
				TcpHeader theader = tcpPkt.getHeader();
				byte[] port = theader.getSrcPort().valueAsString().getBytes();
				return ByteArrays.concatenate(src,port);
			}else if(packet.contains(UdpPacket.class)){
			UdpPacket udpPacket = packet.get(UdpPacket.class);
				UdpHeader uh = udpPacket.getHeader();
				byte[] port = uh.getSrcPort().valueAsString().getBytes();
				return ByteArrays.concatenate(src,port);
			}
			else{
				return ByteArrays.concatenate(src, dest);
			}			
			
		}else if(LogicController.headerChoice==4){
			//4: dest dest port	
			if(packet.contains(TcpPacket.class)){
				TcpPacket tcpPkt = packet.get(TcpPacket.class);
				TcpHeader theader = tcpPkt.getHeader();
				byte[] port = ByteArrays.toByteArray(theader.getDstPort().valueAsInt());
				return ByteArrays.concatenate(dest,port);
			}else if(packet.contains(UdpPacket.class)){
			UdpPacket udpPacket = packet.get(UdpPacket.class);
				UdpHeader uh = udpPacket.getHeader();
				byte[] port = uh.getDstPort().valueAsString().getBytes();
				return ByteArrays.concatenate(dest,port);
			}
			
			else{
				return ByteArrays.concatenate(src, dest);
			}
		}else if(LogicController.headerChoice==5){
			 
			if(packet.contains(TcpPacket.class)){
							
				TcpPacket tcpPkt = packet.get(TcpPacket.class);
				TcpHeader theader = tcpPkt.getHeader();
				
				byte[] a = ByteArrays.concatenate(src,dest);
				  //byte[] ipHeaderBytes = ByteArrays.concatenate(a,new byte[]{ipHeader.getProtocol().value().byteValue()});
				  
				  byte[]  tVal =ByteArrays.concatenate(theader.getSrcPort().valueAsString().getBytes(),theader.getDstPort().valueAsString().getBytes());
				  
				//src,dst,protocol,
					//Key  key = new Key(ByteArrays.concatenate(ipHeaderBytes,tVal));
					
					//source
					//return new byte[](key,1);
				  return ByteArrays.concatenate(a,tVal);
				  
			}else if(packet.contains(UdpPacket.class)){
				
				UdpPacket tcpPkt = packet.get(UdpPacket.class);
				UdpHeader theader = tcpPkt.getHeader();
	
	byte[] a = ByteArrays.concatenate(src,dest);
	  //byte[] ipHeaderBytes = ByteArrays.concatenate(a,new byte[]{ipHeader.getProtocol().value().byteValue()});
	  
	  byte[]  tVal =ByteArrays.concatenate(theader.getSrcPort().valueAsString().getBytes(),theader.getDstPort().valueAsString().getBytes());
	  
	//src,dst,protocol,
		//Key  key = new Key(ByteArrays.concatenate(ipHeaderBytes,tVal));
		
		//source
		//return new byte[](key,1);
	  return ByteArrays.concatenate(a,tVal);	  
}
			
			else{
				return ByteArrays.concatenate(src,dest);
			}
			
		}else{
			return ByteArrays.concatenate(src,dest);
		}
	}else{
		return null;}
	}
	

	
	
	/**
	 * push to the message bus, in batch
	 */
	private void AsyncPush(String topic){
				            	
	            	/**
	            	 * output a map, delete existed items
	            	 */
	            	//Map<Key, Integer> deduplicatedBuff = mergeDuplicatedItemsAndDelte(buff);
	            	
	            	//not in, create, delay
	            	if(!producerMap.containsKey(topic)){
	            		try {
							CreateProducer(topic,LogicController.batchMessagesMaxMessagesPerBatch);
						} catch (InterruptedException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						} catch (ExecutionException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						}	            		 
	            	}
	            	log.info("#publish"+topic+", "+buffMap.size());
	            	Producer<byte[]> producer = producerMap.get(topic);
	            	/**
	            	 * init set, copy to hash set
	            	 */
	            	//Set<Key> Keys = Sets.newConcurrentHashSet(buffMap.keySet());
	            	
	            	 
	            	for (Key key : buffMap.keySet()){
	            		  
	            		 //if(buffMap.containsKey(key)){
	            		 
	            		 
	            		 if(buffMap.containsKey(key)){
	            			 Integer val = buffMap.get(key);
	            			 //clear,skip
	            			 if(val<=0){buffMap.remove(key);continue;}
	            			 //concenate
	            			 byte[] out = createKVByte(key,val);
	            			 
	            			//get producer
			    			 
			    			 //byte[] tempTable = new byte[](buffMap);
		                        producer.sendAsync(out).thenRun(() -> {
			                            messagesSent.increment();
			                            bytesSent.add(out.length);
			                            //update
			                            TopicMap.replace(topic,  TopicMap.get(topic)+out.length);	
			                            
			                            //delete
			                            rescaleBufferMap(buffMap,key,val);
			                            
			                            //CheckNumberPacketsEnoughEndMessageAndSend(topic);
			                            //buffMap.remove(key);
			                            //remove from the set
			                            //ier.remove();
			                            		                           		                            
			                        }).exceptionally(ex -> {
			                            log.warn("Write error on message", ex);
			                            //System.exit(-1);
			                            return null;
			                        });
	            		 }
	            		 }
	            		 
	            	 }                    
	                
	            
	
	/**
	 * create kv bytes
	 * @param key, flow id, 
	 * @param val, 4 bytes,
	 * @return
	 */
	public static byte[] createKVByte(Key key, Integer val) {
		// TODO Auto-generated method stub
		byte[] out = ByteArrays.concatenate(key.getBytes(), ByteArrays.toByteArray(val));
		return out;
	}

	/**
	 * assume: same order
	 * @param kvBytes
	 * @return
	 */
	public static byte[] getKey4KVBytes(byte[] kvBytes){
		byte[] key = new byte[kvBytes.length-ByteArrays.INT_SIZE_IN_BYTES];
		System.arraycopy(kvBytes, 0, key, 0, key.length);
		return key;
	}
	
	/**
	 * assume same order
	 * @return
	 */
	public static int getValue4KVBytes(byte[] kvBytes){
		//byte[] key = new byte[ByteArrays.INT_SIZE_IN_BYTES];
		//System.arraycopy(kvBytes, kvBytes.length-ByteArrays.INT_SIZE_IN_BYTES,key, 0, key.length);
		return ByteArrays.getInt(kvBytes, kvBytes.length-ByteArrays.INT_SIZE_IN_BYTES,ByteArrays.INT_SIZE_IN_BYTES);
		
	}
	/**
	 * get integer
	 * @param kvBytes
	 * @param offset
	 * @return
	 */
	public static int getIntegerValue4KVBytes(byte[] kvBytes,int offset){
		//byte[] key = new byte[ByteArrays.INT_SIZE_IN_BYTES];
		//System.arraycopy(kvBytes, offset,key, 0, key.length);
		return ByteArrays.getInt(kvBytes, offset,ByteArrays.INT_SIZE_IN_BYTES);
		
	}

	/**
	 * update
	 * @param buffMap2
	 * @param tempTable
	 */
	private void rescaleBufferMap(Map<Key, Integer> buffMap2, Key key, Integer val) {
		// TODO Auto-generated method stub
		//Iterator<Entry<Key, Integer>> ier = tempTable.fullRecords.entrySet().iterator();
		//while(ier.hasNext()){
			//Entry<Key, Integer> tmp = ier.next();
		//remove succeeded
		//buffMap2.remove(key);
		if(buffMap2.containsKey(key)){
			int valMe = buffMap2.get(key);
			if(valMe==val){
				buffMap2.remove(key);
			}else{
				buffMap2.replace(key, valMe-val);
			}
		}
		
			//buffMap2.replace(key, buffMap2.get(key)-val);
		//}
	}



	/**
	 * merge duplicated items, use hash map to deduplicate,
	 * @param buff2
	 * @return
	 */
	private Map<Key, Integer> mergeDuplicatedItemsAndDelte0(simpleRingBuffer<byte[]> buff2) {
		// TODO Auto-generated method stub
		//Iterator<byte[]> ier0 = buff2.iterator();	            	
    
//		Map<Key, Integer> deduplicatedMap = Maps.newLinkedHashMap();
//    	                       	
//		while(!buff2.isEmpty()){
//			//delete
//			byte[] out = buff2.pop();
//			//push to the hash table
//			if(!deduplicatedMap.containsKey( out.key)){
//				deduplicatedMap.put( out.key, out.counter);	  
//			}else{
//				deduplicatedMap.replace(out.key, out.counter+deduplicatedMap.get(out.key));
//			}
//		}
//		return deduplicatedMap;
		return null;
	}

	/**
	 * 
	 * @param next
	 * @return
	 */
	private byte[] getMyKeyValueSchema0(Entry<Key, Integer> next) {
		// TODO Auto-generated method stub
		//return  new byte[](next.getKey(),next.getValue());
		return null;
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
	        
        		
	        
				// schedule the next interval end info
				//IntervalEndTimeout = timer.newTimeout(IntervalEndAndSendTask
					//	, TimeUnit.MILLISECONDS.toMillis(intervalPeriod)
						//, TimeUnit.MILLISECONDS);     
	}
	
	/**
	 * create one producer
	 * @param topic
	 * @param batchTimeMillis
	 * @throws InterruptedException
	 * @throws ExecutionException
	 */
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
	
	/**
	 * collector
	 */

	/**
	 * handler to pcap
	 */
    
    

    
	//pcap
	private boolean openPcap(){
		
		
		byte[] nodeA = NetUtil.stringIPToByteIP(LogicController.myDNSAddress);
		
		   /***************************************************************************
	     * Fouth we create a packet handler which receives packets and tells the 
	     * dumper to write those packets to its output file
	     **************************************************************************/
	   
		boolean result = false;
		
		int snaplen = 64 * 1024;           // Capture all packets, no trucation
	    int timeout = 10 * 1000;           // 10 seconds in millis

	    PcapNetworkInterface card=null;
	    
	    		List<PcapNetworkInterface> tmp;
				try {
					tmp = Pcaps.findAllDevs();
					POut.toString(tmp);
		    		if(!tmp.isEmpty()){
		    			Iterator<PcapNetworkInterface> ier = tmp.iterator();
		    			while(ier.hasNext()){
		    				PcapNetworkInterface nxt = ier.next();
		    				log.info(nxt.getName());
		    				
		    				Iterator<PcapAddress> ierPcap = nxt.getAddresses().iterator();
		    				while(ierPcap.hasNext()){
		    					byte[] addr = ierPcap.next().getAddress().getAddress();
		    					log.info(NetUtil.byteIPAddrToString(addr));
		    					
		    					if(Arrays.equals(addr,nodeA)){
		    						try {
		    							card = nxt;
		    								
		    							 
		    							
										dumpHandler =nxt.openLive(snaplen, PromiscuousMode.PROMISCUOUS,  timeout);
										
										//prog = dumpHandler.compileFilter(
		    							//	       filterProg, BpfCompileMode.OPTIMIZE, PcapHandle.PCAP_NETMASK_UNKNOWN
		    							//	      );
		    							//dumpHandler.setFilter(prog);
		    							
										//prog = dumpHandler.compileFilter(
										//        "tcp", BpfCompileMode.OPTIMIZE, PcapHandle.PCAP_NETMASK_UNKNOWN
										//	      );
										
										//dumpHandler.setFilter(prog);;
										
										result = dumpHandler.isOpen();
										break;
									} catch (Exception e) {
										// TODO Auto-generated catch block
										e.printStackTrace();
									}
		    					}
		    				}
		    				 
		    			}
		    		}
				} catch (PcapNativeException e1) {
					// TODO Auto-generated catch block
					e1.printStackTrace();
				}
		if(card!=null){		
			log.info("selected card: "+card.getName());
		}
	    return result;
	}
	
	/**
	 * byteBuffer
	 * @param ofile
	 * @param count
	 */
	public void pcapStart(int count){
   	    		 	
	    /***************************************************************************
	     * Fifth we enter the loop and tell it to capture  packets.
	     **************************************************************************/

	    try {
	    	
			dumpHandler.loop(count, listener);
		} catch (PcapNativeException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (NotOpenException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}
	
	public void close(){
          
/***************************************************************************
* Last thing to do is close the dumper and pcap handles
**************************************************************************/   
		dumpHandler.close();
	}
	
	/**
	 * stop
	 * call the end point, stop the measurement
	 */
	public void terminateCollect(){
			try {
				dumpHandler.breakLoop();				
				dumpHandler.close();
				log.info("terminate!");
				 
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}	
			

	}

	/**
	 * write
	 * @throws Exception
	 */
	private void writePacketStream2Pulsar(BufferedInputStream is,Producer<byte[]> producer) throws Exception{
		//Producer<byte[]> producer = test.producerMakeSync();
		
		//
		int headerLen = 13;
		int fourByte = 4;
		byte[] bufferHeader = new byte[headerLen];
		
		int count = ByteStreams.read(is, bufferHeader,0,bufferHeader.length);
		
		//count=is.read(bufferHeader);
		while(count == headerLen){
			
			byte[] source = new byte[fourByte];
			System.arraycopy(bufferHeader, 0, source, 0, fourByte);
			 //send asyn
			producer.send(source);
			
			count = ByteStreams.read(is, bufferHeader,0,bufferHeader.length);
		}
	}
	
	/**
	 * main
	 * @param args
	 */
	public static void main(String[] args){
		
		Pub4PCapStreamKVTable  one = new Pub4PCapStreamKVTable();
		String topic = LogicController.myDNSAddress;
		
		try {
			
			//start       	
			one.CreateProducer(topic,LogicController.batchingMaxPublishDelay);
			//publish
			one.startPublishPcap(topic);
			//loop
			one.pcapStart(Integer.MAX_VALUE);
			
		} catch (InterruptedException | ExecutionException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		
	}
	
}
