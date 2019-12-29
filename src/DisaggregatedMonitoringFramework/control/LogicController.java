package DisaggregatedMonitoringFramework.control;

import org.apache.pulsar.client.api.SubscriptionType;

import edu.harvard.syrah.sbon.async.Config;

/**
 * logic controller of ingest, sketching and query
 * @author quanyongf
 *
 */
public class LogicController {

	
	static {

		// All config properties in the file must start with 'HSHer.'

		Config.read("LSS", System
				.getProperty("LSS.config", "conf/LSS.cfg"));
	}
	
	/**
	 * interval end
	 */
	//public static final long intervalPeriod = Long.parseLong(Config.getConfigProps()
	//		.getProperty("intervalPeriod", "1000"));
	
	public static final long NumberFlowsPerPeriod = Long.parseLong(Config.getConfigProps()
			.getProperty("NumberFlowsPerPeriod", "10000"));
	/**
	 * service url
	 */
	public static final String serviceURL=Config.getConfigProps()
			.getProperty("serviceURL", "pulsar://127.0.0.1:6650");

	/**
	 * cluster method
	 */
	public static final int clusterArrayChoiceMethod= Integer.parseInt(
			Config.getConfigProps()
			.getProperty("clusterArrayChoiceMethod", "1")
			);
	
	 
	public static final int batchMessagesMaxMessagesPerBatch = Integer.parseInt(
			Config.getConfigProps()
			.getProperty("batchMessagesMaxMessagesPerBatch", "10000")
			);
	
	public static int headerChoice = Integer.parseInt(
			Config.getConfigProps()
			.getProperty("headerChoice", "1")
			);
	 
	public static final  float scaleBuff = Float.parseFloat(
			Config.getConfigProps()
			.getProperty("scaleBuff", "0.1"));
	
	public static final  int batchingMaxPublishDelay  = Integer.parseInt(
			Config.getConfigProps()
			.getProperty("batchingMaxPublishDelay", "1000")
			);
	
	//my Ip address
			public static final String myDNSAddress = Config.getConfigProps()
					.getProperty("myDNSAddress", "");//.split("[\\s]");

    /**
     * service url
     */
   // public static final String serviceURL=Config.getConfigProps()
    //.getProperty("serviceURL", "pulsar://127.0.0.1:6650");
    
    
    public static final String[] kvTopics = Config.getConfigProps()
    .getProperty("kvTopics", " ").split("[\\s]");
    
    
    /**
     * cluster number
     */
    public static final int clusterCount  =Integer.parseInt(Config.getConfigProps()
                                                            .getProperty("clusterCount", "8"));
    
    /**
     * number of total buckets
     */
    public static final int bucketCount =Integer.parseInt(Config.getConfigProps()
                                                          .getProperty("bucketCount", "1000"));
    
    /**
     * query
     */
    public static final  float expectedFP = Float.parseFloat(Config.getConfigProps()
                                                             .getProperty("expectedFP", "0.001"));
    
    public static final  String traceName=Config.getConfigProps()
    .getProperty("traceName", "0.dat");
    
    public static int numEntriesPerBucket  =Integer.parseInt(Config.getConfigProps()
                                                             .getProperty("numEntriesPerBucket", "16"));
    
    public static int FingerLen = Integer.parseInt(Config.getConfigProps()
                                                   .getProperty("FingerLen", "16"));
    
    
    public static String WriteLogFileName = Config.getConfigProps()
    .getProperty("WriteLogFileName", "Sub4sketchingKVStream");
    
    public static final int receiverQueueSize = Integer.parseInt(Config.getConfigProps()
                                                                 .getProperty("receiverQueueSize", "10000"));
 
    
    /**
	 * grouping delay ack
	 */
	public static int acknowledgmentsGroupingDelayMillis = Integer.parseInt(Config.getConfigProps()
			.getProperty("acknowledgmentsGroupingDelayMillis", "100"));
	/**
	 * subscribe type
	 */
	public static SubscriptionType subscriptionType =  SubscriptionType.Shared;
	
    
//			/**
//			 * interval end
//			 */
//			//public static final long intervalPeriod = Long.parseLong(Config.getConfigProps()
//			//		.getProperty("intervalPeriod", "1000"));
//			
//			public static final long NumberPacketsPerPeriod = Long.parseLong(Config.getConfigProps()
//					.getProperty("NumberPacketsPerPeriod", "1000000"));
//			/**
//			 * service url
//			 */
//			public static final String serviceURL=Config.getConfigProps()
//					.getProperty("serviceURL", "pulsar://127.0.0.1:6650");
//
//			 
//			public static final int batchMessagesMaxMessagesPerBatch = Integer.parseInt(
//					Config.getConfigProps()
//					.getProperty("batchMessagesMaxMessagesPerBatch", "10000")
//					);
//			 
//			
//			public static final  int batchingMaxPublishDelay  = Integer.parseInt(
//					Config.getConfigProps()
//					.getProperty("batchingMaxPublishDelay", "1000")
//					);
			
	
}
