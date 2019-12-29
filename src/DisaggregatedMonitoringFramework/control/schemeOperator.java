package DisaggregatedMonitoringFramework.control;

import org.apache.pulsar.client.impl.schema.AvroSchema;

public class schemeOperator {

	static AvroSchema<MyKeyValueSchema> as=null;
	
	/**
	 * singleton
	 * @return
	 */
	public static AvroSchema<MyKeyValueSchema> getSingleton(){
		if(as==null){
		 as = AvroSchema.of(MyKeyValueSchema.class,null);			
		}
		return as;
	}
	
	
	/**
	 * encode
	 * @return
	 */
	public byte[] encode(MyKeyValueSchema val){
				 return as.encode(val);		 
	}
	
	/**
	 * 
	 * @param data
	 * @return
	 */
	public MyKeyValueSchema decode(byte[] data){
		return as.decode(data);
	}
	
	
}
