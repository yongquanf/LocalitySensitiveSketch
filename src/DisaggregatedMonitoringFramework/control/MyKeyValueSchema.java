package DisaggregatedMonitoringFramework.control;
 

import org.apache.pulsar.client.impl.schema.AvroSchema;

import util.bloom.Apache.Key;

public class MyKeyValueSchema {

	public Key key;
	public int counter;
	
	public MyKeyValueSchema(){
		key = null;
		counter = 0;
	}
		
	public MyKeyValueSchema(Key _k, int _c){
		key = _k;
		counter = _c;
	}
	

	public long length() {
		// TODO Auto-generated method stub
		return key.getBytes().length+4;
	}
	
	 @Override
     public int hashCode() {
         return key.hashCode()+counter;
     }

     @Override
     public boolean equals(Object other) {
         return (other instanceof MyKeyValueSchema) && (((MyKeyValueSchema)other).key.equals(key))&&(((MyKeyValueSchema)other).counter==counter);
     }

	public Key getKey() {
		return key;
	}

	public void setKey(Key key) {
		this.key = key;
	}

	public int getCounter() {
		return counter;
	}

	public void setCounter(int counter) {
		this.counter = counter;
	}
	
	
}
