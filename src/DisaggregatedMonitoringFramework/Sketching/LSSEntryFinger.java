package DisaggregatedMonitoringFramework.Sketching;

import org.pcap4j.util.ByteArrays;

	/**
	 * entry
	 * @author quanyongf
	 *
	 */
	public class LSSEntryFinger{
		//number of flows
		int counter=0;
		//sum of flow counts
		int sum=0;
		/**
		 * to bytes
		 * @return
		 */
		public byte[] toByteArray(){
			return ByteArrays.concatenate(ByteArrays.toByteArray(counter), ByteArrays.toByteArray(sum));
		}

		

		
		public LSSEntryFinger(){
			counter=0;
			sum=0;		
		}
		
		
		public LSSEntryFinger(int[] val) {
			// TODO Auto-generated constructor stub
			this.counter=val[0];
			this.sum = val[1];
		}




		public int getCounter() {
			return counter;
		}
		public float getSum() {
			return sum;
		}
		
		
		/**
		 * 
		 * @param flowCount
		 * @param currentClusterCenter
		 * @param nextclusterCenter
		 * @return
		 */
		public boolean nearer2Next(int flowCount,double currentClusterCenter, double nextclusterCenter){
			if(Math.abs(flowCount-currentClusterCenter)>Math.abs(flowCount-nextclusterCenter)){
				return true;
			}else{
				return false;
			}
		}
		
		
		
		/**
		 * estimator
		 * @return
		 */
		public float getAvgEstimator() {
			// TODO Auto-generated method stub
			if(this.counter>0){
				float val1= this.sum/(0.0f + this.counter);
				
					return val1;
			
			}else{
				return 0;
			}
		}
		


		/**
		 * copy
		 * @return
		 */
		public LSSEntryFinger copy() {
			// TODO Auto-generated method stub
			LSSEntryFinger now = new LSSEntryFinger();
			now.counter= this.counter;
			now.sum= this.sum;
			return now;
		}
		/**
		 * add to the entry
		 * @param rawVal
		 * @param updateCounter
		 */
		public void addEntry(int rawVal, boolean updateCounter) {
			// TODO Auto-generated method stub
			if(updateCounter){
				this.sum+=rawVal;
				this.counter+=1;
			}else{
				this.sum+=rawVal;
			}
		}
		/**
		 * delete entry
		 * @param newValResult
		 */
		public void deleteEntry(int newValResult) {
			// TODO Auto-generated method stub
			this.counter--;
			this.sum-=newValResult;
		}
		
		public int byteNum() {
			// TODO Auto-generated method stub
			return ByteArrays.INT_SIZE_IN_BYTES*2;
		}




		public void clear() {
			// TODO Auto-generated method stub
			this.sum=0;
			this.counter=0;
		}
	}
