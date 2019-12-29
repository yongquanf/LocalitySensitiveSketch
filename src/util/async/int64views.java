package util.async;

public class int64views {

			 long as_int64; 
			int[] as_int32s = new int[2];
			short[] as_int16s = new short[4];
			char[] as_int8s = new char[8];
			 
			int64views(){
				  as_int64 = 0;
				  for(int i=0;i<2;i++){as_int32s[i]=0;}
				  for(int i=0;i<3;i++){as_int16s[i]=0;}
				  for(int i=0;i<7;i++){as_int8s[i]=0;}
			  }
			  
			 void shiftRecord(){
				  as_int32s[0]= (int) (as_int64 & 0xFFFFFFFF);
				  as_int32s[1]= (int) ((as_int64 >> 32) & 0xFFFFFFFF);
				  
				  as_int16s[0] = (short) (as_int32s[0] & 0xFFFF);
				  as_int16s[1] = (short) ((as_int32s[0] >> 16) & 0xFFFF);			  			  
				  as_int16s[2] = (short) (as_int32s[1] & 0xFFFF);
				  as_int16s[3] = (short) ((as_int32s[1] >> 16) & 0xFFFF);	
				  
				  as_int8s[0] = (char) (as_int16s[0] & 0xFF);
				  as_int8s[1] = (char) ((as_int16s[0] >> 8) & 0xFF);	
				  
				  as_int8s[2] = (char) (as_int16s[1] & 0xFF);
				  as_int8s[3] = (char) ((as_int16s[1] >> 8) & 0xFF);
				  
				  as_int8s[4] = (char) (as_int16s[2] & 0xFF);
				  as_int8s[5] = (char) ((as_int16s[2] >> 8) & 0xFF);
				  
				  as_int8s[6] = (char) (as_int16s[3] & 0xFF);
				  as_int8s[7] = (char) ((as_int16s[3] >> 8) & 0xFF);
			  }
		 

	 
}
