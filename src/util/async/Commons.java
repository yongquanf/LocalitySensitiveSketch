package util.async;

import java.net.InetAddress;


public class Commons {

	public static int HL = 31;
	public static int MOD = 2147483647;
	
	public static long MAXUINT64 = (long) (-1);
	public static  int MAXUINT32 = (-1);
	public static short MAXUINT16 = (short) (-1);
	public static  char  MAXUINT8  = (char)  (-1);
	public static  double E = (double) (2.71828182846);

	public static  int FIELD_SRCIP = 1;
	public static  int FIELD_DSTIP = 2;

	public static  int UPDATETYPE_SET = 1;
	public static  int UPDATETYPE_INCREMENT = 2;
	public static  int UPDATETYPE_NEXT = 3;

	public static  int ONELEVEL_FUNCTION = 1;
	public static  int TWOLEVEL_FUNCTION = 2;


	public static  int HASHTYPE_DIETZTHORUP32 = 1;
	public static  int HASHTYPE_REVERSIBLE8TO3 = 2;
	
	public static  int REVERSIBLE_NUMROWS = 5;
	public static  int REVERSIBLE_COUNTERSPERROW = 4096;
	public static  int REVERSIBLE_ITERSIZE = 128;//64;
	public static  int REVERSIBLE_NUMDIVS = 4;
	public static  int REVERSIBLE_NUMKEYS = 256;
	public static  int REVERSIBLE_R = 2;
	public static  int REVERSIBLE_BINSPERDIV = 8;
	
	
	// http://en.wikipedia.org/wiki/Universal_hashing is great!
	public static int os_dietz_thorup32(int x, int bins, long a, long b){
	  // ((ax mod 2**62) + b) mod 2 ** 62, hi 32, pad 0 on left
	  // = (ax + b) mod 2 ** 62, hi 32, pad 0 on the left

	  return ((int) ((a*x+b) >> 32)) % bins;
	  // mod 64 then top 32 bits 
	  // (this impl. okay for little endian)
	  // wiki says just ax .. is 2-universal
	  // why is it strongly universal/ 2-wise indep.?
	}

	public static long hash31(long a,  long b, long x)
	{

	   long result;
	  long lresult;  

	  // return a hash of x using a and b mod (2^31 - 1)
	// may need to do another mod afterwards, or drop high bits
	// depending on d, number of bad guys
	// 2^31 - 1 = 2147483647

	  //  result = ((long ) a)*((long ) x)+((long ) b);
	  result=(a * x) + b;
	  result = ((result >> HL) + result) & MOD;
	  lresult=(long) result; 
	  
	  return(lresult);
	}

	long fourwise(long  a, long  b, long  c, long  d, long  x)
	{
	  long  result;
	  long lresult;
	  
	  // returns values that are 4-wise independent by repeated calls
	  // to the pairwise indpendent routine. 

	  result = hash31(hash31(hash31(x,a,b),x,c),x,d);
	  lresult = (long) result;
	  return lresult;
	}

	
	// 2-universal hashing, assumes bins power of 2, very fast
	char os_dietz8to3(char x, char a) {
	  return (char) (((a*x)  >> 5) & 7);
	}
	// mod 8 then top 3 bits


	int reversible4096(int value, int bins, long hasha) {
	  int j, index, tmp; 



	  char a, x;  
	  int64views vv = new int64views();
	  int64views vha = new int64views();
	  //cout << "hashing value " << value << "\n";
	  //cout << "hashing value " << value << "\n";
	  vv.as_int64 = value;
	  vv.shiftRecord();
	  vha.as_int64 = hasha;
	  vha.shiftRecord();
	  index = 0;
	  
	  for (j = 0; j < 4; j++) {
	      x = vv.as_int8s[j];
	      a = vha.as_int8s[j];
	      /*cout << "x: " << (int) (x & 0xFF)\
		<< ", a: " << (int) (a & 0xFF)\
		<< ", b: " << (int) (b & 0xFF) << "\n";*/
	      tmp = os_dietz8to3(x, a) & 0xFF;
	      //cout << "tmp: " << tmp << "\n";
	      tmp = tmp << 3*j;
	      index += tmp;
	    }
	  return index;

	}


	int os_dietz64to32(long x, long a) {
	  if ((~(a%2))>0) a--;
	  //cout << "a*x (lower 64?): " << a*x << "\n";
	  //cout << "a*x & MAXUINT32 (lower32): " << (a*x & (MAXUINT32)) << "\n";
	  //cout << "a*x >> 32 (high32, maybe padded with 1s on right): " << (a*x >> 32) << "\n";
	  //cout << "a*x >> (int) (32  & MAXUINT32 (high32)): " << (int) ((a*x >> 32) & (MAXUINT32)) << "\n";
	  // all work as expected .. but to be safe & MAXUINT32
	  return (int) (((a*x) >> 32) & Integer.MAX_VALUE);
	}


	// ----------- from Reversible Sketch Code at http://www.zhichunli.org/software/download.php?file=RevSketch-1.0.tar.gz 

	double os_current_time()
	{
		long tv=System.currentTimeMillis();

	  return tv;
	}

	// Generates a 32-bit random string
	// Note that rand() is not random enough -- it has a period of 2^32.
	int os_rand32bit()
	{
	  return Integer.valueOf(MathUtil.randString(32));
	}


	public String os_ipint2string(InetAddress a)
	{

	  byte[] ip = a.getAddress();
	  return getIPAddress(ip);

	}
	
	/**
	 * raw to string
	 * @param rawBytes
	 * @return
	 */
	public static String getIPAddress(byte[] rawBytes){
		int i=4;
		String ia="";
		for(byte raw : rawBytes){
			ia += (raw & 0xFF);
			if(--i > 0){
				ia+=".";
			}
		}
		return ia;
	}
}
