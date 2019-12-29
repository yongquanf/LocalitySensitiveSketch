package util.bloom.Apache;


/** CRC32
* A class for calculating Cyclic Redundancy Checksum's (CRC-32)
* @author Eddy L O Jansson
* @author http://hem2.passagen.se/eddy1/
* @version 0.1.0
*/
public class CRC32{
 private static final int[] CRCtable = new int[256];
 
 //private static final int CRCseed = 0xFFFFFFFF;
 private static int CRCseed = 0xFFFFFFFF;
 
 private static final int CRCpoly = 0xEDB88320;
 private int CRCvalue = CRCseed;

  static{                          // static initialization, run only once.
    int value;                     // calculate crc-table based on polynome.
    for (short x=0;x<256;x++){
      value=x;
      for (byte y=1;y<9;y++)
       value=((value & 1)==1) ? value>>>1^CRCpoly : value>>>1;
      CRCtable[x]=value;
    }
  }
  
  
  public static CRC32 instance=null;
  
  public static CRC32 getInstance(){
	  if(instance==null){
		  instance=new CRC32();
	  }	  
		  return instance;	  
  }

  /**
   * set the seed
   * @param val
   */
  public void setSeed(int val){
	  CRCseed=val;
  }
  /**
   * 
   * @param s
   * @param seed
   * @return get the checksum
   */
  public int getValue(String s, int seed){
	  CRCseed=seed;
	  update(s);
	  return value();
  }
  
  
  /** Reset CRC value to seed
  * @param args none
  * @return CRC32 object
  * @exception exceptions No exceptions thrown
  */
  public CRC32 reset()
  {
    CRCvalue = CRCseed;
    return this;
  }
  /** Update CRC from string
  * @param args A string
  * @return CRC32 object
  * @exception exceptions No exceptions thrown
  */
  public CRC32 update(String s)
  {
    for (int i=0;i<s.length();i++)
     CRCvalue=((CRCvalue>>>8) & 0xFFFFFF)^CRCtable[(CRCvalue^s.charAt(i))&255];
    return this;
  }

  public CRC32 update(byte[] b)
  {
    for (int i=0;i<b.length;i++)
     CRCvalue=((CRCvalue>>>8) & 0xFFFFFF)^CRCtable[(CRCvalue^b[i])&255];
    return this;
  }

  public CRC32 update(byte b)
  {
    CRCvalue=((CRCvalue>>>8) & 0xFFFFFF)^CRCtable[(CRCvalue^b)&255];
    return this;
  }

  public CRC32 update(int i)
  {
    CRCvalue=((CRCvalue>>>8) & 0xFFFFFF)^CRCtable[(CRCvalue^(byte)i)&255];
    return this;
  }

  /** Return current CRC32
  * @param args none
  * @return int
  * @exception exceptions No exceptions thrown
  */
  public int value()
  {
    return CRCvalue^CRCseed;
  }

} // end CRC32 class
