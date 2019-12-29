/*
 * SBON
 * 
 * @author Last modified by $Author: prp $
 * @version $Revision: 1.1 $ on $Date: 2007/06/04 17:55:40 $
 * @since Jan 7, 2005
 */
package edu.harvard.syrah.sbon.async.comm.obj;

import edu.harvard.syrah.prp.Log;

/**
 * 
 * Abstract representation of a message
 * 
 */
public abstract class ObjMessage implements ObjMessageIF {
  private static final Log log = new Log(ObjMessage.class);

  static final long serialVersionUID = 1000000001L;

  // Message id for the message
  private Long messageId = null;

  private boolean isResponse = false;

  private boolean isError = false;	

  public boolean hasMsgId() { return messageId != null; }

  public int hasRepeated=0;
  
  
  public long getMsgId() {
	if (messageId == null)
	  log.error("Accessed messageId for a message that doesn't have one.");
	return messageId;
  }

  public void setMsgId(long messageId) { this.messageId = messageId; }

  public void setResponse(boolean isResponse) { this.isResponse = isResponse; }

  public boolean isResponse() { return isResponse; }

  public boolean isError() { return isError; }

  public void setError(boolean isError) { this.isError = isError; }	
}
