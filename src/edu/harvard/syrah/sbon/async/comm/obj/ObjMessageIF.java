/*
 * @author Last modified by $Author: prp $
 * @version $Revision: 1.1 $ on $Date: 2007/06/04 17:55:40 $
 * @since Jan 6, 2005
 */
package edu.harvard.syrah.sbon.async.comm.obj;

import java.io.Serializable;


/**
 *
 * This interfaces represents a message that can be sent by the Comm layer.
 *
 */
public interface ObjMessageIF extends Serializable {

  public boolean hasMsgId();

  public long getMsgId();

  void setMsgId(long msgId);

  boolean isResponse();

  void setResponse(boolean isReponse);

  boolean isError();

  void setError(boolean isError);

}
