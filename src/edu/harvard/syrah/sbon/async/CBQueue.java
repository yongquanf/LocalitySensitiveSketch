/*
 * @author Last modified by $Author: prp $
 * @version $Revision: 1.1 $ on $Date: 2007/06/04 17:55:41 $
 * @since Nov 8, 2005
 */
package edu.harvard.syrah.sbon.async;

import java.util.LinkedList;
import java.util.List;

import edu.harvard.syrah.prp.Log;
import edu.harvard.syrah.sbon.async.CallbacksIF.CB0;
import edu.harvard.syrah.sbon.async.CallbacksIF.CB1;


public class CBQueue {
  private static final Log log = new Log(CBQueue.class);
    
  private static final long RETRY_INTERVAL = 1000;
  
  private int maxCBs;
  
  private List<CB1<CB1<Boolean>>> waitingCBs = new LinkedList<CB1<CB1<Boolean>>>();
  
  private int outstandingCBs = 0;
  
  public CBQueue(int maxCBs) { this.maxCBs = maxCBs; }
  
  public void enqueueDequeue(final CB1<CB1<Boolean>> newCB) {
    //log.debug("outstandingCBs=" + outstandingCBs + " maxCBs=" + maxCBs + " waitingCBs.size=" + waitingCBs.size());

    if (newCB != null)
      waitingCBs.add(newCB);
    
    if ((maxCBs == 0 || outstandingCBs < maxCBs) && !waitingCBs.isEmpty()) {
      
      final CB1<CB1<Boolean>> cb = waitingCBs.remove(0);
      outstandingCBs++;      
      cb.call(CBResult.OK(), new CB1<Boolean>() {
        protected void cb(CBResult result, Boolean handled) {
          outstandingCBs--;
          if (handled) {
            dequeue();
          } else {
            waitingCBs.add(0, cb);
            
            EL.get().registerTimerCB(RETRY_INTERVAL, new CB0() {
              protected void cb(CBResult result) {
                dequeue();
              }              
            });
          }
        }
      });        
    }
  }
  
  public void dequeue() { enqueueDequeue(null); }
  
}
