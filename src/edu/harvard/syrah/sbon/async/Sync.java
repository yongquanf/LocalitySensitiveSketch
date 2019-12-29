/*
 * @author Last modified by $Author: ledlie $
 * @version $Revision: 1.3 $ on $Date: 2009/04/23 20:55:47 $
 * @since Jun 21, 2006
 */
package edu.harvard.syrah.sbon.async;

import edu.harvard.syrah.prp.Log;
import edu.harvard.syrah.sbon.async.CallbacksIF.CB0;

import java.util.concurrent.Executors;

public class Sync {
  private static final Log log = new Log(Sync.class);
  
  private static int threadCounter = 0;

	static final java.util.concurrent.ScheduledExecutorService scheduler =
		Executors.newScheduledThreadPool(10);

  
  /*
   * TODO convert this to use a thread pool
   */
  
  public static void callBlocking(final CB0 cbBlocking, final CB0 cbDone) {
    final Thread blockingThread = new Thread("BlockingThread-" + (++threadCounter)) {

      @Override
      public void run() {
        super.run();
        
        try {
        	cbBlocking.callOK();
        } catch (final Error e) {
        	// Return any exceptions
          EL.get().registerTimerCB(new CB0() {
            protected void cb(CBResult result) { cbDone.call(CBResult.ERROR(new Exception(e))); }
          });
          return;
        }
                
        EL.get().registerTimerCB(new CB0() {
          protected void cb(CBResult result) { cbDone.call(result); }          
        });        
      }      
    };
    
    //blockingThread.run();    

		scheduler.execute (blockingThread);

  }
}
