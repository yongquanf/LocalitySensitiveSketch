/*
 * @author Last modified by $Author: prp $
 * @version $Revision: 1.1 $ on $Date: 2007/06/04 17:55:41 $
 * @since Jan 28, 2005
 */
package edu.harvard.syrah.sbon.async;

import edu.harvard.syrah.prp.Log;
import edu.harvard.syrah.sbon.async.CallbacksIF.CB0;
import edu.harvard.syrah.sbon.async.CallbacksIF.CB0R;
import edu.harvard.syrah.sbon.async.EL.Priority;


/**
 *
 * Allows us to synchronize callbacks
 *
 */
public class Barrier {
	private static final Log log = new Log(Barrier.class);
	
	private static final long PREDICATE_INTERVAL = 1000;
	
	private boolean triggered = false;
	private boolean startState;
	private boolean activated = true;
	
	private int count;	
	private CB0 cb;
	private long delay;	
	protected CB0 userCB;
	
	public Barrier(boolean startState) { this(startState, 0, null); }
		
	public Barrier(int count) {	this(count == 0 ? true : false, count, null); }
	
	public Barrier(int count, CB0 cb) { this(count == 0 ? true : false, count, cb);	}
	
	public Barrier(final CB0R<Boolean> cbPredicate) {
		this.startState = false;
		this.count = 1;
		this.triggered = startState;
		
		EL.get().registerTimerCB(new CB0() {
			protected void cb(CBResult resultOK) {
				if (cbPredicate.call(resultOK))
					Barrier.this.join();
				else
					EL.get().registerTimerCB(PREDICATE_INTERVAL, this);
			}			
		});
	}
	
	private Barrier(boolean startState, int count, CB0 cb) {
		this.startState = startState;
		this.count = count;
		this.cb = cb;		
		this.triggered = startState;
	}	
	
	public void activate() { activated = true; }
	
	public void deactivate() { activated = false; }
	
	public boolean isActive() { return activated; }
	
	public void fork() {
		assert startState || !triggered || !activated : "Could not fork: startState=" + startState + " triggered=" 
			+ triggered + " activated=" + activated;
	
		startState = false;
		count++;
		triggered = false;
	}
	
	public void join() {
		assert startState || !triggered || !activated : "Could not join: startState=" + startState + " triggered=" 
			+ triggered + " activated=" + activated;
	
		startState = false;
		count--;
		
		if (count == 0 && activated) {			
			triggered = true;
			
			if (cb != null) {
				log.debug("Barrier triggered.");
				// Register the event callback
				EL.get().registerTimerCB(delay, cb);
			}
			
			if (userCB != null) {
				EL.get().registerTimerCB(new CB0() {
					public void cb(CBResult result) {
						userCB.cb(CBResult.OK());
					}					
				});				
			}			
		}
	}
		
	/*
	 * added by glp
	 */
	public void setNumForks(int newCount){ 
		this.count = newCount; 
		if (newCount > 0) {
			triggered = false;
		}
	}
	
	public void remove() { log.debug("Barrier removed."); }
		
	public void registerCB(CB0 myUserCB) {
		this.userCB = myUserCB;
		
		if (triggered) {
			EL.get().registerTimerCB(new CB0() {
				public void cb(CBResult result) {
					userCB.cb(result);
				}				
			});
		}
	}
	
	void registerTimerCB(long delay, CB0 cb, Priority priority) {
		this.cb = cb;
		this.delay = delay;
		if (triggered) {
			log.debug("Barrier triggered.");
			// Register the event callback
			EL.get().registerTimerCB(delay, cb, priority);			
		}
	}
	
	public String toString() {
		return "count=" + count + " triggered=" + triggered;
	}
	
}
