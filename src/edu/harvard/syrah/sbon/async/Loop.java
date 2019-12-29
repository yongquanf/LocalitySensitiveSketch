/*
 * @author Last modified by $Author: prp $
 * @version $Revision: 1.1 $ on $Date: 2007/06/04 17:55:41 $
 * @since Aug 11, 2005
 */
package edu.harvard.syrah.sbon.async;

import edu.harvard.syrah.prp.Log;
import edu.harvard.syrah.sbon.async.CallbacksIF.CB0;
import edu.harvard.syrah.sbon.async.CallbacksIF.CB1;
import edu.harvard.syrah.sbon.async.CallbacksIF.CB2;
import edu.harvard.syrah.sbon.async.EL.Priority;

public class Loop<T> {
	protected static final Log log = new Log(Loop.class);

	private String name;
	protected CB2<T, CB1<T>> cbRecursion;
	private Priority priority;
	private T startItem;

	protected Barrier loopBarrier;
	
	public Loop(String name, T item, CB2<T, CB1<T>> cbRecursion) {
		this(name, item, cbRecursion, Priority.NORMAL);
	}

	public Loop(String name, T item, CB2<T, CB1<T>> cbRecursion, Priority priority) {
		this.name = name;
		this.cbRecursion = cbRecursion;
		this.startItem = item;
		this.priority = priority;
		this.loopBarrier = new Barrier(false);
	}

	public Barrier execute() {

		recurse(startItem);

		/*
		 * EventLoop.get().registerTimerCB(new EventCB(name) { protected void
		 * cb(CBResult result, Event timerEvent) { recurse(startItem); } },
		 * priority);
		 */

		return loopBarrier;
	}

	public void recurse(final T item) {
		log.debug("Forking barrier=" + loopBarrier);
		loopBarrier.fork();
		log.debug("Forked barrier=" + loopBarrier);
		EL.get().registerTimerCB(new CB0(name) {
			protected void cb(CBResult result) {
				cbRecursion.call(CBResult.OK(), item, new CB1<T>() {
					protected void cb(CBResult result, T item) {
						if (item != null)
							recurse(item);
						else {
							log.debug("Joining barrier" + loopBarrier);
							loopBarrier.join();
							log.debug("Joined barrier" + loopBarrier);
						}
					}
				});
			}
		}, priority);
	}

}
