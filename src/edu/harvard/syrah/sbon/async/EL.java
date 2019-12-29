/*	
 * SBON
 * 
 * @author Last modified by $Author: ledlie $
 * @version $Revision: 1.3 $ on $Date: 2009/04/23 20:55:47 $
 * @since Jan 6, 2005
 */
package edu.harvard.syrah.sbon.async;

import static edu.harvard.syrah.sbon.async.EL.Priority.HIGH;
import static edu.harvard.syrah.sbon.async.EL.Priority.LOW;
import static edu.harvard.syrah.sbon.async.EL.Priority.NORMAL;

import java.io.IOException;
import java.nio.channels.*;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.PriorityBlockingQueue;

import edu.harvard.syrah.prp.ANSI;
import edu.harvard.syrah.prp.Log;
import edu.harvard.syrah.prp.POut;
import edu.harvard.syrah.prp.PTimer;
import edu.harvard.syrah.prp.ANSI.Color;
import edu.harvard.syrah.sbon.async.CallbacksIF.*;

/**
 * 
 * Implementation of the main event loop.
 * 
 * This class is modeled after the Java implementation of David Mazieres'
 * libasync library, which was done by Sean Rhea as part of the Bamboo
 * implementation.
 * 
 */
public class EL implements EventLoopIF {
	protected static final Log log = new Log(EL.class);
	
	public int SELECTION_KEY_ALL_OPS = SelectionKey.OP_ACCEPT
			| SelectionKey.OP_CONNECT | SelectionKey.OP_READ | SelectionKey.OP_WRITE;

	// Maximum time that we can spend servicing the event queue without accessing
	// the network
	private static final long MAX_EVENT_QUEUE_PERIOD = 10; //

	static final long MAX_CB_TIME = 0; // was 5ms

	// Maximum size for the event queue
	private static final long EVENT_QUEUE_LIMIT = 1000;

	private static final boolean DEFAULT_SHOW_CB_TIME = true;

	private static final long DEFAULT_STATE_DUMP_INTERVAL = 0;

	private static final boolean DEFAULT_SHOW_IDLE = false;

	private boolean showIdle;
	private boolean showCBTime;
	
	final static int SubThreadCanRun=2;
	final static int MainCanRunCallback=1;
	
	private volatile int CAN_SELECT=SubThreadCanRun; //2 - subThread can run, 1 - main thread run the callback
	
	private volatile boolean token=true;    //enter the callback handler
	

	private static final long NO_SLACKTIME = 0;
	private static final long INFINITE_SLACKTIME = Long.MAX_VALUE;

	public enum Priority {
		HIGH, NORMAL, LOW;
	}

	private Queue<CB0> now_eQ_HP = new ConcurrentLinkedQueue<CB0>();
	private Queue<CB0> now_eQ_NP = new ConcurrentLinkedQueue<CB0>();
	private Queue<CB0> now_eQ_LP = new ConcurrentLinkedQueue<CB0>();

	private Queue<CB0> eQ_HP = new PriorityBlockingQueue<CB0>();
	private Queue<CB0> eQ_NP = new PriorityBlockingQueue<CB0>();
	private Queue<CB0> eQ_LP = new PriorityBlockingQueue<CB0>();

	private Selector selector;

	private Map<SelectableChannel, ChannelSub> channelSubTable = new ConcurrentHashMap<SelectableChannel, ChannelSub>();

	private boolean loopExit = false;
	private boolean forceExit = false;
	private boolean ranShutdown = false;

	private int numSelectorKeys;

	private static EventLoopIF eventLoop = null;

	private int max_eQ_HP;
	private int max_eQ_NP;
	private int max_eQ_LP;

	private int max_now_eQ_HP;
	private int max_now_eQ_NP;
	private int max_now_eQ_LP;

	public static void set(EventLoopIF newEventLoop) {
		eventLoop = newEventLoop;
	}

	public static EventLoopIF get() {
		return eventLoop;
	}

	public EL() {
		this(DEFAULT_STATE_DUMP_INTERVAL, DEFAULT_SHOW_IDLE);
	}

	public EL(final long stateDumpInterval, final boolean showIdle) {

		Thread.currentThread().setPriority(Thread.MAX_PRIORITY);

		this.showIdle = showIdle;
		this.showCBTime = DEFAULT_SHOW_CB_TIME;

		log.debug("Opening the selector...");
		try {
			selector = Selector.open();
		} catch (IOException e) {
			log.error("Could not open selector: " + e);
		}

		/*
		 * TODO
		 * 
		 * This is sometimes not executed on shutdown and I don't understand why.
		 * This might be connected to ant...?
		 */

		// Make sure that we shutdown cleanly
		Runtime.getRuntime().addShutdownHook(new Thread() {
			public void run() {
				EL.this.shutdown();
			}
		});

		if (stateDumpInterval != 0) {
			this.registerTimerCB(stateDumpInterval, new CB0("ELStateDumper") {
				protected void cb(CBResult result) {
					dumpState(true);
					if (!shouldExit())
						EL.this.registerTimerCB(stateDumpInterval, this);
				}
			});
		}
	}

	public void dumpState(boolean eventQueueDump) {
	/*	log.info("now_eQ_HP.size=" + now_eQ_HP.size() + " now_eQ_HP.max="
				+ max_now_eQ_HP
				+ (eventQueueDump ? " now_eq_HP=" + POut.toString(now_eQ_HP) : ""));
		log.info("now_eQ_NP.size=" + now_eQ_NP.size() + " now_eQ_NP.max="
				+ max_now_eQ_NP
				+ (eventQueueDump ? " now_eq_NP=" + POut.toString(now_eQ_NP) : ""));
		log.info("now_eQ_LP.size=" + now_eQ_LP.size() + " now_eQ_LP_max="
				+ max_now_eQ_LP
				+ (eventQueueDump ? " now_eq_LP=" + POut.toString(now_eQ_LP) : ""));

		log.info("eQ_HP.size=" + eQ_HP.size() + " eQ_HP.max=" + max_eQ_HP
				+ (eventQueueDump ? " eq_HP=" + POut.toString(eQ_HP) : ""));
		log.info("eQ_NP.size=" + eQ_NP.size() + " eQ_NP.max=" + max_eQ_NP
				+ (eventQueueDump ? " eq_NP=" + POut.toString(eQ_NP) : ""));
		log.info("eQ_LP.size=" + eQ_LP.size() + " eQ_LP_max=" + max_eQ_LP
				+ (eventQueueDump ? " eq_LP=" + POut.toString(eQ_LP) : ""));

		log.info("cST.size=" + channelSubTable.size() + " cST="
				+ POut.toString(channelSubTable) + " numSelectorKeys="
				+ numSelectorKeys);*/
/*
		log.info("totalMem="
				+ POut.toString(((double) Runtime.getRuntime().totalMemory())
						/ (1024 * 1024))
				+ " MB"
				+ " freeMem="
				+ POut.toString(((double) Runtime.getRuntime().freeMemory())
						/ (1024 * 1024)) + " MB");*/

		// log.main("eQ_NP.peek=" + eQ_NP.remove());
		// log.main("eQ_NP.peek=" + eQ_NP.remove());
		// log.main("eQ_NP.peek=" + eQ_NP.remove());

		max_eQ_HP = eQ_HP.size() > max_eQ_HP ? eQ_HP.size() : max_eQ_HP;
		max_eQ_NP = eQ_NP.size() > max_eQ_NP ? eQ_NP.size() : max_eQ_NP;
		max_eQ_LP = eQ_LP.size() > max_eQ_LP ? eQ_LP.size() : max_eQ_LP;

		max_now_eQ_HP = now_eQ_HP.size() > max_now_eQ_HP ? now_eQ_HP.size()
				: max_now_eQ_HP;
		max_now_eQ_NP = now_eQ_NP.size() > max_now_eQ_NP ? now_eQ_NP.size()
				: max_now_eQ_NP;
		max_now_eQ_LP = now_eQ_LP.size() > max_now_eQ_LP ? now_eQ_LP.size()
				: max_now_eQ_LP;
	}

	/*
	 * Effectively inserts an event into the event queue
	 * 
	 * (non-Javadoc)
	 * 
	 * @see edu.harvard.syrah.sbon.async.EventLoopIF#registerTimerEvent(edu.harvard.syrah.sbon.async.CB0)
	 */

	public CB0 registerTimerCB(CB0 cb) {
		return registerTimerCB(cb, Priority.NORMAL);
	}

	public CB0 registerTimerCB(CB0 cb, Priority priority) {
		return registerTimerCB(0, cb, priority);
	}

	public CB0 registerTimerCB(long delay, CB0 cb) {
		return registerTimerCB(delay, cb, Priority.NORMAL);
	}

	/*
	 * Acts like delaycb from the libasync C library
	 * 
	 * (non-Javadoc)
	 * 
	 * @see edu.harvard.syrah.sbon.async.EventLoopIF#registerTimerEvent(long,
	 *      edu.harvard.syrah.sbon.async.CB0)
	 */
	public CB0 registerTimerCB(long delay, CB0 cb,
			Priority priority) {
		cb.ts = System.currentTimeMillis() + delay;

		Queue<CB0> eventQueue = null;
		if (delay != 0) {
			eventQueue = getEventQueue(priority);
		} else {
			eventQueue = getNowEventQueue(priority);
		}

		eventQueue.add(cb);

		/*
		 * TODO handle this properly
		 */
		/*
		 * if (eventQueue.size() > EVENT_QUEUE_LIMIT) { log.warn("The queue with
		 * pri=" + priority + " has grown beyong its limit (" + EVENT_QUEUE_LIMIT + ")
		 * size=" + eventQueue.size() + " cb=" + cb); }
		 */
		
		// Make sure that the selector is not block if this is called from another thread
		selector.wakeup();
					
		return cb;
	}

	public void registerTimerCB(Barrier barrier, CB0 cb) {
		registerTimerCB(barrier, cb, Priority.NORMAL);
	}

	public void registerTimerCB(Barrier barrier, CB0 cb,
			Priority priority) {
		registerTimerCB(barrier, 0, cb, priority);
	}

	public void registerTimerCB(Barrier barrier, long delay, CB0 cb) {
		registerTimerCB(barrier, delay, cb, Priority.NORMAL);
	}

	public void registerTimerCB(Barrier barrier, long delay, CB0 cb,
			Priority priority) {
		barrier.registerTimerCB(delay, cb, priority);
	}

	public long deregisterTimerCB(CB0 cb) {
		return deregisterTimerCB(cb, Priority.NORMAL);
	}

	public long deregisterTimerCB(CB0 cb, Priority priority) {
		assert cb != null;

		Queue<CB0> eventQueue = getEventQueue(priority);
		if (!eventQueue.remove(cb)) {
			log.error("The eventQueue=" + POut.toString(eventQueue)
					+ " doesn't contain the cb=" + cb);
		}
		return cb.ts - System.currentTimeMillis();
	}

	public void registerCommCB(SelectableChannel channel, int selectionKey)
			throws ClosedChannelException {
		assert channel != null;
		ChannelSub channelSub = channelSubTable.get(channel);
		assert channelSub != null : "channelSub is null for registerCommCB call. channel="
				+ channel;
		assert channelSub.checkCommCBs(selectionKey);

		try {
			// Register with selector while preserving previous registrations
			if (channel.keyFor(selector) != null) {
				// log.debug("Updating key.");
				channel.register(selector, channel.keyFor(selector).interestOps()
						| selectionKey, channelSub);
			} else {
				// log.debug("Adding new key.");
				channel.register(selector, selectionKey, channelSub);
			}
		} catch (IllegalArgumentException e) {
			log.error("Illegal key selector mask: " + e + " selectionKey="
					+ selectionKey);
		} catch (CancelledKeyException e) {
			log.warn(("CancelledKeyException channel=" + channel + " e=" + e));
		}
		// log.debug("channelSub: channel=" + channel + " "+ channelSub);
	}

	public void deregisterCommCB(SelectableChannel channel, int selectionKey)
			throws ClosedChannelException {
		assert channel != null;
		ChannelSub channelSub = channelSubTable.get(channel);
		// assert channelSub != null;
		if (channelSub == null) {
			log.warn("channelSub==null channel=" + channel + " Ignoring.");
			return;
		}

		SelectionKey key = channel.keyFor(selector);
		if (key == null) {
			log.warn("key==null channel=" + channel + " Ignoring.");
			return;
		}
		// Deregister with selector while preserving previous registrations
		channel.register(selector, key.interestOps() & (~selectionKey), channelSub);
	}

	public void deregisterAllCommCBs(SelectableChannel channel)
			throws ClosedChannelException {
		assert channel != null;
		ChannelSub channelSub = channelSubTable.get(channel);
		// assert channelSub != null : "channel=" + channel;
		if (channelSub == null) {
			log.warn("channelSub==null channel=" + channel + " Ignoring.");
			return;
		}

		channel.register(selector, 0, channelSub);
	}

	public CB1R<Boolean, SelectionKey> getCommCB(SelectableChannel channel, int selectionKey) {
		assert channel != null;
		ChannelSub channelSub = channelSubTable.get(channel);

		// Is this an unknown channel?
		if (channelSub == null)
			return null;

		return channelSub.getCommCB(selectionKey);
	}

	public void setCommCB(SelectableChannel channel, int selectionKey,
			CB1R<Boolean, SelectionKey> commCB) {
		assert channel != null;
		assert commCB != null;

		ChannelSub channelSub = channelSubTable.get(channel);

		if (channelSub == null) {
			channelSub = new ChannelSub();
			channelSubTable.put(channel, channelSub);
		}

		channelSub.setCommCBs(selectionKey, commCB);
	}

	public void unsetCommCB(SelectableChannel channel, int selectionKey) {
		assert channel != null;
		ChannelSub channelSub = channelSubTable.get(channel);
		assert channelSub != null;

		assert (!channel.keyFor(selector).isValid() || (channel.keyFor(selector)
				.interestOps() & selectionKey) == 0);
		channelSub.setCommCBs(selectionKey, null);
		if (channelSub.isEmpty()) {
			channelSubTable.remove(channel);
		}
	}

	public void unsetAllCommCBs(SelectableChannel channel) {
		assert channel != null;
		ChannelSub channelSub = channelSubTable.get(channel);

		if (channelSub != null) {

			/*
			 * TODO This assertion is violated when the key has already been
			 * cancelled.
			 */
			// assert channel.keyFor(selector) == null ||
			// (channel.keyFor(selector).interestOps() & SELECTION_KEY_ALL_OPS) == 0;
			channelSub.setCommCBs(SELECTION_KEY_ALL_OPS, null);
			channelSubTable.remove(channel);
		} else {
			log.warn("Trying to close an already closed channel=" + channel);
		}
	}

	public void main() {
		log.debug(ANSI.color(Color.LIGHTRED, "Starting main event loop..."));

		long timeInterval=10;
		
		long slackTime;
		long nextEventTS;
		 PTimer pt = new PTimer(false);

		while (!forceExit && !(loopExit && eventQueuesEmpty())) {
			// log.debug(" Starting new event loop iteration");

			// pt.start();
			nextEventTS = handleEventQueues();
			// pt.stop(log, "handleEventQueues took");

			slackTime = (nextEventTS == INFINITE_SLACKTIME) ? INFINITE_SLACKTIME
					: (nextEventTS - System.currentTimeMillis());
			// log.debug("slackTime=" + (slackTime == INFINITE_SLACKTIME ? "INF" :
			// String.valueOf(slackTime)) + " eventQueuesEmpty=" +
			// eventQueuesEmpty());

			if ((!now_eQ_HP.isEmpty() || !now_eQ_NP.isEmpty() || !now_eQ_LP.isEmpty())
					&& (slackTime == INFINITE_SLACKTIME)) {
				log.warn("Now_queues have events but slacktime is INF. Resetting.");
				slackTime = 0;
			}

			assert (slackTime != INFINITE_SLACKTIME) || eventQueuesEmpty() : "slackTime="
					+ slackTime;

			if (showIdle && slackTime != INFINITE_SLACKTIME && slackTime > 0)
				log.info("Idle for " + slackTime + " ms"); // now_eQ_NORM.size=" +
																										// now_eQ_NP.size());

			if (!forceExit && !loopExit) {
				 //pt.start();
				
				//Ericfu
				handleSelector(slackTime%timeInterval);
				//handleSelector(timeInterval);
				//handleSelector(-1);
				
				// pt.stop(log, "handleSelector (slackTime=" + (slackTime ==
				// INFINITE_SLACKTIME ? "INF" : String.valueOf(slackTime)) + ") took");
				// pt.start();
				handleSelectCallbacks();
				// pt.stop(log, "handleSelectCBs took");
				// log.debug("numSelectors=" + numSelectorKeys);
			}
		}

		log.main("Exited main event loop.");
	}

	public void forceExit() {
		forceExit = true;
	}

	public void exit() {
		log.main(ANSI.color(Color.LIGHTRED, "Ready to exit main event loop..."));
		loopExit = true;
	}

	public boolean shouldExit() {
		return loopExit;
	}

	/**
	 * Ericfu, synchronized the network library, todo
	 */
	public void handleNetwork() {
		
		
		handleSelector(-1);
		//Ericfu
		handleSelectCallbacks();
	}

	public boolean checkChannelState(SelectableChannel channel, int selectionKey) {
		
		handleSelector(-1);
		log.debug("checkState: "+Thread.currentThread().toString());
		
		CAN_SELECT=EL.MainCanRunCallback;
		boolean results=false;;
		for (Iterator<SelectionKey> keyIt = selector.selectedKeys().iterator(); keyIt.hasNext();) {
			SelectionKey key = (SelectionKey) keyIt.next();

			//Ericfu- 11-15
			if(!key.isValid()){
				continue;
			}
			
			if (key.channel().equals(channel) && key.isValid()
					&& ((key.readyOps() & selectionKey) == selectionKey)) {
				//return true;
				results=true;
				break;
			}
		}
		CAN_SELECT=EL.SubThreadCanRun;
		
		return results;
	}

	private boolean eventQueuesEmpty() {
		return eQ_HP.isEmpty() && eQ_NP.isEmpty() && eQ_LP.isEmpty()
				&& now_eQ_HP.isEmpty() && now_eQ_NP.isEmpty() && now_eQ_LP.isEmpty();
	}

	private Queue<CB0> getEventQueue(Priority priority) {
		Queue<CB0> eventQueue = null;

		switch (priority) {
			case HIGH: {
				eventQueue = eQ_HP;
				break;
			}
			case NORMAL: {
				eventQueue = eQ_NP;
				break;
			}
			case LOW: {
				eventQueue = eQ_LP;
				break;
			}
			default: {
				log.error("Unknown priority");
				break;
			}
		}
		return eventQueue;
	}

	private Queue<CB0> getNowEventQueue(Priority priority) {
		Queue<CB0> eventQueue = null;

		switch (priority) {
			case HIGH: {
				eventQueue = now_eQ_HP;
				break;
			}
			case NORMAL: {
				eventQueue = now_eQ_NP;
				break;
			}
			case LOW: {
				eventQueue = now_eQ_LP;
				break;
			}
			default: {
				log.error("Unknown priority");
				break;
			}
		}
		return eventQueue;
	}

	private long handleEventQueues() {

		long startTime = 0;
		long globalNextTS = INFINITE_SLACKTIME;
		long processingTime = 0;

		boolean HP_hasSlack = false;
		boolean NP_hasSlack = false;
		boolean LP_hasSlack = false;

		// while ((!(eQ_HP.isEmpty() && now_eQ_HP.isEmpty()) && !HP_hasSlack)
		// || (!(eQ_NP.isEmpty() && now_eQ_NP.isEmpty()) && !NP_hasSlack)
		// || (!(eQ_LP.isEmpty() && now_eQ_LP.isEmpty()) && !LP_hasSlack)) {

		while (((!eQ_HP.isEmpty() && !HP_hasSlack) || !now_eQ_HP.isEmpty())
				|| ((!eQ_NP.isEmpty() && !NP_hasSlack) || !now_eQ_NP.isEmpty())
				|| ((!eQ_LP.isEmpty() && !LP_hasSlack) || !now_eQ_LP.isEmpty())) {

			// log.debug("Executing events...");

			long currentTime = System.currentTimeMillis();
			if (startTime == 0)
				startTime = currentTime;

			processingTime = currentTime - startTime;
			// log.debug("processing.remaining=" + (MAX_EVENT_QUEUE_PERIOD -
			// processingTime));
			if (processingTime >= MAX_EVENT_QUEUE_PERIOD) {
				// Make sure that we continue the event queue processing
				globalNextTS = currentTime;
				break;
			}

			HP_hasSlack = false;
			NP_hasSlack = false;
			LP_hasSlack = false;
			globalNextTS = INFINITE_SLACKTIME;

			boolean handleNext = true;

			/* HIGH */

			// log.debug("Considering HIGH queue...");
			if (!eQ_HP.isEmpty() && handleNext) {
				long nextEventTS = handleEvent(HIGH, currentTime);
				if (nextEventTS > currentTime) {
					HP_hasSlack = now_eQ_HP.isEmpty();
					// log.debug("HIGH has slack");
				} else {
					handleNext = false;
				}
				globalNextTS = Math.min(nextEventTS, globalNextTS);
			}

			// log.debug("Considering now_HIGH queue...");

			if (!now_eQ_HP.isEmpty() && handleNext) {
				long nextEventTS = handleNowEvent(HIGH);
				handleNext = false;
				globalNextTS = Math.min(nextEventTS, globalNextTS);
			}

			/* NORMAL */

			// log.debug("Considering NORMAL queue...");
			if (!eQ_NP.isEmpty() && handleNext) {
				long nextEventTS = handleEvent(NORMAL, currentTime);
				if (nextEventTS > currentTime) {
					NP_hasSlack = now_eQ_NP.isEmpty();
					// log.debug("NORMAL has slack");
				} else {
					handleNext = false;
				}
				globalNextTS = Math.min(nextEventTS, globalNextTS);
			}

			// log.debug("Considering now_NORMAL queue...");

			if (!now_eQ_NP.isEmpty() && handleNext) {
				long nextEventTS = handleNowEvent(NORMAL);
				handleNext = false;
				globalNextTS = Math.min(nextEventTS, globalNextTS);
			}

			/* LOW */

			// log.debug("Considering LOW queue...");
			if (!eQ_LP.isEmpty() && handleNext) {
				long nextEventTS = handleEvent(LOW, currentTime);
				if (nextEventTS > currentTime) {
					LP_hasSlack = now_eQ_LP.isEmpty();
					// log.debug("LOW has slack");
				} else {
					handleNext = false;
				}
				globalNextTS = Math.min(nextEventTS, globalNextTS);
			}

			// log.debug("Considering now_LOW queue...");

			if (!now_eQ_LP.isEmpty() && handleNext) {
				long nextEventTS = handleNowEvent(LOW);
				handleNext = false;
				globalNextTS = Math.min(nextEventTS, globalNextTS);
			}

			// log.debug("globalNextTS=" + globalNextTS);

		}
		// log.debug("globalNextTS=" + (globalNextTS == INFINITE_SLACKTIME ? "INF" :
		// String.valueOf(globalNextTS))
		// + " eventQueueTime=" + (MAX_EVENT_QUEUE_PERIOD - processingTime));
		return globalNextTS;
	}

	private long handleEvent(Priority priority, long currentTime) {
		Queue<CB0> eventQueue = getEventQueue(priority);
		CB0 cb = eventQueue.peek();
		long eventSlackTime = cb.ts - currentTime;
		// log.debug("pri=" + priority + " eventSlackTime=" + eventSlackTime);

		// Do we need to execute the event at once?
		if (eventSlackTime <= 0) {

			assert cb != null;

			PTimer pt = null;
			if (showCBTime)
				pt = new PTimer();

			// log.debug("cb=" + nextEvent.cb);
			cb.cb(CBResult.OK());
			// pt.stop(log, "Event CB (cb=" + nextEvent.cb + ") took");

			if (showCBTime) {
				pt.stop();

				if (MAX_CB_TIME > 0 && pt.getTime() > MAX_CB_TIME)
					log.warn("cb=" + cb + " took " + pt.toString() + " > "
							+ MAX_CB_TIME + " ms");
			}

			eventQueue.remove(cb);

			if (!eventQueue.isEmpty()) {
				cb = eventQueue.peek();
				return cb.ts;
			}

			return INFINITE_SLACKTIME;
		}

		// log.debug("pri=" + priority + " nextEventTS=" + nextEvent.ts);
		return cb.ts;
	}

	private long handleNowEvent(Priority priority) {
		Queue<CB0> eventQueue = getNowEventQueue(priority);
		CB0 cb = eventQueue.remove();

		assert cb != null && cb != null;

		PTimer pt = null;
		if (showCBTime)
			pt = new PTimer();

		// log.info("now_cb=" + nextEvent.cb);
		cb.call(CBResult.OK());
		// pt.stop(log, "Now_Event CB (cb=" + nextEvent.cb + ") took");

		if (showCBTime) {
			pt.stop();

			if (MAX_CB_TIME > 0 && pt.getTime() > MAX_CB_TIME)
				log.warn("cb=" + cb + " took " + pt.toString() + " > "
						+ MAX_CB_TIME + " ms");
		}

		if (!eventQueue.isEmpty()) {
			cb = eventQueue.peek();
			return cb.ts;
		}

		return INFINITE_SLACKTIME;
	}

	private void handleSelector(long slackTime) {
		
		

		//===========================
		//can add
		log.debug("Out: "+Thread.currentThread().getName());
		
		if (slackTime > 0) {

			if (slackTime == INFINITE_SLACKTIME) {
				// Block indefinitely
				try {
					// log.debug("Sleeping..." /* + eQ=" + eventQueue.size() */ );
					
					if(CAN_SELECT==EL.MainCanRunCallback){
						return;
					}
						
					selector.select();
					// log.debug("Woken up...");
					
					
					
					//assert (eventqueues not empty || forceExit || !selector.selectedKeys().isEmpty()) : "The selector returned without any selected keys. This should never happen.";

				} catch (IOException e) {
					

					log.error("Could not complete select(): " + e);
				}
			} else {
				// Block for slacktime
				try {
					// log.debug("Sleeping for " + slackTime + " ms");
					
					if(CAN_SELECT==EL.MainCanRunCallback){
						return;
					}
					
					selector.select(slackTime);
					
					// log.debug("Woken up...");
				} catch (IOException e) {			
					log.error("Could not complete select(slackTime): " + e);
				}
			}
		} else {
			/*
			 * TODO It would make sense to plan in advance here and sleep until the
			 * next event. A selectNow would only be necessary if the next event has
			 * to be executed immediately.
			 */
			
			// Do an instant select (non blocking)
			try {
				if(CAN_SELECT==EL.MainCanRunCallback){
					return;
				}
				
				selector.selectNow();
			} catch (IOException e) {
				log.error("Could not complete selectNow(): " + e);				
			}
		}
	
	
	}

	
	/**
	 * Ericfu
	 * @return
	 */
	public synchronized boolean getToken(){
		boolean result=false;
		if(token){
			result=token;
			token =false;
		}
		return result;
	}
	/**
	 * 
	 * @return
	 */
	public synchronized boolean releaseToken(){
		token=true;
		return token;
	}
	                          
	private void handleSelectCallbacks() {
		
		/*//get the token
		if(!getToken()){
			return;
		}*/
		
		try {
			// PTimer pt = new PTimer();

		
			CAN_SELECT=EL.MainCanRunCallback;
				
			
			log.debug("In: "+Thread.currentThread().getName());
			
			numSelectorKeys = selector.selectedKeys().size();

			log.debug("selectedKeys.size=" + numSelectorKeys);
			for (Iterator<SelectionKey> keyIt = selector.selectedKeys().iterator(); keyIt.hasNext();) {
				SelectionKey key = (SelectionKey) keyIt.next();

				//Ericfu- 11-15
				if(!key.isValid()){
					continue;
				}
				
				CB1R<Boolean, SelectionKey> cbComm = null;
				// commEvent.ts = System.currentTimeMillis();

				ChannelSub channelSub = (ChannelSub) key.attachment();

				int readyOps = key.readyOps();
				
				//log.debug("channel=" + key.channel());

				if (key.isValid() && key.isAcceptable()) {
					// log.debug("Handling comms cb with key.isAcceptable()");
					if (channelSub.acceptCB != null) {
						cbComm = channelSub.acceptCB;
						// PTimer pt = new PTimer();
						if (cbComm.cb(CBResult.OK(), key) && key.isValid()) {
							readyOps &= ~SelectionKey.OP_ACCEPT;
						}
						// pt.stop(log, "Accept CB took");
					} else {
						log.debug("acceptCB=null channel=" + key.channel() + "cST="
								+ POut.toString(channelSubTable));
					}
				}
				if (key.isValid() && key.isConnectable()) {
					// log.debug("Handling comms cb with key.isConnectable()");
					if (channelSub.connectCB != null) {
						cbComm = channelSub.connectCB;
						// PTimer pt = new PTimer();
						if (cbComm.cb(CBResult.OK(), key) && key.isValid()) {
							readyOps &= ~SelectionKey.OP_ACCEPT;
						}
						// pt.stop(log, "Connect CB took");
					} else {
						log.debug("connectCB=null channel=" + key.channel() + " cST="
								+ POut.toString(channelSubTable));
					}
				}
				if (key.isValid() && key.isReadable()) {
					//log.debug("Handling comms cb with key.isReadable()");
					if (channelSub.readCB != null) {
						cbComm = channelSub.readCB;
						// PTimer pt = new PTimer();
						if (cbComm.cb(CBResult.OK(), key) && key.isValid()) {
							readyOps &= ~SelectionKey.OP_READ;
						}
						// pt.stop(log, "Read CB took");
					} else {
						log.debug("readCB=null channel=" + key.channel() + " cST="
								+ POut.toString(channelSubTable));
					}
				}
				if (key.isValid() && key.isWritable()) {
					// log.debug("Handling comms cb with key.isWritable()");
					if (channelSub.writeCB != null) {
						cbComm = channelSub.writeCB;
						// PTimer pt = new PTimer();
						if (cbComm.cb(CBResult.OK(), key) && key.isValid()) {
							readyOps &= ~SelectionKey.OP_WRITE;
						}
						// pt.stop(log, "Write CB took");
					} else {
						log.debug("writeCB=null channel=" + key.channel() + "cST="
								+ POut.toString(channelSubTable));
					}
				}
				
				
				// Have you dealt with all the interest ops?
				if ((!key.isValid()) || readyOps  == 0) {
					try {
						keyIt.remove();
					} catch (ConcurrentModificationException e) {
						log.error(e.toString());
					}
				}
				
			}
			
//			synchronized(CAN_SELECT){
			CAN_SELECT=EL.SubThreadCanRun;
			
			releaseToken();
			 
			// pt.stop(log, "HandleSelectCBs took");

		} catch (ClosedSelectorException e) {
			log.warn("Selector closed.");

			  CAN_SELECT=EL.SubThreadCanRun;
			  
			  releaseToken();
							 
			shutdown();
		}

	}

	protected void shutdown() {
		if (!ranShutdown) {
			log.debug("Running shutdown hook.");
			forceExit = true;
			try {
				log.debug("Closing selector with all connections.");
				selector.close();
			} catch (IOException e) {
				log.error("Could not close selector");
			}
			log.debug("Shutdown hook complete.");
			ranShutdown = true;
		}
	}

	class ChannelSub {
		protected CB1R<Boolean, SelectionKey> acceptCB = null;
		protected CB1R<Boolean, SelectionKey> connectCB = null;
		protected CB1R<Boolean, SelectionKey> readCB = null;
		protected CB1R<Boolean, SelectionKey> writeCB = null;

		void setCommCBs(int selectionKey, CB1R<Boolean, SelectionKey> commCB) {
			if ((selectionKey & SelectionKey.OP_ACCEPT) == SelectionKey.OP_ACCEPT) {
				acceptCB = commCB;
			}
			if ((selectionKey & SelectionKey.OP_CONNECT) == SelectionKey.OP_CONNECT) {
				connectCB = commCB;
			}
			if ((selectionKey & SelectionKey.OP_READ) == SelectionKey.OP_READ) {
				readCB = commCB;
			}
			if ((selectionKey & SelectionKey.OP_WRITE) == SelectionKey.OP_WRITE) {
				writeCB = commCB;
			}
		}

		CB1R<Boolean, SelectionKey> getCommCB(int selectionKey) {
			if ((selectionKey & SelectionKey.OP_ACCEPT) == SelectionKey.OP_ACCEPT) {
				return acceptCB;
			}
			if ((selectionKey & SelectionKey.OP_CONNECT) == SelectionKey.OP_CONNECT) {
				return connectCB;
			}
			if ((selectionKey & SelectionKey.OP_READ) == SelectionKey.OP_READ) {
				return readCB;
			}
			if ((selectionKey & SelectionKey.OP_WRITE) == SelectionKey.OP_WRITE) {
				return writeCB;
			}
			log.error("Unknown selectionKey");
			return null;
		}

		boolean checkCommCBs(int selectionKey) {
			if ((selectionKey & SelectionKey.OP_ACCEPT) == SelectionKey.OP_ACCEPT
					&& acceptCB == null) {
				return false;
			}
			if ((selectionKey & SelectionKey.OP_CONNECT) == SelectionKey.OP_CONNECT
					&& connectCB == null) {
				return false;
			}
			if ((selectionKey & SelectionKey.OP_READ) == SelectionKey.OP_READ
					&& readCB == null) {
				return false;
			}
			if ((selectionKey & SelectionKey.OP_WRITE) == SelectionKey.OP_WRITE
					&& writeCB == null) {
				return false;
			}
			return true;
		}

		boolean isEmpty() {
			return acceptCB == null && connectCB == null && readCB == null
					&& writeCB == null;
		}

		public String toString() {
			return "acceptCB=" + (acceptCB != null) + "["
					+ (acceptCB != null ? acceptCB.toString() : "") + "] " + "connectCB="
					+ (connectCB != null) + "["
					+ (connectCB != null ? connectCB.toString() : "") + "] " + "readCB="
					+ (readCB != null) + "[" + (readCB != null ? readCB.toString() : "")
					+ "] " + "writeCB=" + (writeCB != null) + "["
					+ (writeCB != null ? writeCB.toString() : "") + "]";
		}
	}

	public static void main(String[] args) {
		EL.set(new EL());

		log.main("Registering callback");

		EL.get().registerTimerCB(new SimpleCB("NORM-3", 1000), HIGH);
		EL.get().registerTimerCB(new SimpleCB("NORM-2", 1000), NORMAL);
		EL.get().registerTimerCB(new SimpleCB("NORM-1", 1000), LOW);

		// EventLoop.get().registerTimerCB(100, new SimpleCB("NORM-A", 1000),
		// NORMAL);
		// EventLoop.get().registerTimerCB(200, new SimpleCB("NORM-B", 1000),
		// NORMAL);
		// EventLoop.get().registerTimerCB(300, new SimpleCB("NORM-C", 1000),
		// NORMAL);

		EL.get().dumpState(true);

		// EventLoop.get().registerTimerCB(new SimpleCB("HIGH", 1500), HIGH);
		// EventLoop.get().registerTimerCB(new SimpleCB("NORM", 1000), NORMAL);
		// EventLoop.get().registerTimerCB(new SimpleCB("LOW", 500), LOW);

		CB0 cbWhileLoop = new CB0() {
			protected void cb(CBResult result) {
				// do stuff
				boolean exitCondition = false;
				if (!exitCondition)
					EL.get().registerTimerCB(this);
			}
		};

		EL.get().registerTimerCB(cbWhileLoop);

		EL.get().main();
	}
}

class SimpleCB extends CB0 {

	private long lastRun;
	private long interval;

	public SimpleCB(String name, long interval) {
		super(name);
		this.interval = interval;
	}

	protected void cb(CBResult result) {
		//EventLoop.get().dumpState(true);
		long currentTime = System.currentTimeMillis();
		if (lastRun == 0)
			lastRun = currentTime;
		POut.p("Running cb " + toString() + "... " + (currentTime - lastRun));
		lastRun = currentTime;
		//EventLoop.get().registerTimerCB(interval, this);
	}
}
