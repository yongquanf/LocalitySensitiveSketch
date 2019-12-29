/*
 * SBON
 * 
 * @author Last modified by $Author: prp $
 * @version $Revision: 1.1 $ on $Date: 2007/06/04 17:55:41 $
 * @since Jan 6, 2005
 */
package edu.harvard.syrah.sbon.async;

import java.nio.channels.ClosedChannelException;
import java.nio.channels.SelectableChannel;
import java.nio.channels.SelectionKey;

import edu.harvard.syrah.sbon.async.CallbacksIF.*;
import edu.harvard.syrah.sbon.async.EL.Priority;

/**
 *
 * This is the interface of the main asynchronous event loop for the node.
 *
 */
public interface EventLoopIF {
	
	/**
	 * Main method that runs the event loop. It should be the last call in the program's main method.
	 */
	public void main();
	
	/**
	 * Force the exist from the main event loop. 
	 */
	public void forceExit();
	
	/**
	 * Gracefully exit the main loop. This will only exit the main event loop if there are no pending
	 * timers.
	 */
	public void exit();
  
  public boolean shouldExit();
	
	public CB0 registerTimerCB(CB0 cb);
	
	public CB0 registerTimerCB(CB0 cbEvent, Priority priority);
	
	/**
	 * This methods allows an object to register a callback timer.
	 * 
	 * @param delay
	 * @param event
	 */
	public CB0 registerTimerCB(long delay, CB0 cbEvent);
	
	public CB0 registerTimerCB(long delay, CB0 cbEvent, Priority priority);
	
	/**
	 * Register a new callback timer when the barrier permits it.
	 * 
	 * @param barrier
	 * @param cbEvent
	 */
	public void registerTimerCB(Barrier barrier, CB0 cbEvent);
	
	public void registerTimerCB(Barrier barrier, CB0 cbEvent, Priority priority);

	public void registerTimerCB(Barrier barrier, long delay, CB0 cbEvent);
	
	public void registerTimerCB(Barrier barrier, long delay, CB0 cbEvent, Priority priority);
	
	/**
	 * Remove an existing timer callback from the event queue.
	 * 
	 * @param CB0
	 */
	public long deregisterTimerCB(CB0 cb);
	
	public long deregisterTimerCB(CB0 cb, Priority priority);
		
	/**
	 * Registers the interest in communication events. Note that selectionKey is not a bit field,
	 * which means that calls to this method are additive.
	 * 
	 * @param channel
	 * @param selectionKey
	 * @param event
	 */
	public void registerCommCB(SelectableChannel channel, int selectionKey) throws ClosedChannelException;
		
	/**
	 * Allows an object to deregister a communication callback.
	 * 
	 * @param channel
	 * @param selectionKey
	 * @throws ClosedChannelException
	 */
	public void deregisterCommCB(SelectableChannel channel, int selectionKey) throws ClosedChannelException;
	
	public void deregisterAllCommCBs(SelectableChannel channel) throws ClosedChannelException;
	
	public void setCommCB(SelectableChannel channel, int selectionKey, CB1R<Boolean, SelectionKey> commCB);
	
	public void unsetCommCB(SelectableChannel channel, int selectionKey);	

	public CB1R<Boolean, SelectionKey> getCommCB(SelectableChannel channel, int selectionkey);
	
	public void unsetAllCommCBs(SelectableChannel channel);
	
	public boolean checkChannelState(SelectableChannel channel, int selectionKey);
		
	public void handleNetwork();
	
	public void dumpState(boolean eventQueueDump);
	
}
