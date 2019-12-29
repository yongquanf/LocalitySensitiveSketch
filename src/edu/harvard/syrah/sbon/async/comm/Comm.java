/*
 * SBON
 * 
 * @author Last modified by $Author: ledlie $
 * @version $Revision: 1.2 $ on $Date: 2009/04/23 20:55:47 $
 * @since Jan 19, 2005
 */
package edu.harvard.syrah.sbon.async.comm;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.SelectableChannel;
import java.nio.channels.SelectionKey;

import edu.harvard.syrah.prp.Log;
import edu.harvard.syrah.sbon.async.EL;
import edu.harvard.syrah.sbon.async.CallbacksIF.*;

/**
 *
 * Parent class for all connection-oriented communication modules
 *
 */
abstract public class Comm implements CommIF {
	protected static final Log log = new Log(Comm.class);
	
	// Size of the buffer used for reading/writing from/to connections (32KB)
	protected static final int MIN_BUFFER_SIZE = 32 * 1024;
	
	// Maximum buffer size of 1 MB
	protected static final int MAX_BUFFER_SIZE =  1 * 1024 * 1024;
	
	// The local address
	public NetAddress localNetAddress;

	// The handler for accepting incoming connections
	protected AcceptConnHandler acceptConnHandler = null;
	
	public AddressIF getLocalAddress() {
		assert localNetAddress != null : "local addr is null";
		return localNetAddress; 
	}
	
	public int getLocalPort() { return localNetAddress.getPort(); }
	
	public void setLocalAddress(AddressIF localAddress) {
		if (!(localAddress instanceof NetAddress))
			log.warn("Can only understand NetAddresses.");
		
		this.localNetAddress = (NetAddress) localAddress;
	}
	
	/* 
	 * @see edu.harvard.syrah.sbon.communication.CommIF#init(edu.harvard.syrah.sbon.communication.AddressIF)
	 */
	public void initServer(AddressIF bindAddress, CB0 cbInit) {
		if (!(bindAddress instanceof NetAddress))
			log.warn("Can only understand NetAddresses.");		
		
		log.debug("initServer with bindAddress=" + bindAddress);
		NetAddress bindNetAddress = (NetAddress) bindAddress;
		setLocalAddress(AddressFactory.createLocal(bindNetAddress.getPort()));
		// Don't pass in a net handler here -- no one's interested
		acceptConnHandler = getAcceptConnHandler(bindNetAddress, cbInit);
	}
	
	public void deinitServer() { closeConnection(acceptConnHandler.channel); }
	
	protected void closeConnection(SelectableChannel channel) {
		assert channel != null;
		try {
			EL.get().deregisterAllCommCBs(channel);
		} catch (ClosedChannelException e) {
			log.debug("Channel already closed");
		}
		EL.get().unsetAllCommCBs(channel);
		
		try {
			channel.close();
		} catch (IOException e) {
			log.debug("Could not close connection: " + e);
		}
	}
		
	protected abstract AcceptConnHandler getAcceptConnHandler(NetAddress localAddress, CB0 cbHandler);
	
	protected abstract ConnectConnHandler getConnectConnHandler(NetAddress remoteAddress, long timeout, CB0 cbHandler);
	
	abstract class ConnHandler extends CB1R<Boolean, SelectionKey> {
		public SelectableChannel channel = null;		
		public NetAddress remoteAddr;
				
		protected ConnHandler(SelectableChannel channel, NetAddress remoteAddr) {
			this.channel = channel;
			this.remoteAddr = remoteAddr;
		}
		
		public String toString() { return ""; }
	}
	
	abstract protected class AcceptConnHandler extends ConnHandler {		
		protected CB0 cbHandler;
		
		protected AcceptConnHandler(CB0 cbHandler) { 
			super(null, null);
			this.cbHandler = cbHandler;
		}
		
		protected void register() {
			try {
				EL.get().registerCommCB(channel, SelectionKey.OP_ACCEPT);
			} catch (ClosedChannelException e) {
				log.warn(e.toString());
			}
		}
		
		protected void deregister() {
			try {
				EL.get().deregisterCommCB(channel, SelectionKey.OP_ACCEPT);
			} catch (ClosedChannelException e) {
				log.warn(e.toString());
			}
		}
		
		protected void setCommCB() { EL.get().setCommCB(channel, SelectionKey.OP_ACCEPT, this); }
	}
	
	abstract protected class ConnectConnHandler extends ConnHandler {		
		protected CB0 cbHandler;
		
		protected ConnectConnHandler(NetAddress remoteAddr, CB0 cbHandler) { 
			super(null, remoteAddr);
			this.cbHandler = cbHandler;
		}
		
		protected void register() {
			try {
				EL.get().registerCommCB(channel, SelectionKey.OP_CONNECT);
			} catch (ClosedChannelException e) {
				log.warn(e.toString());
			}
		}
		
		protected void deregister() {
			try {
				EL.get().deregisterCommCB(channel, SelectionKey.OP_CONNECT);
			} catch (ClosedChannelException e) {
				log.warn(e.toString());
			}
		}		
		
		protected void destruct() { EL.get().unsetCommCB(channel, SelectionKey.OP_CONNECT); }
		
		protected void setCommCB() { EL.get().setCommCB(channel, SelectionKey.OP_CONNECT, this);	}
	}
	
	/*
	 * Callback to read data from an existing connection. Must be overidden.
	 */
	protected abstract class ReadConnHandler extends ConnHandler {	
		private final Log log = new Log(ReadConnHandler.class);			

		/*
		 * TODO
		 * There some problem with the buffer allocation here -- we're running out of direct
		 * memeory. If this problem doesn't go away, these buffers must be replaced by
		 * non-direct ones.
		 */
		public ByteBuffer buffer = ByteBuffer.allocate /* Direct */ (MIN_BUFFER_SIZE);
		
		protected ReadConnHandler(SelectableChannel channel, NetAddress remoteAddr) {
			super(channel, remoteAddr);
			log.debug("Allocating new ReadConnHandler for remoteAddr=" + remoteAddr);
			EL.get().setCommCB(channel, SelectionKey.OP_READ, this);
		}
		
		public boolean hasData() { return buffer.position() != 0; }
		
		public void register() {
			try {
				EL.get().registerCommCB(channel, SelectionKey.OP_READ);
			} catch (ClosedChannelException e) {
				log.warn("remoteAddr="+remoteAddr+" "+e.toString());
			}
		}
		
		public void deregister() {
			try {
				EL.get().deregisterCommCB(channel, SelectionKey.OP_READ);
			} catch (ClosedChannelException e) {
				log.warn("remoteAddr="+remoteAddr+" "+e.toString());
			}
		}		
		
		public void destruct() {	EL.get().unsetCommCB(channel, SelectionKey.OP_READ);	}
		
		public String toString() { return String.valueOf(buffer.capacity()); }
	}
	
	/*
	 * Callback to write data from an existing connection. Must be overidden.
	 */
	protected abstract class WriteConnHandler extends ConnHandler {		
		private final Log log = new Log(WriteConnHandler.class);

		protected ByteBuffer buffer = ByteBuffer.allocate /* Direct */ (MIN_BUFFER_SIZE);
				
		protected WriteConnHandler(SelectableChannel channel, NetAddress remoteAddr) {
			super(channel, remoteAddr);
			log.debug("Allocating new WriteConnHandler for remoteAddr=" + remoteAddr);
			EL.get().setCommCB(channel, SelectionKey.OP_WRITE, this);
		}
				
		public void register() {
			try {
				EL.get().registerCommCB(channel, SelectionKey.OP_WRITE);
			} catch (ClosedChannelException e) {
				log.warn("remoteAddr="+remoteAddr+" "+e.toString());
			}
		}
		
		public void deregister() {
			assert buffer.position() == 0;
			try {
				EL.get().deregisterCommCB(channel, SelectionKey.OP_WRITE);
			} catch (ClosedChannelException e) {
				log.warn("remoteAddr="+remoteAddr+" "+e.toString());
			}				
		}

		public SelectableChannel getChannel() { return channel;	}
		
		public void setChannel(SelectableChannel channel) { this.channel = channel; }
		
		public String toString() { return String.valueOf(buffer.capacity()); }
	}
	
}


