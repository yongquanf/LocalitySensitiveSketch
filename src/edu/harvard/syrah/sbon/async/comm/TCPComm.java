/*
 * @author Last modified by $Author: prp $
 * @version $Revision: 1.1 $ on $Date: 2007/06/04 17:55:41 $
 * @since Jan 9, 2005
 */
package edu.harvard.syrah.sbon.async.comm;

import java.io.IOException;
import java.net.ServerSocket;
import java.nio.channels.*;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import edu.harvard.syrah.prp.Log;
import edu.harvard.syrah.prp.POut;
import edu.harvard.syrah.sbon.async.CBResult;
import edu.harvard.syrah.sbon.async.Config;
import edu.harvard.syrah.sbon.async.EL;
import edu.harvard.syrah.sbon.async.CallbacksIF.CB0;


/**
 * Abstract class that is the base for all communication layer implementations. It handles the
 * creation and acceptance of connections but doesn't specify any strategy for reading and
 * writing.
 */
abstract public class TCPComm extends Comm implements TCPCommIF {
	protected static final Log log = new Log(TCPComm.class);
				
	protected static final int CONNECTION_RETRIES = Integer.valueOf(Config.getConfigProps().getProperty(
		"sbon.connection.retries", "3"));
		
	protected static final long STATE_DUMP_INTERVAL= Long.valueOf(Config.getConfigProps().getProperty(
		"sbon.comm.statedump", "0"));
	
	// Keep track of pending connections
	protected Map<NetAddress, Comm.ConnectConnHandler> pendingConnectionPool = new ConcurrentHashMap<NetAddress, 
		Comm.ConnectConnHandler>();
			
	// Destinations to which we maintain connections
	public Map<NetAddress, Comm.WriteConnHandler> destConnectionPool = new ConcurrentHashMap<NetAddress, 
		Comm.WriteConnHandler>();
	
	//public Map<NetAddress, Comm.WriteConnHandler> destConnectionTried = new ConcurrentHashMap<NetAddress, Comm.WriteConnHandler>();
	
	//public Map<NetAddress, Comm.ReadConnHandler> destConnectionReadTried = new ConcurrentHashMap<NetAddress, Comm.ReadConnHandler>();
	
	// Stores timeout handlers for outgoing connections
	protected Map<SelectableChannel, TimeoutHandler> timeoutPool = new ConcurrentHashMap<SelectableChannel, TimeoutHandler>();
						
	protected TCPComm() {
		super();
		
		if (STATE_DUMP_INTERVAL != 0) {
			EL.get().registerTimerCB(STATE_DUMP_INTERVAL, new CB0("TCPComm-StateDumper") {
				protected void cb(CBResult result) {
					log.main("Comm: " + TCPComm.this.getClass().getSimpleName());
					log.main("destConnectionPool.size=" + destConnectionPool.size() + " destConnectionPool=" 
						+ POut.toString(destConnectionPool.keySet()));
					log.main("pendingConnectionPool.size=" + pendingConnectionPool.size() + " pendingConnectionPool=" 
						+ POut.toString(pendingConnectionPool.keySet()));
					log.main("timeoutPool.size=" + timeoutPool.size() + " timeoutPool=" + POut.toString(timeoutPool.keySet()));
          if (!EL.get().shouldExit())
            EL.get().registerTimerCB(STATE_DUMP_INTERVAL, this);
				}			
			});
		}
	}
	
	/**
	 * create a new connection
	 * @param destAddr
	 * @param reuseConnections
	 * @param timeout
	 * @param cbHandler
	 * @return
	 */
	public Comm.ConnectConnHandler createConnection(AddressIF destAddr, boolean reuseConnections, long timeout, 
		CB0 cbHandler) {
		
		log.debug("Creating a new TCP connection to destAddr=" + destAddr);
		assert destAddr instanceof NetAddress;
		
		NetAddress remoteAddr = (NetAddress) destAddr;
		
		if (remoteAddr.isWildcard())
			log.error("Cannot create a connection to wildcard addres. remoteAddr=" + remoteAddr);
		
		if (!remoteAddr.isResolved())
			log.error("Cannot create a connection to an unresolved address. remoteAddr=" + remoteAddr);
		
		//log.debug("connectionPool.keySet()=" + Util.toString(connectionPool.keySet()));
		//log.debug("pendingConnectionPool.keySet()=" + POut.toString(pendingConnectionPool.keySet()));
		Comm.WriteConnHandler writeConnHandler = destConnectionPool.get(remoteAddr);
		
		// Do we have an existing connection?
		if (writeConnHandler == null || (!reuseConnections)) {
			log.debug("No Existing connection to destAddr=" + destAddr);
			/**
			 * cache all pending requests for many threads
			 */
			Comm.ConnectConnHandler connectConnHandler = pendingConnectionPool.get(remoteAddr);
			
			if (connectConnHandler == null || (!reuseConnections)) {
				log.debug("Connecting with TCP to remoteAddr=" + remoteAddr);
				connectConnHandler = getConnectConnHandler(remoteAddr, timeout, cbHandler);
				pendingConnectionPool.put(remoteAddr, connectConnHandler);				
				return connectConnHandler;
			} 
			log.debug("Waiting for pending connection to destAddr=" + remoteAddr);
			return connectConnHandler;
		} 
		log.warn("Connection to destAddr=" + remoteAddr + " already exists.");
		return null;
	}
	
	public void closeConnection(AddressIF remoteAddr, SelectableChannel channel) {
		assert channel != null;
		
		Comm.WriteConnHandler writeConnHandler = null;
		if (remoteAddr != null) {
			writeConnHandler = destConnectionPool.remove(remoteAddr);
			//close the pending tried
			
		/*	if(this.destConnectionTried.containsKey(remoteAddr)){
			this.destConnectionTried.remove(remoteAddr);
			}*/
		/*	if(this.destConnectionReadTried.containsKey(remoteAddr)){
			this.destConnectionReadTried.get(remoteAddr).deregister();
			this.destConnectionReadTried.remove(remoteAddr);
			}*/
		}

		// Has the connection been already closed by us?
		if (writeConnHandler != null) {
			log.debug("Closing channel=" + channel);
		}
		
		if (timeoutPool.containsKey(channel))
			timeoutPool.remove(channel).deregister();
		
		super.closeConnection(channel);	
	}
	
	public boolean checkConnection(Comm.WriteConnHandler writeConnHandler) {
		if (!writeConnHandler.channel.isOpen()) {
			log.warn("Checking connection and channel=" + writeConnHandler.channel + " has been closed");
			closeConnection(writeConnHandler.remoteAddr, writeConnHandler.channel);
			return false;
		} 
		return true;
	}
	
	protected TimeoutHandler createTimeoutHandler(SelectableChannel channel, AddressIF remoteAddr, long timeout) {
		return new TimeoutHandler(channel, remoteAddr, timeout);
	}
			
	/*
	 * Callback to accept a new connection
	 */
	protected abstract class AcceptConnHandler extends Comm.AcceptConnHandler {
		private final Log log = new Log(AcceptConnHandler.class);
				
		protected AcceptConnHandler(AddressIF bindAddress, CB0 cbHandler) {
			super(cbHandler);
			assert bindAddress instanceof NetAddress;
			
			NetAddress bindNetAddress = (NetAddress) bindAddress;
			
			try {
				// Create the server socket
				ServerSocketChannel serverSocketChannel = ServerSocketChannel.open();
				ServerSocket serverSocket = serverSocketChannel.socket();
				serverSocket.bind(bindNetAddress.getInetSocketAddress());
				serverSocketChannel.configureBlocking(false);				
				
				this.channel = serverSocketChannel;
				this.setCommCB();

				// Register a callback with the main event loop when a new connection is created
				this.register();
				
			} catch (ClosedChannelException e1) {
				log.warn("Could not register interest with closed channel: " + e1);
				cbHandler.call(CBResult.ERROR(e1.toString()));
        return;
			} catch (IOException e2) {
				log.warn("Could not create server socket: " + e2);
				cbHandler.call(CBResult.ERROR(e2.toString()));
        return;
			}
			if (cbHandler != null)
				cbHandler.callOK();
		}
				
		public Boolean cb(CBResult result, SelectionKey key) {			
			if (key.isValid() && key.isAcceptable()) {												
				ServerSocketChannel serverSocketChannel = (ServerSocketChannel) key.channel();
				try {
					channel = serverSocketChannel.accept();
					channel.configureBlocking(false);
					log.debug("Accepting. channel=" + channel);																									
				} catch (IOException e) {
					log.warn("Could not accept new connection: " + e);
				}							
			} else {
				log.error("We received an acceptable callback for a key that is not acceptable. Bug?");
			}		
			return true;
		}		
	}
	
	/*
	 * Callback for connecting a new connection
	 */
	protected abstract class ConnectConnHandler extends Comm.ConnectConnHandler {
		private final Log log = new Log(ConnectConnHandler.class);

		private int retryCounter = 1;
		private long timeout;
								
		protected ConnectConnHandler(NetAddress destAddr, long timeout, CB0 cbHandler) {
			super(destAddr, cbHandler);
			this.timeout = timeout;
			doConnect();
		}
		
		public Boolean cb(CBResult result, SelectionKey key) {
			
			if (key.isValid() && key.isConnectable()) {
				channel = key.channel();
				SocketChannel socketChannel = (SocketChannel) channel;
				log.debug("Finishing new connection to remoteAddr=" + remoteAddr);
				try {
					// Try to finish the connection
					socketChannel.finishConnect();					
				} catch (final IOException e) {
					log.debug("Connection failed for remoteAddr=" + remoteAddr + ": " + e + " retry=" + retryCounter);

					//bugWorkaround();
					this.destruct();
					
					if (retryCounter < CONNECTION_RETRIES) {					
						retryCounter++;
						doConnect();
					} else {
            log.debug("Failed trying to establish the connection to remoteAddr=" + remoteAddr);
						pendingConnectionPool.remove(remoteAddr);
						EL.get().registerTimerCB(new CB0() {
							protected void cb(CBResult resultOK) {
								cbHandler.call(CBResult.ERROR("remoteAddr=" + remoteAddr.toString(false) + ": "	+ e.getMessage()));								
							}							
						});
					}					
					return true;					
				} 
				
				log.debug("Connected with TCP to remoteAddr=" + remoteAddr);									
				//pendingConnectionPool.remove(remoteAddr);

				if (timeout != 0) {
					createTimeoutHandler(channel, remoteAddr, timeout);
				}

				bugWorkaround();
				this.destruct();
				log.debug("Finished");
				
			} else {
				log.error("Received a connectable CB for a key that is not connectable. Bug?");				
			}
			return true;
		}
				
		private void doConnect() {			
			try {
				channel = SocketChannel.open();
				this.setCommCB();
				
				SocketChannel socketChannel = (SocketChannel) channel;
				socketChannel.configureBlocking(false);
				
				socketChannel.connect(remoteAddr.getInetSocketAddress());
				
			} catch (IOException e) {
				log.warn("Could not connect to remoteAddr=" + remoteAddr + ": " + e);
			}
			
			register();
		}
		
		private void bugWorkaround() {
			/*
			 * Here we need to explicitly de-register the OP_CONNECT
			 * interest, otherwise we will constantly be getting callbacks.
			 * 
			 * This is BUG #4960791 in the Sun JDK bug database.
			 */			
			deregister();
		}				
	}
	
	protected abstract class ReadConnHandler extends Comm.ReadConnHandler {
		protected ReadConnHandler(SelectableChannel channel, NetAddress remoteAddr) {	super(channel, remoteAddr);	}

		protected Boolean cb(CBResult result, SelectionKey key) {
			if (timeoutPool.containsKey(channel)) { 
				timeoutPool.get(channel).touchConnection();
			}
			return true;
		}
	}
	
	protected abstract class WriteConnHandler extends Comm.WriteConnHandler {
		protected WriteConnHandler(SelectableChannel channel, NetAddress remoteAddr) { super(channel, remoteAddr); }

		protected Boolean cb(CBResult result, SelectionKey key) { 
			if (timeoutPool.containsKey(channel)) { 
				timeoutPool.get(channel).touchConnection();
			}
			return true;
		}
	}
	
	/**
	 * 
	 * This handler runs periodically and closes connection that have not been used for at least
	 * CONNECTION_TIMEOUT.
	 * 
	 * TODO: There appears to be a bug where we eventually run out of file descriptors after a long time.
	 * 			 It may be that the code below is not working properly...?
	 *
	 */
	public class TimeoutHandler extends CB0 {		
		private final Log log = new Log(TimeoutHandler.class);
		
		protected SelectableChannel channel;
		protected AddressIF remoteAddr;
		private long timeout;
				
		private boolean touchedConnection = false;
		
		private CB0 cbTimer;
		
		public TimeoutHandler(SelectableChannel channel, AddressIF remoteAddr, long timeout) {
			log.debug("New timeouthandler: channel=" + channel);
			this.channel = channel;
			this.remoteAddr = remoteAddr;
			this.timeout = timeout;
			this.cbTimer = EL.get().registerTimerCB(timeout, this);
			timeoutPool.put(channel, this);
		}
		
		void touchConnection() {
			//log.debug("Touching connection: remoteAddr=" + remoteAddr);
			touchedConnection = true;
		}				
		
		void deregister() {
			if (cbTimer != null) {
				log.debug("Deregistering the timeout handler");
				EL.get().deregisterTimerCB(cbTimer);
			}
		}
		
		protected void timeoutConnection() {
			log.debug("Connection with remoteAddr=" + remoteAddr + " has expired. Closing.");
			closeConnection(remoteAddr, channel);
		}

		public void cb(CBResult result) {
			cbTimer = null;
			log.debug("destConnectionPool.keySet()=" + destConnectionPool.keySet());
			if (!touchedConnection) {
				if (!EL.get().checkChannelState(channel, SelectionKey.OP_READ)) {
					timeoutConnection();
				} else {
					log.debug("Cannot close. Read outstanding.");					
				}
				return;
			}
			
			log.debug("Channel=" + channel + " is still active.");
			touchedConnection = false;
			cbTimer = EL.get().registerTimerCB(timeout, this);			
		}
    
    public String toString() { return "TCPTimeout"; }
	}	
}
