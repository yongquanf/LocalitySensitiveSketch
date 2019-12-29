/*
 * SBON
 * 
 * @author Last modified by $Author: prp $
 * @version $Revision: 1.3 $ on $Date: 2007/08/14 11:18:13 $
 * @since Jan 7, 2005
 */
package edu.harvard.syrah.sbon.async.comm.obj;

import java.io.IOException;
import java.nio.channels.SelectableChannel;
import java.nio.channels.SelectionKey;
import java.nio.channels.SocketChannel;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import edu.harvard.syrah.prp.*;
import edu.harvard.syrah.sbon.async.CBResult;
import edu.harvard.syrah.sbon.async.Config;
import edu.harvard.syrah.sbon.async.EL;
import edu.harvard.syrah.sbon.async.LoopIt;
import edu.harvard.syrah.sbon.async.CallbacksIF.CB0;
import edu.harvard.syrah.sbon.async.CallbacksIF.CB0R;
import edu.harvard.syrah.sbon.async.CallbacksIF.CB1;
import edu.harvard.syrah.sbon.async.CallbacksIF.CB2;
import edu.harvard.syrah.sbon.async.comm.*;

/**
 * 
 * Implementation of the Comm module with Java object serialisation
 * 
 * TODO - Short-cut for messages sent to localhost
 * 
 */
public class ObjComm extends TCPComm implements ObjCommIF {
	protected static final Log log = new Log(ObjComm.class);

	// Number of times to retry sending when buffer is full
	private static final int MAX_SEND_RETRIES = 3;

	// Retry a message send after this many ms
	private static final long SEND_RETRY_DELAY = 2500;

	// Timeout for all request/response calls (3min)
	private static final long RR_MSG_TIMEOUT = 180000;

	// Timeout for the CB that handles an RR ObjComm msg (30s)
	private static final long RR_CB_TIMEOUT = 30000;

	// Timeout for the CB that handles a regular ObjComm msg (60s)
	private static final long CB_TIMEOUT = 60 * 1000;

	private static final int MAX_OUTSTANDING_CBS = 3; // was: 10

	private static final int KEEPALIVE_TIMEOUT = Integer.parseInt(Config.getConfigProps().getProperty(
			"sbon.connection.obj.timeout", "30000"));

	private static final boolean TRACE = Boolean.valueOf(Config.getConfigProps().getProperty(
			"sbon.trace", "false"));

	// Callback table for message classes
	@SuppressWarnings("unchecked")
	protected Map<Class, List<ObjCommCB>> callbackTable = new ConcurrentHashMap<Class, List<ObjCommCB>>();

	// Callback table for request/response with message ids
	@SuppressWarnings("unchecked")
	protected Map<Long, ObjCommRRCB> requestResponseCBTable = new ConcurrentHashMap<Long, ObjCommRRCB>();

	// Callback table for request/error with message ids
	@SuppressWarnings("unchecked")
	protected Map<Long, ObjCommRRCB> requestErrorCBTable = new ConcurrentHashMap<Long, ObjCommRRCB>();

	public ObjComm() {
		super();
	}

	public void initServer(AddressIF bindAddress, CB0 cbInit) {
		super.initServer(bindAddress, cbInit);
		log.info("Binding to addr=" + bindAddress + " (localAddr=" + localNetAddress + ")");
	}

	public void sendMessage(ObjMessageIF message, AddressIF destAddr, CB0 cbSent) {
		sendMessage(message, destAddr, false, cbSent);
	}

	/*
	 * @see edu.harvard.syrah.sbon.communication.CommIF#sendMessage(edu.harvard.syrah.sbon.communication.MessageIF,
	 *      edu.harvard.syrah.sbon.communication.AddressIF)
	 */
	public void sendMessage(ObjMessageIF message, AddressIF destAddr, boolean bestEffort, final CB0 cbSent) {
		// log.debug("Sending msg=" + message);
		// log.debug(" destAddr=" + destAddr);
		
		assert destAddr != null : "DestAddr is null";

		assert destAddr instanceof NetAddress : "Can only send messages to NetAddresses (addr=" + destAddr + ")";

		if (getLocalAddress() == null) {
			log.error("Must set local address with initServer before sending messages.");
		}
	
		final NetAddress remoteAddr = (NetAddress) destAddr;

		if (!remoteAddr.isResolved()) {
			EL.get().registerTimerCB(new CB0() {
				protected void cb(CBResult result) {
					cbSent.call(CBResult.ERROR("Cannot send a message to an unresolved address="
							+ remoteAddr.toString(false)));
				}
			});
			return;
		}

		if (!remoteAddr.hasPort()) {
			EL.get().registerTimerCB(new CB0() {
				protected void cb(CBResult result) {
					cbSent.call(CBResult.ERROR("Cannot send a message without a valid port to address="
							+ remoteAddr.toString(false)));
				}
			});
			return;
		}

		
		byte[] msgArray = marshallMsg(message);
		
		/*if(msgArray==null||msgArray.length==0){
			log.warn("message is null!"+message.toString());
			
			EL.get().registerTimerCB(new CB0() {
				protected void cb(CBResult result) {					
					cbSent.call(CBResult.ERROR("Cannot send a message with zero content"
							+ remoteAddr.toString(false)));
				}
			});

			return;
		}{*/
		// log.debug("destConnectionPool.keySet()=" + destConnectionPool.keySet());
		Comm.WriteConnHandler writeConnHandler = destConnectionPool.get(remoteAddr);

		// Do we have an existing connection that is alive
		if (writeConnHandler == null || (!checkConnection(writeConnHandler))) {
			log.debug("No existing connection found.");

			if(!pendingConnectionPool.containsKey(destAddr)){
			Comm.ConnectConnHandler connectConnHandler = createConnection(destAddr, true,
					KEEPALIVE_TIMEOUT, new CB0() {
						protected void cb(CBResult result) {
							switch (result.state) {
								case OK: {
									// ignore
									break;
								}
								case TIMEOUT:
								case ERROR: {
									cbSent.call(result);
									break;
								}
							}
						}
					});
			// Add the message to the queue
			((ConnectConnHandler) connectConnHandler).addRequest(new ObjRequest(msgArray, bestEffort,
					cbSent));
			}else{
				//have not received the answer, add the request into pending connection
				((ConnectConnHandler)pendingConnectionPool.get(remoteAddr)).addRequest(new ObjRequest(msgArray, bestEffort,
						cbSent));
			}

		} else {
			log.debug("Existing connection to destAddr=" + destAddr + " found.");
			
			// Add the message to this handler
			((WriteConnHandler) writeConnHandler).addRequest(new ObjRequest(msgArray, bestEffort, cbSent));			
		}

		// log.debug("Handling the network...");
		log.debug(Thread.currentThread().toString());
		//change the selector
		//Ericfu
//		EL.get().handleNetwork();
		//EL.get().handleSelector(-1);
		// log.debug("Done handling the network.");
		
		//}
	}
	
	private byte[] marshallMsg(ObjMessageIF message) {
		// log.debug("message=" + message);
		byte[] byteArray = NetUtil.serializeObject(message);
		// log.debug("message2=" + Util.deserializeObject(byteArray));
		return byteArray;
	}

	public void sendRequestMessage(ObjMessageIF message, AddressIF destAddr,
			ObjCommRRCB<? extends ObjMessageIF> cbResponseMessage) {
		
		
		//sendRequestMessage(message, destAddr, cbResponseMessage, null);
		sendRequestMessage(message, destAddr, cbResponseMessage, new ObjCommRRCB(){

			@Override
			protected void cb(CBResult result, Object arg1, Object arg2,
					Object arg3) {
				// TODO Auto-generated method stub
				
			}
			
			
		});
		
		
	}

	public void sendRequestMessage(ObjMessageIF message, final AddressIF destAddr,
			final ObjCommRRCB<? extends ObjMessageIF> cbResponseMessage,
			ObjCommRRCB<? extends ObjMessageIF> cbErrorMessage) {

		if (!cbResponseMessage.hasTimeout()) {
			// Add a timeout
			cbResponseMessage.setTimeout("ObjCommRRTimeout", RR_MSG_TIMEOUT);
		}

		long messageId = PUtil.getRandomPosLong();
		// log.debug("Sending request message=" + message);
		// log.debug(" id=" + messageId + " to destAddr=" + destAddr);
		message.setMsgId(messageId);
		message.setResponse(false);

		if (requestResponseCBTable.containsKey(messageId)) {
			log.error("Picked a key that already exists in the requestResponseCBTable. This is very unlikely and probably a bug");
		}
		requestResponseCBTable.put(messageId, cbResponseMessage);
		requestErrorCBTable.put(messageId, cbErrorMessage);
		// log.debug("requestResponseCBTable=" +
		// POut.toString(requestResponseCBTable));
		// log.debug("requestErrorCBTable=" + POut.toString(requestErrorCBTable));

		sendMessage(message, destAddr, false, new CB0() {
			protected void cb(CBResult result) {
				// Did the sending of the message fail?
				switch (result.state) {
					case OK: {
						// Ignore this because we expect the response message back and there
						// is a timeout on this
						break;
					}
					case ERROR:
					case TIMEOUT: {
						log.warn("Request/reponse failed. destAddr=" + destAddr + " :" + result);
						cbResponseMessage.call(result, null, destAddr, null);
						break;
					}
				}
			}
		});
	}

	public void sendResponseMessage(ObjMessageIF message, AddressIF destAddr, long requestMsgId,
			CB0 cbSent) {
		assert requestMsgId > 0 : "msg ids need to be positive. msgId=" + requestMsgId;
		message.setMsgId(requestMsgId);
		message.setResponse(true);
		// log.debug("Sending response msg=" + message);
		// log.debug(" id=" + requestMsgId + " to destAddr=" + destAddr);
		sendMessage(message, destAddr, false, cbSent);
	}

	public void sendErrorMessage(ObjMessageIF message, AddressIF destAddr, long requestMsgId,
			CB0 cbSent) {
		assert requestMsgId > 0 : "msg ids need to be positive. msgId=" + requestMsgId;
		message.setMsgId(requestMsgId);
		message.setError(true);
		message.setResponse(true);
		// log.debug("Sending error msg=" + message);
		// log.debug(" id=" + requestMsgId + " to destAddr=" + destAddr);
		sendMessage(message, destAddr, false, cbSent);
	}

	/*
	 * @see edu.harvard.syrah.sbon.communication.CommIF#registerCallback(edu.harvard.syrah.sbon.communication.CommCBIF)
	 */
	@SuppressWarnings("unchecked")
	public void registerMessageCB(Class messageClass, ObjCommCB<? extends ObjMessageIF> newCallback) {
		log.debug("Registering new callback for class=" + messageClass);
		List<ObjCommCB> callbacks = callbackTable.get(messageClass);

		if (callbacks == null) {
			callbacks = new LinkedList<ObjCommCB>();
			callbackTable.put(messageClass, callbacks);
		}
		callbacks.add(newCallback);
	}

	@SuppressWarnings("unchecked")
	public void deregisterMessageCB(Class messageClass, ObjCommCB<? extends ObjMessageIF> oldCallback) {
		log.debug("Deregistering callback for class=" + messageClass);
		List<ObjCommCB> callbacks = callbackTable.get(messageClass);
		callbacks.remove(oldCallback);
	}

	protected AcceptConnHandler getAcceptConnHandler(NetAddress localAddress, CB0 cbHandler) {
		return new AcceptConnHandler(localAddress, cbHandler);
	}

	protected ConnectConnHandler getConnectConnHandler(NetAddress remoteAddress, long timeout,
			CB0 cbHandler) {
		return new ConnectConnHandler(remoteAddress, timeout, cbHandler);
	}

	protected ReadConnHandler getReadConnHandler(SelectableChannel channel, NetAddress remoteAddr) {
		ReadConnHandler readConnHandler = (ReadConnHandler) EL.get().getCommCB(channel,
				SelectionKey.OP_READ);
		if (readConnHandler == null) {
			readConnHandler = new ReadConnHandler(channel, remoteAddr);
			//records
			//destConnectionReadTried.put(remoteAddr, readConnHandler);
			/**
			 * Ericfu
			 * remove the pending request
			 */
			/*if(this.destConnectionReadTried.containsKey(remoteAddr)){
				this.destConnectionReadTried.remove(remoteAddr);
			}*/
		}
		return readConnHandler;
	}

	protected WriteConnHandler getWriteConnHandler(SelectableChannel channel, NetAddress remoteAddr) {
		WriteConnHandler writeConnHandler = (WriteConnHandler) EL.get().getCommCB(channel,
				SelectionKey.OP_WRITE);
		if (writeConnHandler == null) {
			writeConnHandler = new WriteConnHandler(channel, remoteAddr);
			destConnectionPool.put(remoteAddr, writeConnHandler);
			
		/*	if(destConnectionTried.containsKey(remoteAddr)){
				//Ericfu
				//remove the record
				destConnectionTried.remove(remoteAddr);
			}*/
			
		}
		return writeConnHandler;
	}

	/*
	 * Callback to accept a new connection
	 */
	class AcceptConnHandler extends TCPComm.AcceptConnHandler {

		AcceptConnHandler(AddressIF bindAddress, CB0 cbHandler) {
			super(bindAddress, cbHandler);
		}

		public Boolean cb(CBResult result, SelectionKey key) {
			super.cb(result, key);

			if (key.isValid() && key.isAcceptable()) {
				
				/*log.warn("addr: "+remoteAddr);
				if(destConnectionReadTried.containsKey(remoteAddr)){
					destConnectionReadTried.remove(remoteAddr);
				}*/
				
				ReadConnHandler readConnHandler = getReadConnHandler(channel, remoteAddr);
				readConnHandler.register();
				/*
				 * We can't allocate a writeConnHandler here yet because we don't know
				 * the identity of the remote party
				 */
			}
			return true;
		}
	}

	/*
	 * Callback for connecting a new connection
	 */
	class ConnectConnHandler extends TCPComm.ConnectConnHandler {
		private final Log log = new Log(ConnectConnHandler.class);

		private List<ObjRequest> objRequestList = new LinkedList<ObjRequest>();

		ConnectConnHandler(NetAddress destAddr, long timeout, CB0 cbHandler) {
			super(destAddr, timeout, cbHandler);
		}

		public Boolean cb(CBResult result, SelectionKey key) {
			super.cb(result, key);

			if (key.isValid() && key.isConnectable()) {

				WriteConnHandler writeConnHandler = null;
				//remove the pending connection
				pendingConnectionPool.remove(remoteAddr);
				if(remoteAddr==null){
					//log.warn("remote address is null!");
					
				}
				else{
				//use counter to test the call process
				if (destConnectionPool.containsKey(remoteAddr)) {
					//log.debug("Already existing connection. Reusing.");
					writeConnHandler = (WriteConnHandler) destConnectionPool.get(remoteAddr);

					try {
						channel.close();
					} catch (IOException e) {
						//log.error("Could not close channel: " + e);
					}

				} else {
					//Ericfu
					//put 
					//if(remoteAddr!=null&&!destConnectionReadTried.containsKey(remoteAddr)){
											
					//log.warn("does not contain, "+remoteAddr+"Allocating new read and write handlers");
					
					Comm.ReadConnHandler readConnHandler = getReadConnHandler(channel, remoteAddr);
					readConnHandler.register();

					writeConnHandler = getWriteConnHandler(channel, remoteAddr);
					//log.warn("Adding hello msg to write buffer.");
						// Put own address into buffer for other party					
					writeConnHandler.addHelloMsg();
					
					//Ericfu, cache it
					//destConnectionTried.put(remoteAddr, writeConnHandler);
					//destConnectionReadTried.put(remoteAddr, readConnHandler);					
					
					//}else{
						//reuse the pending request, but do not send the pending hello message 
						//writeConnHandler=(WriteConnHandler)destConnectionTried.get(remoteAddr);
					/*	Comm.ReadConnHandler readConnHandler =(Comm.ReadConnHandler)destConnectionReadTried.get(remoteAddr);
						readConnHandler.register();
					*/	
						//add the writing message into the queue of the writeConnHandler, but do not register!
						// to avoid the sending, reset problem
						//wait for the next write
					/*	if(objRequestList!=null&&!objRequestList.isEmpty()){
							writeConnHandler.objRequestList.addAll(objRequestList);
							objRequestList.clear();
						//}
						
					}*/
				  

				}

				if(writeConnHandler!=null){
					//2010-3-11
					//test the request
					//if (writeConnHandler!=null&&objRequestList!=null&&!objRequestList.isEmpty()) {
					
					if(objRequestList!=null){
					synchronized(objRequestList){
					//thread safe?
					writeConnHandler.addRequests(objRequestList);		
					if(objRequestList!=null){
						objRequestList.clear();
					}
					
					}
					}
						writeConnHandler.register();
					}	
				
				
				}//---------------------------remote Address is null
				
			
				
				
				// Return connection callbacks
				cbHandler.call(CBResult.OK());
			}
			return true;
		}

		protected synchronized void addRequest(ObjRequest objRequest) {
			objRequestList.add(objRequest);
			
		}

		public String toString() {
			if(super.toString()==null||objRequestList==null){
				return "null object!";
			}
			return super.toString() + "/" + objRequestList.size();
		}
	}

	/*
	 * Callback to read data from an existing connection
	 */
	class ReadConnHandler extends TCPComm.ReadConnHandler {
		protected final Log log = new Log(ReadConnHandler.class);

		protected int outstandingCBs = 0;

		protected boolean resolvingAddr = false;

		private long timestamp;

		ReadConnHandler(SelectableChannel channel, NetAddress destAddr) {
			super(channel, destAddr);
		}

		public Boolean cb(CBResult result, SelectionKey key) {
			super.cb(result, key);
			
			if (TRACE)
				timestamp = System.nanoTime();

			if (key.isValid() && key.isReadable()) {

				/*
				 * TODO: FIX ME this may be buggy since we might never hear back about
				 * the last bytes read from a socket
				 */
				if (MAX_OUTSTANDING_CBS != 0 && outstandingCBs >= MAX_OUTSTANDING_CBS) {
					// log.warn("Too many outstanding callbacks (" + outstandingCBs + ").
					// Not reading more data. remoteAddr=" + remoteAddr);
					return false;
				}

				if (resolvingAddr) {
					// log.warn("Resolving address. Returning. remoteAddr=" + remoteAddr);
					return false;
				}

				channel = key.channel();
				SocketChannel socketChannel = (SocketChannel) channel;
				// log.debug("channel=" + channel);
				//log.debug("destConnectionPool.keySet=" + destConnectionPool.keySet());

				// Is there enough free buffer space to do another read?
				if (buffer.position() < MAX_BUFFER_SIZE) {
					log.debug("Reading. remoteAddr=" + remoteAddr + " buffer=" + buffer);
					if (buffer.remaining() == 0) {
						log.debug("Full read buffer for channel=" + channel + ". buffer=" + buffer
								+ ". Extending...");
						buffer = PUtil.extendByteBuffer(buffer, buffer.capacity() * 2);
						buffer.limit(buffer.capacity());
						log.debug("new buffer=" + buffer);
					}

					int count = 0;
					try {
						count = socketChannel.read(buffer);
						//log.debug("Read count=" + count + " bytes");
					} catch (IOException e) {
						log.warn("Error reading from socket: " + e + " remoteAddr=" + remoteAddr + " count="
								+ count + " key=" + key + " channel=" + channel); // + "
																																	// destCP=" +
																																	// POut.toString(destConnectionPool));
						closeConnection(remoteAddr, socketChannel);
						return true;
					}

					/**
					 * read zero bytes!!
					 * Ericfu
					 */
					
					if (count == 0) {
						//log.warn("Odd. Received a selector key and only read 0 bytes. This is a bug?");
						return true;
					}

					//receiver disconnect quickly
					if (count == -1 ) {
						// Is this channel still open?
						log.warn("Connection to remoteAddr=" + remoteAddr + " has been closed.");
						log.warn("  channel=" + channel);

						if (remoteAddr != null) {
							WriteConnHandler writeConnHandler = (WriteConnHandler) destConnectionPool.get(remoteAddr);
							if (writeConnHandler != null && writeConnHandler.hasRequests()) {

								/*
								 * TODO Fix this: move the requests to a new connection...
								 */

								log.warn("The connection had outstanding requests but was still closed.");
							}
						}
						closeConnection(remoteAddr, socketChannel);
						return true;
					}
				} else {
					// log.warn("Read buffer is full. Not reading more data. buffer=" +
					// buffer + " remoteAddr=" + remoteAddr);
				}

				// Try to receive a hello message
				receiveHello(new CB0() {
					protected void cb(CBResult result) {

						// log.debug("touching channel=" + channel);
						// Tell the timeout handler that this connection is in use
						// WriteConnHandler writeConnHandler = (WriteConnHandler)
						// EventLoop.get().getCommCB(channel,
						// SelectionKey.OP_WRITE);

						// Did we get enough bytes to read the length field?
						while ((remoteAddr != null) && (buffer.position() >= 4)) {
							buffer.flip();

							int currentMsgSize = buffer.getInt();

							if (currentMsgSize <= 0) {
								log.warn("Connection from remoteAddr=" + remoteAddr + " had msgSize="
										+ currentMsgSize + ". Closing.");
								closeConnection(remoteAddr, channel);
								return;
							}

							assert currentMsgSize > 0 : "currentMsgSize=" + currentMsgSize;
							//log.debug("before: buffer=" + buffer + " currentMsgSize=" + currentMsgSize);

							// Can we read a full message?
							if (buffer.limit() < currentMsgSize + 4) {
								// Prepare the buffer for more reading
								buffer.position(buffer.limit());
								buffer.limit(buffer.capacity());
								break;								
							}

							byte[] msgArray = new byte[currentMsgSize];
							buffer.get(msgArray);

							// Compact the buffer
							int limit = buffer.limit();
							int pos = buffer.position();
							buffer.compact();
							buffer.limit(limit - pos);

							Object msgObject = unmarshallMsg(msgArray);

							if (msgObject == null) {
								log.warn("No valid msgObject received from remoteAddr=" + remoteAddr
										+ ". Ignoring.");
								closeConnection(remoteAddr, channel);
								return;
							}

							assert msgObject instanceof ObjMessageIF : "Received a message that is not a ObjMessageIF.";
							// log.warn("msgObj=" + msgObject.toString());

							ObjMessageIF msg = (ObjMessageIF) msgObject;
							performCallback(msg, timestamp);																
								
							// Prepare the buffer for more reading
							buffer.position(buffer.limit());
							buffer.limit(buffer.capacity());							
						}
					}
				});

			} else {
				log.error("We received a readable callback for a key that is not readable. Bug?");
			}
			return true;
			// log.debug("(after reading) buffer.remaining()=" + buffer.remaining());
		}

		private void receiveHello(final CB0 cb) {
			if (remoteAddr == null && buffer.position() >= 8) {
				log.debug("Receiving hello msg.");
				buffer.flip();

				final byte[] addrByteArray = new byte[4];
				buffer.get(addrByteArray);
				int port = buffer.getInt();

				log.debug("Resolving: " + NetUtil.byteIPAddrToString(addrByteArray));
				resolvingAddr = true;

				AddressFactory.createResolved(addrByteArray, port, new CB1<AddressIF>() {
					protected void cb(CBResult result, AddressIF resolvedAddr) {
						resolvingAddr = false;
						remoteAddr = (NetAddress) resolvedAddr;
						log.debug("Received remoteAddr=" + remoteAddr);

						WriteConnHandler writeConnHandler = null;

						//for debug
						log.debug("localAddress: "+ NetUtil.byteIPAddrToString(((NetAddress)localNetAddress).getByteIPAddr())+", "+
								"remoteAddress: "+ NetUtil.byteIPAddrToString(((NetAddress)resolvedAddr).getByteIPAddr()));
						
						// Be careful because we might already know about a loopback
						// connection
						if (destConnectionPool.containsKey(remoteAddr) && !remoteAddr.equals(localNetAddress)) {

							// log.debug("connectionPool.keySet()=" +
							// POut.toString(channelPool.keySet()));
							//log.debug("destConnectionPool.keySet()=" + POut.toString(destConnectionPool.keySet()));
							log.warn("Received another connection from remoteAddr=" + remoteAddr);
							// log.debug(" destConnectionSet=" +
							// POut.toString(destConnectionPool));

							WriteConnHandler oldWriteConnHandler = (WriteConnHandler) destConnectionPool.get(remoteAddr);

							if (localNetAddress.getIntIPAddr() > remoteAddr.getIntIPAddr()
									&& checkConnection(oldWriteConnHandler)) {
								log.warn("Rejecting new connection.");

								ReadConnHandler.this.deregister();
								ReadConnHandler.this.destruct();

								try {
									channel.close();
								} catch (IOException e) {
									log.error("Could not close channel: " + e);
								}
								return;
							}

							log.warn("Closing old connection.");
							List<ObjRequest> objRequests = oldWriteConnHandler.getRequests();
							closeConnection(remoteAddr, oldWriteConnHandler.getChannel());
							writeConnHandler = getWriteConnHandler(channel, remoteAddr);

							if (!objRequests.isEmpty()) {
								log.debug("Old connection has outstanding requests");
								writeConnHandler.addRequests(objRequests);
								writeConnHandler.register();
							}
						} else {
							//we have a writer
							writeConnHandler = getWriteConnHandler(channel, remoteAddr);
							//register the query
							writeConnHandler.register();
						}

						int limit = buffer.limit();
						buffer.compact();
						buffer.position(limit - 8);
						cb.call(CBResult.OK());
					}
				});
			} else {
				cb.call(CBResult.OK());
			}
		}

		@SuppressWarnings("unchecked")
		protected void performCallback(final ObjMessageIF msg, final long timestamp) {
			//log.debug("msg=" + msg);
			outstandingCBs++;

			// Is this a response message?
			if (!msg.isResponse()) {
				List<ObjCommCB> callbacks = callbackTable.get(msg.getClass());
				if (callbacks == null) {
					log.warn("No callback registered for message class=" + msg.getClass() + ". Ignoring.");
					return;
				}

				// log.debug("numCBs=" + callbacks.size() + " outstandingCBs=" +
				// outstandingCBs);

				LoopIt<ObjCommCB> cbLoop = new LoopIt<ObjCommCB>("ObjCommCBLoop", callbacks,
						new CB2<ObjCommCB, CB0R<Boolean>>() {
							protected void cb(CBResult result, final ObjCommCB objCommCB,
									final CB0R<Boolean> cbRecursion) {
								//log.debug("Performing cb for msg=" + msg);
								//log.debug(" remoteAddr=" + remoteAddr);
								//log.debug(" objCommCB=" + objCommCB);
								objCommCB.call(CBResult.OK(), msg, remoteAddr, timestamp, new CB1<Boolean>(
										CB_TIMEOUT) {
									protected void cb(CBResult result, Boolean handled) {
										switch (result.state) {
											case OK: {
												if (handled) {
													outstandingCBs--;
													// log.debug("Message handled by callback.");
												} else {
													log.warn("Message not handled by callback. Continuing...");
													if (!cbRecursion.call(result)) {
														log.warn("No valid callback found for msg=" + msg + " remoteAddr="
																+ remoteAddr);
													}
												}
												break;
											}
											case TIMEOUT:
											case ERROR: {
												log.warn("Call for ObjComm msg failed: cb=" + objCommCB + " msg=" + msg
														+ " remoteAddr=" + remoteAddr + " result=" + result);
												break;
											}
										}
									}
								});
							}
						});
				cbLoop.execute();
			} else {

				long msgId = msg.getMsgId();
				assert msgId > 0 : "msgId must be positive. msgId=" + msgId;

				/*
				 * Is this a response message?
				 */
				if (!msg.isError()) {
					// Perform a callback for this response message.
					// log.debug("Performing cb for r/r msg=" + msg);
					// log.debug(" remoteAddr=" + remoteAddr + " msg.id=" + msgId);
					final ObjCommRRCB<ObjMessageIF> callback = requestResponseCBTable.remove(msgId);
					// log.debug("requestResponseCBTable=" +
					// POut.toString(requestResponseCBTable));
					requestErrorCBTable.remove(msgId);

					if (callback == null) {
						log.warn("No response callback registered for msg=" + msg + " with msgId=" + msgId
								+ " from " + remoteAddr + ". Duplicate response msg? requestResponseCBTable="
								+ POut.toString(requestResponseCBTable));
					}else{
						//run the callback function
					// log.debug("callback.hasTimeout=" + callback.hasTimeout());
					EL.get().registerTimerCB(new CB0("ObjCommRRCB") {
						protected void cb(CBResult result) {
							if (!callback.isCancelled()) {
								outstandingCBs--;
								callback.call(CBResult.OK(), msg, remoteAddr, timestamp);
							} else {
								log.warn("Callback cb=" + callback + " already cancelled");
								outstandingCBs--;
							}
						}
					});
					}

				} else {
					/*
					 * Perform a callback for this error message.
					 */
					// log.debug("Performing callback for request/error msg=" + msg + "
					// from remoteAddr=" + remoteAddr);
					final ObjCommRRCB<ObjMessageIF> callback = requestErrorCBTable.remove(msgId);
					// log.debug("requestErrorCBTable=" +
					// POut.toString(requestResponseCBTable));
					requestResponseCBTable.remove(msgId).cancel();

					if (callback == null) {
						log.error("No error callback registered for msg=" + msg + " with msgId=" + msgId
								+ " from " + remoteAddr + ". requestResponseCBTable="
								+ POut.toString(requestResponseCBTable));
					}

					EL.get().registerTimerCB(new CB0("OBJCommRRCB") {
						protected void cb(CBResult result) {
							callback.call(CBResult.OK(), msg, remoteAddr, timestamp);
							outstandingCBs--;
						}
					});
				}
			}
		}

		protected Object unmarshallMsg(byte[] msgArray) {
			Object msg = NetUtil.deserializeObject(msgArray);
			// log.debug("message=" + msg);
			return msg;
		}
	}

	/*
	 * Callback to write data from an existing connection
	 */
	class WriteConnHandler extends TCPComm.WriteConnHandler {
		private final Log log = new Log(WriteConnHandler.class);

		private List<ObjRequest> objRequestList = new LinkedList<ObjRequest>();

		WriteConnHandler(SelectableChannel channel, NetAddress remoteAddr) {
			super(channel, remoteAddr);
		}

		public Boolean cb(CBResult result, SelectionKey key) {
			super.cb(result, key);

			if (key.isValid() && key.isWritable()) {

				while (handleNextRequest()) { /* empty */ }

				log.debug("Writing. remoteAddr=" + remoteAddr + ". buffer.position()=" + buffer.position());
				SocketChannel socketChannel = (SocketChannel) channel;
				buffer.flip();

				int count = 0;
				int writeCount = 0;
				//do {
					try {
						writeCount = socketChannel.write(buffer);
						count += writeCount;
						//log.debug("Written count=" + writeCount + " bytes. buffer=" + buffer);
					} catch (IOException e) {
						log.debug("Error writing to socket: " + e + " remoteAddr=" + remoteAddr
								+ " writeCount=" + count + " totalCount=" + count + " key=" + key + " channel="
								+ channel + " buffer=" + buffer + " destCP=" + POut.toString(destConnectionPool));
						return true;
					}
				//} while (writeCount != 0 && buffer.remaining() > 0);

				int limit = buffer.limit();
				// log.debug("limit=" + limit);
				buffer.compact();
				buffer.position(limit - count);

				if (buffer.position() == 0 && !hasRequests()) {
					log.debug("Write buffer is empty.");
					deregister();
				} else {
					log.debug("(after) buffer.position()=" + buffer.position());
				}

			} else {
				log.warn("We received a writable callback for a key that is not writable. Bug?");
			}
			return true;
		}

		protected void addRequest(ObjRequest objRequest) {
			objRequestList.add(objRequest);
			//while (handleNextRequest()) { /* empty */ }
			
			// Register this write handler for comm callback
			register();			
		}

		protected void addRequests(List<ObjRequest> objRequests) {
			if(objRequests!=null&&!objRequests.isEmpty()){
				objRequestList.addAll(objRequests);
			}
			//while (handleNextRequest()) { /* empty */	}
			
			// Register this write handler for comm callback
			register();
		}

		protected boolean hasRequests() {
			return !objRequestList.isEmpty();
		}

		protected List<ObjRequest> getRequests() {
			return objRequestList;
		}

		private boolean handleNextRequest() {			
			//log.warn("handleNextRequest BEGIN l=" + POut.toString(objRequestList));
			if (objRequestList.isEmpty()) {
				//log.warn("handlNextRequest END");
				return false;				
			}

			final ObjRequest objRequest = objRequestList.get(0);
			
			
			//if the array is not null, send it
			//||objRequest.msgArray.length==0
			if(objRequest==null||objRequest.msgArray==null){
				log.debug("handlNextRequest empty objRequest");
				return false;
			}
			//test whether the request is empty
			if (sendMessage(objRequest.msgArray, objRequest.bestEffort)) {
				// log.debug("Handling next obj request.");
				objRequestList.remove(0);
				
				EL.get().registerTimerCB(new CB0() {
					protected void cb(CBResult result) {
						objRequest.cbSent.call(CBResult.OK());
					}
				});
				
				//log.warn("handlNextRequest END");
				return true;
			}else{
				//null requests
				
			}
			//log.warn("handlNextRequest END");
			return false;
		}

		protected boolean sendMessage(final byte[] msgArray, boolean bestEffort) {
			int totalMsgSize = msgArray.length + Integer.SIZE;
			log.debug("totalMsgSize=" + totalMsgSize);

			if (buffer.capacity() < totalMsgSize) {
				log.warn("Send buffer not large enough to contain entire message with size="
						+ totalMsgSize + ". buffer=" + buffer + ". Extending...");

				buffer = PUtil.extendByteBuffer(buffer, buffer.capacity()
						+ (totalMsgSize - buffer.remaining()));
				buffer.limit(buffer.capacity());
				//log.debug("new buffer=" + buffer);
			}

			if (buffer.remaining() < totalMsgSize)
				return false;

			/*
			 * if (buffer.remaining() < totalMsgSize) { log.warn("Send buffer is full
			 * when sending msg=" + msg + " to remoteAddr=" + remoteAddr + "
			 * msgArray.size=" + msgArray.length + Integer.SIZE + " buffer=" +
			 * buffer);
			 * 
			 * if (bestEffort) { log.warn("Best effort: Dropping message."); return; }
			 * else { log.warn("Retrying send in " + SEND_RETRY_DELAY + " ms..."); }
			 * 
			 * EventLoop.get().registerTimerEvent(SEND_RETRY_DELAY, new EventCB() {
			 * private int retryAttempt;
			 * 
			 * @Override protected void cb(CBResult result, Event timerEvent) { if
			 * (retryAttempt < MAX_SEND_RETRIES) { retryAttempt++; sendMessage(msg,
			 * false, cbSent); } else { log.warn("Dropping message. Could not send
			 * msg=" + msg + ". remoteAddr=" + remoteAddr); } } }); return; }
			 */

			// log.debug("(before) buffer.position()=" + buffer.position());
			// log.debug("Added msg=" + msg);
			// log.debug(" writeConnhandler with " + msgArray.length + " bytes");
			buffer.putInt(msgArray.length);
			buffer.put(msgArray);
			// log.debug("(after) buffer.position()=" + buffer.position());

			/*
			 * TODO This is not entirely correct because we should only confirm the
			 * transmission of a message after it has been put on the wire...
			 */

			// Confirm the transmission of the message
			return true;
		}

		
		/**
		 * original method
		 * 2010-3/18
		 */
		protected void addHelloMsg() {
			buffer.put(localNetAddress.getInetSocketAddress().getAddress().getAddress());
			buffer.putInt(localNetAddress.getPort());
		}
		
		//Ericfu
	/*	protected void addHelloMsg() {
			//Ericfu 11-12 add the local Address
			//buffer.put(localNetAddress.getInetSocketAddress().getAddress().getAddress());
			buffer.put(((NetAddress)Ninaloader.me).getInetSocketAddress().getAddress().getAddress());
			//buffer.put();
			buffer.putInt(((NetAddress)Ninaloader.me).getPort());
		}
		*/
		

		public String toString() {
			//return super.toString() + "/" + POut.toString(objRequestList); // ANSI.color(Color.LIGHTRED,
				return "";																															// String.valueOf(objRequestList.size()));
		}
	}

	class ObjRequest {
		byte[] msgArray;
		boolean bestEffort;
		CB0 cbSent;

		ObjRequest(byte[] msgArray, boolean bestEffort, CB0 cbSent) {
			this.msgArray = msgArray;
			this.bestEffort = bestEffort;
			this.cbSent = cbSent;
		}

		public String toString() {
			return "ObjRequest: msg.length=" + msgArray.length;
		}
	}

	public static void main(String[] args) {
		log.main("Testing object communication");

		ANSI.use(true);

		EL.set(new EL());

		final ObjCommIF objComm = new ObjComm();
		objComm.initServer(AddressFactory.createLocalhost(7777), new CB0() {
			protected void cb(CBResult result) {
				switch (result.state) {
					case OK: {
						
						log.main("OK");
						final ObjMessageIF myMsg = new ObjMessage() {

							static final long serialVersionUID = 1000000001L;

							public int myfield;
						};

						AddressFactory.createResolved("www.baidu.com", 80, new CB1<AddressIF>() {
							protected void cb(CBResult result, AddressIF destAddr) {

								log.info("Sending message...");
								objComm.sendRequestMessage(myMsg, destAddr, new ObjCommRRCB<ObjMessage>() {
									protected void cb(CBResult result, ObjMessage responseMessage,
											AddressIF remoteAddr, Long ts) {

										switch (result.state) {
											case OK: {
												log.info("Everything ok");
												log.info("responseMessage=" + responseMessage);
												log.info("ADDR "+result);
												break;
											}
											case TIMEOUT:
											case ERROR: {
												log.error("Failed: " + result.state + " " + result.what);
												break;
											}
										}
									}
								});
							}
						});
						break;
					}
					case TIMEOUT:
					case ERROR: {
						log.warn(result.toString());
						break;
					}
				}
			}
		});

		EL.get().exit();
		
		EL.get().main();
	}
}
