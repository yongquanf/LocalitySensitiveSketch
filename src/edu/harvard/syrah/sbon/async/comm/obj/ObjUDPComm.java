package edu.harvard.syrah.sbon.async.comm.obj;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.PortUnreachableException;
import java.nio.ByteBuffer;
import java.nio.channels.DatagramChannel;
import java.nio.channels.SelectableChannel;
import java.nio.channels.SelectionKey;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import edu.harvard.syrah.prp.ANSI;
import edu.harvard.syrah.prp.Log;
import edu.harvard.syrah.prp.NetUtil;
import edu.harvard.syrah.prp.POut;
import edu.harvard.syrah.prp.PUtil;
import edu.harvard.syrah.prp.ANSI.Color;
import edu.harvard.syrah.sbon.async.CBResult;
import edu.harvard.syrah.sbon.async.EL;
import edu.harvard.syrah.sbon.async.Loop;
import edu.harvard.syrah.sbon.async.LoopIt;
import edu.harvard.syrah.sbon.async.CallbacksIF.CB0;
import edu.harvard.syrah.sbon.async.CallbacksIF.CB0R;
import edu.harvard.syrah.sbon.async.CallbacksIF.CB1;
import edu.harvard.syrah.sbon.async.CallbacksIF.CB2;
import edu.harvard.syrah.sbon.async.EL.Priority;
import edu.harvard.syrah.sbon.async.comm.AddressFactory;
import edu.harvard.syrah.sbon.async.comm.AddressIF;
import edu.harvard.syrah.sbon.async.comm.Comm;
import edu.harvard.syrah.sbon.async.comm.NetAddress;
import edu.harvard.syrah.sbon.async.comm.UDPComm1;


/**
 * send message based on UDP protocol
 * @author visitor
 *
 */
public class ObjUDPComm extends UDPComm1 implements ObjCommIF {
	static Log log=new Log(ObjUDPComm.class);
	
	
	public static ExecutorService execNina = Executors.newFixedThreadPool(15);

	
	// Callback table for message classes
	@SuppressWarnings("unchecked")
	protected Map<Class, List<ObjCommCB>> callbackTable = new ConcurrentHashMap<Class, List<ObjCommCB>>();

	// Callback table for request/response with message ids
	@SuppressWarnings("unchecked")
	protected Map<Long, ObjCommRRCB> requestResponseCBTable = new ConcurrentHashMap<Long, ObjCommRRCB>();

	// Callback table for request/error with message ids
	@SuppressWarnings("unchecked")
	protected Map<Long, ObjCommRRCB> requestErrorCBTable = new ConcurrentHashMap<Long, ObjCommRRCB>();
	
	// Timeout for all request/response calls (3min)
	private static final long RR_MSG_TIMEOUT = 180000;
	

	// Timeout for the CB that handles a regular ObjComm msg (60s)
	private static final long CB_TIMEOUT = 60 * 1000;
	
	public ObjUDPComm() {
		super(Priority.NORMAL);
		// TODO Auto-generated constructor stub
	}
	
	
	public void initServer(AddressIF bindAddress, CB0 cbInit) {
		super.initServer(bindAddress, cbInit);
		log.debug("Binding to addr=" + bindAddress + " (localAddr=" + localNetAddress + ")");
	}

	@Override
	public void deregisterMessageCB(Class messageClass, ObjCommCB<? extends ObjMessageIF> oldCallback) {
		// TODO Auto-generated method stub
		log.debug("Deregistering callback for class=" + messageClass);
		List<ObjCommCB> callbacks = callbackTable.get(messageClass);
		callbacks.remove(oldCallback);
	}

	@Override
	public void registerMessageCB(Class messageClass,
			ObjCommCB<? extends ObjMessageIF> newCallback) {
		// TODO Auto-generated method stub
		log.debug("Registering new callback for class=" + messageClass);
		List<ObjCommCB> callbacks = callbackTable.get(messageClass);

		if (callbacks == null) {
			callbacks = new LinkedList<ObjCommCB>();
			callbackTable.put(messageClass, callbacks);
		}
		callbacks.add(newCallback);
	}

	
	public void sendErrorMessage(ObjMessageIF message, AddressIF destAddr,
			long requestMsgId, CB0 cbSent) {
		// TODO Auto-generated method stub

	}

	@Override
	public void sendMessage(ObjMessageIF message, AddressIF destAddr, CB0 cbSent) {
		// TODO Auto-generated method stub
		sendMessage(message, destAddr, false, cbSent);
	}

	@Override
	public void sendMessage(ObjMessageIF message, AddressIF destAddr,
			boolean bestEffort,final CB0 cbSent) {
		// TODO Auto-generated method stub
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
		
		ByteBuffer buffer1 = ByteBuffer.allocate /* Direct */ (MIN_BUFFER_SIZE);
		//send the message
		sendMessage(msgArray ,buffer1,destAddr,cbSent);
		
		
		
	}
	
	protected boolean sendMessage(final byte[] msgArray, ByteBuffer buffer, AddressIF destAddr,final CB0 cbSent) {

		//address+port+lengthOfBody+body
		//int totalMsgSize = msgArray.length + Integer.SIZE+((localNetAddress).getInetSocketAddress().getAddress().getAddress()).length+ Integer.SIZE;
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

		if (buffer.remaining() < totalMsgSize){
			cbSent.call(CBResult.ERROR());
			return false;
		}
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
		
		//from address
		// addHelloMsg(buffer,localNetAddress);
		
		buffer.putInt(msgArray.length);
		buffer.put(msgArray);
		// log.debug("(after) buffer.position()=" + buffer.position());

		/*
		 * TODO This is not entirely correct because we should only confirm the
		 * transmission of a message after it has been put on the wire...
		 */

		// Confirm the transmission of the message
		
		sendPacket(buffer,  destAddr) ;
		cbSent.call(CBResult.OK());
		return true;
	}
	
	protected void addHelloMsg1(ByteBuffer buffer,AddressIF addr) {
		buffer.put(((NetAddress)addr).getInetSocketAddress().getAddress().getAddress());
		buffer.putInt(addr.getPort());
	}

	@Override
	public void sendRequestMessage(ObjMessageIF message, AddressIF destAddr,
			ObjCommRRCB<? extends ObjMessageIF> cbResponseMessage) {
		// TODO Auto-generated method stub
		//sendRequestMessage(message, destAddr, cbResponseMessage, null);
		sendRequestMessage(message, destAddr, cbResponseMessage, new ObjCommRRCB(){

			@Override
			protected void cb(CBResult result, Object arg1, Object arg2,
					Object arg3) {
				// TODO Auto-generated method stub
				
			}
			
			
		});
	}

	@Override
	public void sendRequestMessage(ObjMessageIF message, final AddressIF destAddr,
			final ObjCommRRCB<? extends ObjMessageIF> cbResponseMessage,
			ObjCommRRCB<? extends ObjMessageIF> cbErrorMessage) {
		// TODO Auto-generated method stub

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

	@Override
	public void sendResponseMessage(ObjMessageIF message, AddressIF destAddr,
			long requestMsgId, CB0 cbSent) {
		// TODO Auto-generated method stub
		assert requestMsgId > 0 : "msg ids need to be positive. msgId=" + requestMsgId;
		message.setMsgId(requestMsgId);
		message.setResponse(true);
		// log.debug("Sending response msg=" + message);
		// log.debug(" id=" + requestMsgId + " to destAddr=" + destAddr);
		sendMessage(message, destAddr, false, cbSent);
	}

	
	private byte[] marshallMsg(ObjMessageIF message) {
		// log.debug("message=" + message);
		byte[] byteArray = NetUtil.serializeObject(message);
		// log.debug("message2=" + Util.deserializeObject(byteArray));
		return byteArray;
	}
	
	protected Object unmarshallMsg(byte[] msgArray) {
		Object msg = NetUtil.deserializeObject(msgArray);
		// log.debug("message=" + msg);
		return msg;
	}
	
	/**
	 * perform the gossip process
	 * @param msg
	 * @param timestamp
	 */
	protected void performCallback(final AddressIF remoteAddr,final ObjMessageIF msg, final long timestamp) {
		log.debug("msg=" + msg);
		

		// Is this a response message?
		if (!msg.isResponse()) {
			List<ObjCommCB> callbacks = callbackTable.get(msg.getClass());
			if (callbacks == null) {
				log.warn("No callback registered for message class=" + msg.getClass() + ". Ignoring.");
				return;
			}
			 log.debug("numCBs=" + callbacks.size() );

			LoopIt<ObjCommCB> cbLoop = new LoopIt<ObjCommCB>("ObjCommCBLoop", callbacks,
					new CB2<ObjCommCB, CB0R<Boolean>>() {
						protected void cb(CBResult result, final ObjCommCB objCommCB,
								final CB0R<Boolean> cbRecursion) {
							log.debug("Performing cb for msg=" + msg);
							log.debug(" remoteAddr=" + remoteAddr);
							//log.debug(" objCommCB=" + objCommCB);
							
							/**
							 * multiple thread
							 */
							execNina.execute(new Runnable(){
								@Override
								public void run() {
									// TODO Auto-generated method stub
									objCommCB.call(CBResult.OK(), msg, remoteAddr, timestamp, new CB1<Boolean>(
											CB_TIMEOUT) {
										protected void cb(CBResult result, Boolean handled) {
											switch (result.state) {
												case OK: {
													if (handled) {
														
														 log.debug("Message handled by callback.");
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
							
							callback.call(CBResult.OK(), msg, remoteAddr, timestamp);
						} else {
							log.warn("Callback cb=" + callback + " already cancelled");
							
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
						
					}
				});
			}
		}
	}
	
	
	 protected ReadConnHandler createReadConnHandler(SelectableChannel channel,
			NetAddress remoteAddress){
		return new ReadConnHandler(channel, remoteAddress);
	}
	
	//read a message
	class ReadConnHandler extends UDPComm1.ReadConnHandler {
		
	protected final Log log = new Log(ReadConnHandler.class, Color.LIGHTYELLOW);

		private long timestamp;
				
		
	ReadConnHandler(SelectableChannel channel, NetAddress destAddr) {
		super(channel, destAddr);
	}

	public Boolean cb(CBResult result, SelectionKey key) {

		timestamp = System.nanoTime();
		
		
		if (key.isValid() && key.isReadable()) {
			log.debug("Reading from socket... buffer=" + buffer);

			// final PTimer pt = new PTimer();

			channel = key.channel();
			DatagramChannel datagramChannel = (DatagramChannel) channel;

			if (buffer.position() != 0) {
				log.warn("There is already data in the UDP buffer");
				return false;
			}

			InetSocketAddress inetSocketAddress = null;
			try {
				inetSocketAddress = (InetSocketAddress) datagramChannel.receive(buffer);
				log.debug("Read count=" + buffer.position() + " bytes.");
			} catch (PortUnreachableException e) {
				log.debug("Error reading from socket. Ignoring. PortUnreachable: " + e);
				buffer.clear();
				return true;
			} catch (IOException e) {
				log.warn("Error reading from socket. Ignoring: " + e);
				buffer.clear();
				return true;
			}

			/**
			 * Ericfu,
			 */
			if( inetSocketAddress == null ){
				buffer.clear();
				return true;
			}
			assert inetSocketAddress != null : "No packet found.";
			assert buffer.position() != 0 : "Odd. Received a selector key and only read 0 bytes. This is a bug?";

			long newNanoTS = 0;
			if (keepNanoTS)
				newNanoTS = System.nanoTime();
			final long nanoTS = newNanoTS;

			buffer.flip();

			final ByteBuffer copyBuffer = ByteBuffer.allocate(buffer.remaining());
			copyBuffer.put(buffer);
			copyBuffer.flip();
			buffer.clear();

			
			
			
			
			
			
			// pt.lap(log, "UDP (before adr res) took");
			AddressFactory.createResolved(inetSocketAddress.getAddress().getAddress(),
					inetSocketAddress.getPort(), new CB1<AddressIF>("ResolveAddr1") {

						protected void cb(CBResult result, AddressIF resolvedRemoteAddr) {
							switch (result.state) {
								case OK: {
									// pt.lap(log, "UDP (after adr res) took");
									remoteAddr = (NetAddress) resolvedRemoteAddr;
									log.debug("remoteAddr=" + remoteAddr + " callbacks.size="
											+ serverCallbacks.size());
									//receive the message and decode it
									
									int currentMsgSize = copyBuffer.getInt();

									if (currentMsgSize <= 0) {
										log.warn("Connection from remoteAddr=" + remoteAddr + " had msgSize="
												+ currentMsgSize + ". Closing.");
										
										return;
									}

									assert currentMsgSize > 0 : "currentMsgSize=" + currentMsgSize;
									//log.debug("before: buffer=" + buffer + " currentMsgSize=" + currentMsgSize);

									// Can we read a full message?
									if (copyBuffer.limit() < currentMsgSize + 4) {
										// Prepare the buffer for more reading
										copyBuffer.position(copyBuffer.limit());
										copyBuffer.limit(copyBuffer.capacity());
										break;								
									}

									byte[] msgArray = new byte[currentMsgSize];
									copyBuffer.get(msgArray);

									// Compact the buffer
									int limit = copyBuffer.limit();
									int pos = copyBuffer.position();
									copyBuffer.compact();
									copyBuffer.limit(limit - pos);

									Object msgObject = unmarshallMsg(msgArray);

									if (msgObject == null) {
										log.warn("No valid msgObject received from remoteAddr=" + remoteAddr
												+ ". Ignoring.");
										
										return;
									}

									assert msgObject instanceof ObjMessageIF : "Received a message that is not a ObjMessageIF.";
									 log.debug("msgObj=" + msgObject.toString());

									ObjMessageIF msg = (ObjMessageIF) msgObject;
									performCallback(remoteAddr ,msg, timestamp);																
										
									/*// Prepare the buffer for more reading
									buffer.position(buffer.limit());
									buffer.limit(buffer.capacity());	
									*/
									
									
									break;
								}
								case TIMEOUT:
								case ERROR: {
									log.error(result.toString());
									break;
								}
							}
						}
					});
		}
		return true;
	}
	
	
	
}
	
	/**
	 * main
	 * @param args
	 */
	public static void main(String[] args){
		
		log.main("Testing object communication");

		ANSI.use(true);

		EL.set(new EL());

		final ObjUDPComm objComm = new ObjUDPComm();
		AddressIF addr=AddressFactory.createServer(55523);
		
		objComm.initServer( addr, new CB0() {
			protected void cb(CBResult result) {
				switch (result.state) {
					case OK: {
						
						
						final CollectRequestMsg myMsg=new CollectRequestMsg(AddressFactory.createUnresolved("192.168.0.3", 55524));
						
						byte[] b=objComm.marshallMsg(myMsg);
						log.main("OK");
					/*	AddressFactory.createResolved("192.168.0.3", 80, new CB1<AddressIF>() {
							protected void cb(CBResult result, AddressIF destAddr) {*/

						if(true){
								log.debug("Sending message...");
								
								
								objComm.sendRequestMessage(myMsg,AddressFactory.createUnresolved("192.168.0.3", 55524), new ObjCommRRCB<ObjMessage>() {
									protected void cb(CBResult result, ObjMessage responseMessage,
											AddressIF remoteAddr, Long ts) {

										switch (result.state) {
											case OK: {
												log.main("Everything ok");
												log.main("responseMessage=" + responseMessage);
												log.main("ADDR "+result);
												break;
											}
											case TIMEOUT:
											case ERROR: {
												log.main("Failed: " + result.state + " " + result.what);
												break;
											}
										}
									}
								});
						}
						/*	}
						});*/
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
