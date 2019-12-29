/*
 * SBON
 * 
 * @author Last modified by $Author: prp $
 * @version $Revision: 1.1 $ on $Date: 2007/06/04 17:55:41 $
 * @since Jan 19, 2005
 */
package edu.harvard.syrah.sbon.async.comm;

import java.io.IOException;
import java.net.DatagramSocket;
import java.net.InetSocketAddress;
import java.net.PortUnreachableException;
import java.nio.ByteBuffer;
import java.nio.channels.DatagramChannel;
import java.nio.channels.SelectableChannel;
import java.nio.channels.SelectionKey;
import java.util.LinkedList;
import java.util.List;

import edu.harvard.syrah.prp.Log;
import edu.harvard.syrah.prp.ANSI.Color;
import edu.harvard.syrah.sbon.async.CBResult;
import edu.harvard.syrah.sbon.async.EL;
import edu.harvard.syrah.sbon.async.Loop;
import edu.harvard.syrah.sbon.async.CallbacksIF.CB0;
import edu.harvard.syrah.sbon.async.CallbacksIF.CB1;
import edu.harvard.syrah.sbon.async.CallbacksIF.CB2;
import edu.harvard.syrah.sbon.async.EL.Priority;

/**
 * 
 * This communication module implements communication with UDP packets
 * 
 */
public class UDPComm extends Comm implements UDPCommIF {
	private static final Log log = new Log(UDPComm.class, Color.LIGHTYELLOW);

	protected List<UDPCommCB> serverCallbacks = new LinkedList<UDPCommCB>();

	protected Priority priority;

	public boolean keepNanoTS = false;

	public UDPComm(Priority priority) {
		this(priority, false);
	}

	public UDPComm(Priority priority, boolean keepNanoTS) {
		this.priority = priority;
		this.keepNanoTS = keepNanoTS;
	}

	/*
	 * @see edu.harvard.syrah.sbon.comm.UDPCommIF#sendPacket(byte[],
	 *      edu.harvard.syrah.sbon.comm.AddressIF)
	 */
	public void sendPacket(ByteBuffer byteBuffer, AddressIF remoteAddr) {
		log.debug("Sending UDP packet for remoteAddr=" + remoteAddr);

		assert remoteAddr != null : "Remote address is null";
		assert remoteAddr instanceof NetAddress : "Can only send packets to NetAddresses.";

		NetAddress remoteNetAddress = (NetAddress) remoteAddr;

		DatagramChannel datagramChannel = connect(remoteNetAddress);

		sendPacket(datagramChannel, byteBuffer);
		
		try {
			datagramChannel.close();
		} catch (IOException e) {
			log.error("Couldn't close UDP channel: " + e);
		}
	}

	private void sendPacket(DatagramChannel datagramChannel, ByteBuffer byteBuffer) {
		byteBuffer.flip();
		int toSent = byteBuffer.remaining();

		try {
			int bytesSent = datagramChannel.write(byteBuffer);
			assert bytesSent == toSent;
		} catch (IOException e) {
			log.error("Could not send UDP packet:" + e);
		}
		// log.debug("Finished sending.");
	}

	private DatagramChannel connect(NetAddress remoteNetAddress) {
		DatagramChannel datagramChannel = null;
		try {
			datagramChannel = DatagramChannel.open();
			datagramChannel.configureBlocking(false);
			datagramChannel.connect(remoteNetAddress.getInetSocketAddress());
		} catch (IOException e) {
			log.error("Could not create a new DatagramChannel: " + e);
		}
		log.debug("remoteAddr=" + remoteNetAddress + " datagramChannel=" + datagramChannel);
		return datagramChannel;
	}

	public void sendPacket(ByteBuffer data, AddressIF destAddr, final UDPCommCB cb) {
		assert destAddr instanceof NetAddress;
		NetAddress remoteNetAddress = (NetAddress) destAddr;
		DatagramChannel datagramChannel = connect(remoteNetAddress);
		
		log.debug("Sending UDP packet to remoteNetAddress=" + remoteNetAddress);

		final ReadConnHandler readConnHandler = createReadConnHandler(datagramChannel, remoteNetAddress);
		readConnHandler.udpCommCB = cb;
		cb.registerCancelledCB(new CB0() {
			protected void cb(CBResult result) {
				readConnHandler.deregister();
				readConnHandler.destruct();
			}
		});
		readConnHandler.register();

		sendPacket(datagramChannel, data);
	}

	/*
	 * @see edu.harvard.syrah.sbon.comm.UDPCommIF#registerPacketCallback(edu.harvard.syrah.sbon.comm.UDPCommCB)
	 */
	public void registerPacketCallback(UDPCommCB cb) {
		assert (!serverCallbacks.contains(cb));
		serverCallbacks.add(cb);
	}

	/*
	 * @see edu.harvard.syrah.sbon.comm.UDPCommIF#deregisterPacketCallback(edu.harvard.syrah.sbon.comm.UDPCommCB)
	 */
	public void deregisterPacketCallback(UDPCommCB cb) {
		assert (serverCallbacks.contains(cb));
		serverCallbacks.remove(cb);
		log.debug("Deregistered UDPCommCB=" + cb + " callbacks.size=" + serverCallbacks.size());
	}

	protected AcceptConnHandler getAcceptConnHandler(NetAddress localAddress, CB0 cbHandler) {
		return new AcceptConnHandler(localAddress, cbHandler);
	}

	protected ConnectConnHandler getConnectConnHandler(NetAddress remoteAddress, long timeout,
			CB0 cbHandler) {
		return null; /* not used */
	}

	protected ReadConnHandler createReadConnHandler(SelectableChannel channel,
			NetAddress remoteAddress) {
		return new ReadConnHandler(channel, remoteAddress);
	}

	protected WriteConnHandler createWriteConnHandler(SelectableChannel channel, NetAddress remoteAddr) {
		return null; /* not used */
	}

	/*
	 * Callback to listen for incoming UDP packets
	 */
	class AcceptConnHandler extends Comm.AcceptConnHandler {
		private final Log log = new Log(AcceptConnHandler.class, Color.LIGHTYELLOW);

		AcceptConnHandler(AddressIF bindAddress, CB0 cbHandler) {
			super(cbHandler);
			log.debug("Listening for UDP packets...");

			assert (bindAddress instanceof NetAddress);

			NetAddress bindNetAddress = (NetAddress) bindAddress;

			try {
				DatagramChannel datagramChannel = DatagramChannel.open();
				datagramChannel.configureBlocking(false);

				ReadConnHandler readConnHandler = createReadConnHandler(datagramChannel, bindNetAddress);
				readConnHandler.register();

				// Create the datagram socket
				DatagramSocket datagramSocket = datagramChannel.socket();
				datagramSocket.bind(bindNetAddress.getInetSocketAddress());

			} catch (IOException e) {
				log.warn("Could not create server socket: " + e);
				cbHandler.call(CBResult.ERROR(e.toString()));
			}
			cbHandler.callOK();
		}

		public Boolean cb(CBResult result, SelectionKey key) { return true; }
	}

	/*
	 * Callback for an incoming packet
	 */
class ReadConnHandler extends Comm.ReadConnHandler {
		protected final Log log = new Log(ReadConnHandler.class, Color.LIGHTYELLOW);

		protected UDPCommCB udpCommCB = null;

		ReadConnHandler(SelectableChannel channel, NetAddress destAddr) {
			super(channel, destAddr);
		}

		public Boolean cb(CBResult result, SelectionKey key) {

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

										if (udpCommCB != null) {

											if (channel.isOpen()) {
												udpCommCB.deregisterAllCancelledCBs();
												deregister();
												destruct();

												try {
													channel.close();
												} catch (IOException e) {
													log.error("Could not close the channel: " + e);
												}
											}

											// pt.lap(log, "UDPPacketHandling before cb timer reg
											// took");
											EL.get().registerTimerCB(
													new CB0("UDPCommCB: remoteAddr=" + remoteAddr) {
														protected void cb(CBResult result) {
															// pt.stop(log, "UDPCommCB (before call) took");
															udpCommCB.call(CBResult.OK(), copyBuffer, remoteAddr, nanoTS,
																	new CB1<Boolean>() {
																		protected void cb(CBResult result, Boolean handled) {
																			switch (result.state) {
																				case OK: {
																					if (!handled) {
																						log.warn("UDP packet from remoteAddr=" + remoteAddr
																								+ " was not handled by UDPCB.");
																					}
																					// pt.stop(log, "UDPCB took");
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
													}, priority);
										} else {
											// pt.lap(log, "UDPPacketHandling up to cb (remoteAddr=" +
											// remoteAddr + ") took");
											UDPCommCB firstCB = serverCallbacks.get(0);
											Loop<UDPCommCB> cbLoop = new Loop<UDPCommCB>("UDPCBLoop", firstCB,
													new CB2<UDPCommCB, CB1<UDPCommCB>>() {
														protected void cb(CBResult result, final UDPCommCB udpCommCB,
																final CB1<UDPCommCB> cbRecursion) {

															// log.debug("UDPCommCB=" + udpCommCB);
															// log.debug("Trying udpCommCB.index=" +
															// serverCallbacks.indexOf(udpCommCB));
															// pt.lap(log, "UDPCommCBs (before call) took");
															udpCommCB.call(CBResult.OK(), copyBuffer, remoteAddr, nanoTS,
																	new CB1<Boolean>() {
																		protected void cb(CBResult result, Boolean handled) {
																			switch (result.state) {
																				case OK: {
																					if (handled) {
																						// log.debug("Packet handled by
																						// callback.");
																						// pt.stop(log, "UDPPacketHandling
																						// (remoteAddr=" + remoteAddr + ")
																						// took");

																					} else {
																						copyBuffer.position(0);
																						// log.debug("Packet not handled by
																						// callback. Continuing...");

																						int cbIndex = serverCallbacks.indexOf(udpCommCB);
																						// log.debug("Tried cbIndex=" +
																						// cbIndex);
																						if ((cbIndex + 1) >= serverCallbacks.size()) {
																							log.error("No valid callback found for UDP packet. remoteAddr="
																									+ remoteAddr + ". buffer=" + copyBuffer);
																						} else {
																							UDPCommCB nextCB = serverCallbacks.get(cbIndex + 1);
																							cbRecursion.call(result, nextCB);
																						}
																					}
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
													}, priority);
											cbLoop.execute();
										}
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
}
