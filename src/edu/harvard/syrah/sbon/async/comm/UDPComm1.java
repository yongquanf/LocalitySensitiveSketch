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
import edu.harvard.syrah.sbon.async.comm.Comm.ConnectConnHandler;
import edu.harvard.syrah.sbon.async.comm.Comm.WriteConnHandler;

abstract public  class UDPComm1  extends Comm implements UDPCommIF {
	private static final Log log = new Log(UDPComm.class, Color.LIGHTYELLOW);

	protected List<UDPCommCB> serverCallbacks = new LinkedList<UDPCommCB>();

	protected Priority priority;

	public boolean keepNanoTS = false;

	public UDPComm1(Priority priority) {
		this(priority, false);
	}

	public UDPComm1(Priority priority, boolean keepNanoTS) {
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
		byteBuffer.clear();
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

	protected abstract ReadConnHandler createReadConnHandler(SelectableChannel channel,
			NetAddress remoteAddress);
	
	/*{
		return new ReadConnHandler(channel, remoteAddress);
	}*/

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
	
	public abstract class ReadConnHandler extends Comm.ReadConnHandler {
		
		protected UDPCommCB udpCommCB = null;
		
		protected ReadConnHandler(SelectableChannel channel, NetAddress remoteAddr) {	super(channel, remoteAddr);	}

		protected Boolean cb(CBResult result, SelectionKey key) {
			
			return true;
		}
	}
	

}
