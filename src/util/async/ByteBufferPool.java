package util.async;

/*
 * Copyright (c) 2003, The Regents of the University of California, through
 * Lawrence Berkeley National Laboratory (subject to receipt of any required
 * approvals from the U.S. Dept. of Energy). All rights reserved.
 */

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;

/**
 * Efficient thread-safe pool of {@link ByteBuffer}s for high performance NIO
 * applications. Using a buffer pool can drastically reduce memory allocation,
 * memory copying and garbage collection by taking buffers from the pool when
 * needed, and recycling them back to the pool when they are no more needed.
 * <p>
 * There is a trade-off here: The improved performance a pool promises comes at
 * the expense of larger overall memory footprint, since buffers in the pool are
 * not subject to intermediate garbage collection (unless the entire pool is no
 * more referenced or cleared, of course).
 * <p>
 * Once you have taken a buffer via the <code>take</code> method from the
 * pool, you can modify it in any way desired. Once you have recycled a buffer
 * via the <code>put</code> method back to the pool you MUST NOT modify it
 * anymore, NOT EVEN it's mark, position or limit, whether directly or
 * indirectly!
 * <p>
 * On <code>put</code> the pool will ignore buffers with
 * <code>buffer.capacity() &lt; bufferCapacity</code> or when the aggregate
 * capacity of all buffers in the pool would become larger than
 * <code>maxPoolCapacity</code>.
 * <p>
 * On <code>take</code> the pool will return a cleared buffer with at least
 * the given <code>bufferCapacity</code>, which will be a direct or heap
 * buffer, depending on the <code>preferDirect</code> flag. In any case, the
 * returned buffer will have the given <code>byteOrder</code>, or BIG_ENDIAN
 * byte order if <code>byteOrder</code> is null.
 * <p>
 * If empty on <code>take</code> the pool will create a new buffer and return
 * that. (The buffer pool is smart in avoiding allocating too many direct
 * buffers and in its preference strategies).
 * <p>
 * Hint: At least in jdk-1.4.2 the total maximum amount of direct buffers that
 * may be allocated is 64MB by default. You can change this via
 * <code>java -XX:MaxDirectMemorySize=256m</code>. See bug 4879883 on Java
 * Bug Parade. See http://iais.kemsu.ru/odocs/javax/JSDK.Src/java/nio/Bits.java
 * 
 * @author whoschek@lbl.gov
 * @author $Author: drc915 $
 * @version $Revision: 1.8 $, $Date: 2007/12/30 16:54:15 $
 */
public class ByteBufferPool {
	
	private final List<ByteBuffer> heapBuffers;
	private final List<ByteBuffer> directBuffers;
	private final int bufferCapacity;
	private final long maxPoolCapacity;
	private final boolean preferDirect;
	private final ByteOrder byteOrder;
	private long currentPoolCapacity;
	
	// statistics
	protected long nrAllocations;
	protected long nrAllocated;
	protected long nrTakes;
	protected long nrTakesReused;
	protected long nrTakenBytes;
	protected long nrReusedBytes;
	protected long nrReplacingPuts;

	//private static final Log log = new Log(ByteBufferPool.class);
	private static final boolean DEBUG = true;
	
	
	private HashMap<ByteBuffer, String> bufferTrace = 
		new HashMap<ByteBuffer, String>();

	/**
	 * Creates a new pool with the given properties.
	 */
	public ByteBufferPool(long maxPoolCapacity, int bufferCapacity, boolean preferDirect, ByteOrder byteOrder) {
		if (bufferCapacity <= 0) throw new IllegalArgumentException("bufferCapacity must be > 0");
		if (maxPoolCapacity < 0) throw new IllegalArgumentException("maxPoolCapacity must be >= 0");
		this.maxPoolCapacity = maxPoolCapacity;
		this.bufferCapacity = bufferCapacity;
		this.preferDirect = preferDirect;
		this.byteOrder = (byteOrder == null ? ByteOrder.BIG_ENDIAN : byteOrder);
		this.heapBuffers = new LinkedList<ByteBuffer>();
		this.directBuffers = new LinkedList<ByteBuffer>();
		
		this.clear();
	}
	
	/** 
	 * Returns the buffer capacity.
	 */
	public int getBufferCapacity() {
		return this.bufferCapacity;
	}
	
	/**
	 * Returns the byte order used when returning buffers.
	 */
	public ByteOrder getByteOrder() {
		return this.byteOrder;
	}
	
	/** 
	 * Returns the maximum pool capacity.
	 */
	public long getMaxPoolCapacity() {
		return this.maxPoolCapacity;
	}

	/** 
	 * Returns whether or not this pool prefers to return direct or heap buffers.
	 */
	public boolean getPreferDirect() {
		return this.preferDirect;
	}
	
	/**
	 * Recycles a buffer back into the pool (adds it to the pool).
	 * @param buffer the buffer to put into the pool.
	 */
	synchronized public void put(ByteBuffer buffer) {
		if (buffer == null || buffer.capacity() < this.bufferCapacity) {
			return; // ignore
		}
		
		if (this.currentPoolCapacity + buffer.capacity() > this.maxPoolCapacity) {
			if (buffer.isDirect() != preferDirect) return; // ignore
			
			// try to drop a non-preferred buffer and see if new buffer fits
			List<ByteBuffer> dropBuffers = buffer.isDirect() ? heapBuffers : directBuffers;
			if (dropBuffers.size() == 0) return; // ignore
			
			int cap = (dropBuffers.get(dropBuffers.size()-1)).capacity();
			//int cap = this.bufferCapacity;
			if (this.currentPoolCapacity - cap + buffer.capacity() > this.maxPoolCapacity) {
				return; // ignore
			}
			else {
				dropBuffers.remove(dropBuffers.size()-1);
				this.currentPoolCapacity -= cap;
				this.nrReplacingPuts++;
			}
		}
				
		List<ByteBuffer> buffers = buffer.isDirect() ? directBuffers : heapBuffers;
		if (DEBUG) {
			for (ByteBuffer o : buffers){
				
				ByteBuffer bb = o;
				if (bb == buffer){
					System.err.print("@"+System.currentTimeMillis()+"\tReturned from:\n"+bufferTrace.get(buffer));
					//throw new RuntimeException("Returning already-returned buffer!");
				}
				/*try{
					if (bb.isDirect()){
						if (bb.arrayOffset() == buffer.arrayOffset()){
							throw new RuntimeException("Returning already-returned buffer!");
						}
					} else {
						if (bb.array().hashCode() == buffer.array().hashCode()){
								throw new RuntimeException("Returning already-returned buffer!");
						}
					}
				} catch (Exception e2){
					if (bb.limit()==buffer.limit()){
						boolean same = true;
						while (bb.hasRemaining()){
							if (bb.get()==buffer.get()){
								same = false;
								break;
							}
						}
						if (same){
							throw new RuntimeException("Returning already-returned buffer!");
						}
					}
							// this should not happen
				}*/
			}
			String s = "@"+System.currentTimeMillis() + "\n";
			for (StackTraceElement e : Thread.getAllStackTraces().get(Thread.currentThread())){
				s += e.toString() + "\n";
			}
			bufferTrace.put(buffer, s);
		}
		
		
				
		buffers.add(0, buffer);
		this.currentPoolCapacity += buffer.capacity();
	}

	/**
	 * Returns a cleared buffer from the pool, or creates and returns a new
	 * buffer.
	 * 
	 * @return a buffer from the pool.
	 */
	synchronized public ByteBuffer take() {
		ByteBuffer buffer = null;
//		synchronized (this) {
			this.nrTakes ++;
			List<ByteBuffer> buffers = preferDirect ? directBuffers : heapBuffers;
			
			if (buffers.size() > 0) { // try preferred buffers
				buffer = buffers.get(0);
			}
			else { // try non-preferred buffers
				buffers = preferDirect ? heapBuffers : directBuffers;
				if (buffers.size() > 0) {
					buffer = buffers.get(0);
				}
			}
			if (buffer != null) {
				buffers.remove(0);
				this.currentPoolCapacity -= buffer.capacity();
				this.nrReusedBytes += buffer.capacity();
				this.nrTakenBytes += buffer.capacity();
				this.nrTakesReused++;
			}			
//		}
		
		if (buffer == null) {
			boolean allocateDirect;
//			synchronized (this) {
				// fix for vm bugs limiting max amount of direct buffer mem that may
				// be allocated
				allocateDirect = this.preferDirect
						&& nrAllocated + this.bufferCapacity > maxPoolCapacity ? false
						: this.preferDirect;
				this.nrAllocated += this.bufferCapacity;
				this.nrTakenBytes += this.bufferCapacity;
				this.nrAllocations++;
//			}
			buffer =this.createBuffer(this.bufferCapacity, allocateDirect);
		}
		
		buffer.clear();
		if (buffer.order() != this.byteOrder) {
			buffer.order(this.byteOrder);
		}
		return buffer;
	}
	
	/**
	 * Override this method to create custom bytebuffers.
	 */
	protected ByteBuffer createBuffer(int capacity, boolean direct) {
		if (direct) {
			try {
				return ByteBuffer.allocateDirect(capacity);
			} catch (OutOfMemoryError e) {
				//log.warn("OutOfMemoryError: No more direct buffers available; trying heap buffer instead");
			} 
		}
		return ByteBuffer.allocate(capacity);
	}
	
	/**
	 * Removes all buffers from the pool.
	 */
	synchronized public void clear() {
		this.heapBuffers.clear();
		this.directBuffers.clear();
		this.currentPoolCapacity = 0;
		
		this.nrAllocations = 0;
		this.nrAllocated = 0;
		this.nrTakes = 0;
		this.nrTakesReused = 0;
		this.nrTakenBytes = 0;
		this.nrReusedBytes = 0;		
		this.nrReplacingPuts = 0;		
	}

	/**
	 * Returns a summary statistics representation of the receiver.
	 */
	public synchronized String toString() {
		String s = this.getClass().getName() + ": ";
		s += "nrAllocated=" + mb(nrAllocated) + " MB";
		s += ", nrAllocations=" + nrAllocations;
		s += ", nrTakes=" + nrTakes;
		s += ", nrTakesReused=" + nrTakesReused;
		s += ", nrReplacingPuts=" + nrReplacingPuts;
		s += ", nrTakenBytes=" + mb(nrTakenBytes) + " MB";
		s += ", nrReusedBytes=" + mb(nrReusedBytes) + " MB";
		s += ", maxPoolCapacity=" + mb(maxPoolCapacity) + " MB";
		s += ", currentPoolCapacity=" + mb(currentPoolCapacity) + " MB";
		s += " --> EFFICIENCY=" + (100.0f * nrReusedBytes / nrTakenBytes) + " %";
		return s;
	}
	
	private static float mb(long bytes) { 
		return bytes / (1024.0f * 1024.0f);
	}

	public String getReturnTrace(ByteBuffer bb) {
		return bufferTrace.get(bb);
	}
}

