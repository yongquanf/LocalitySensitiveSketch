/*
 * @author Last modified by $Author: prp $
 * @version $Revision: 1.2 $ on $Date: 2007/08/14 11:18:13 $
 * @since Jan 28, 2005
 */

package edu.harvard.syrah.sbon.async;

import java.util.LinkedList;
import java.util.List;

//import edu.NUDT.pdl.Nina.Ninaloader;

/**
 * 
 * Asynchronous callback interface
 * 
 */

public interface CallbacksIF {

	
	//Ericfu
	
	
	@SuppressWarnings("unchecked")
	public abstract class AbstractCB implements Comparable {

		private long timeout = 0;
		private String name = null;
		protected boolean cancelled = false;
		private List<CB0> cancelledCBs = null;
		protected CB0 cbTimeout = null;

		protected long ts;

		public AbstractCB() { /* empty */
		}

		public AbstractCB(long timeout) {
			setTimeout("CBTimeout", timeout);
		}

		public AbstractCB(String name) {
			this.name = name;
		}

		public abstract void callCBResult(CBResult result);

		protected boolean checkCancelled(CBResult result) {
			if (cbTimeout != null) {
				EL.get().deregisterTimerCB(cbTimeout);
				cbTimeout = null;
			}

			boolean returnResult = cancelled;

			if (result.state == CBResult.CBState.ERROR || result.state == CBResult.CBState.TIMEOUT) {
				cancelled = true;
			}

			return returnResult;
		}

		public void setTimeout(String name, final long timeout) {

			if (timeout == 0)
				return;

			this.timeout = timeout;

			// Register the timer for the timeout
			cbTimeout = EL.get().registerTimerCB(timeout, new CB0(name) {
				public void cb(CBResult result) {
					if (!cancelled) {
						// Call the timeout callback
						AbstractCB.this.callCBResult(CBResult.TIMEOUT(timeout + "ms"));
						AbstractCB.this.cancel();
					}
					AbstractCB.this.cbTimeout = null;
				}
			});
		}

		public void cancel() {
			cancelled = true;
			if (cbTimeout != null) {
				EL.get().deregisterTimerCB(cbTimeout);
				cbTimeout = null;
			}
			if (cancelledCBs != null) {
				for (CB0 cancelledCB : cancelledCBs)
					cancelledCB.callOK();
			}
		}

		public void cancel(CB0 cb) {
			cancel();
			cb.callOK();
		}

		public boolean isCancelled() {
			return cancelled;
		}

		public void registerCancelledCB(CB0 cancelledCB) {
			if (cancelledCBs == null)
				cancelledCBs = new LinkedList<CB0>();
			cancelledCBs.add(cancelledCB);
		}

		public void deregisterCancelledCB(CB0 cancelledCB) {
			cancelledCBs.remove(cancelledCB);
		}

		public void deregisterAllCancelledCBs() {
			cancelledCBs.clear();
		}

		public boolean hasTimeout() {
			return cbTimeout != null;
		}

		public String toString() {
			String cbName = super.toString();
			return (ts != 0 ? ts : "") + name == null ? cbName.substring(cbName.lastIndexOf('.') + 1)
					: name;
		}

		public long getTS() {
			return ts;
		}

		public int compareTo(Object obj) {
			assert obj instanceof AbstractCB;
			AbstractCB cmpCB = (AbstractCB) obj;

			// log.debug("this=" + this + " cmpEvent=" + cmpEvent);

			// Equality means that this is the same object
			if (this == cmpCB) {
				// log.debug("equal");
				return 0;
			}

			/*
			 * >=
			 */

			int comparison = (ts > cmpCB.ts) ? 1 : -1;
			// log.debug("comparison=" + comparison);

			return comparison;
		}

	}

	public abstract class CB0 extends AbstractCB {
		public CB0() { /* emtpty */
		}

		public CB0(long timeout) {
			super(timeout);
		}

		public CB0(String name) {
			super(name);
		}

		protected abstract void cb(CBResult result);

		public void callCBResult(CBResult result) {
			if (!checkCancelled(result))
			cb(result);
		}

		public void callOK() {
			if (!checkCancelled(CBResult.OK()))
				cb(CBResult.OK());
		}

		public void callERROR() {
			if (!checkCancelled(CBResult.ERROR()))
				cb(CBResult.ERROR());
		}

		public void call(final CBResult result) {
		
					if (!checkCancelled(result))
						cb(result);						


		}
	}
	
	public abstract class CB1<T1> extends AbstractCB {
		public CB1() { /* emtpty */ }

		public CB1(String name) {
			super(name);
		}
		
		public CB1(long timeout) {
			super(timeout);
		}

		protected abstract void cb(CBResult result, T1 arg1);
		
		public void callCBResult(CBResult result) {
			cb(result, null);
		}

		public void call(CBResult result, T1 arg1) {
			if (!checkCancelled(result))
				cb(result, arg1);
		}
	}

	public abstract class CB2<T1, T2> extends AbstractCB {
		public CB2() { /* emtpty */
		}

		public CB2(long timeout) {
			super(timeout);
		}

		protected abstract void cb(CBResult result, T1 arg1, T2 arg2);

		public void callCBResult(CBResult result) {
			cb(result, null, null);
		}

		public void call(CBResult result, T1 arg1, T2 arg2) {
			if (!checkCancelled(result))
				cb(result, arg1, arg2);
		}
	}

	public abstract class CB3<T1, T2, T3> extends AbstractCB {
		public CB3() { /* emtpty */
		}

		public CB3(long timeout) {
			super(timeout);
		}
		
		public CB3(String name) {
			super(name);
		}

		protected abstract void cb(CBResult result, T1 arg1, T2 arg2, T3 arg3);

		public void callCBResult(CBResult result) {
			cb(result, null, null, null);
		}

		public void call(CBResult result, T1 arg1, T2 arg2, T3 arg3) {
			if (!checkCancelled(result))
				cb(result, arg1, arg2, arg3);
		}
	}

	public abstract class CB4<T1, T2, T3, T4> extends AbstractCB {
		public CB4() { /* emtpty */
		}

		public CB4(long timeout) {
			super(timeout);
		}

		public CB4(String name) {
			super(name);
		}

		protected abstract void cb(CBResult result, T1 arg1, T2 arg2, T3 arg3, T4 arg4);

		public void callCBResult(CBResult result) {
			cb(result, null, null, null, null);
		}

		public void call(CBResult result, T1 arg1, T2 arg2, T3 arg3, T4 arg4) {
			if (!checkCancelled(result))
				cb(result, arg1, arg2, arg3, arg4);
		}
	}

	public abstract class CB5<T1, T2, T3, T4, T5> extends AbstractCB {
		public CB5() { /* emtpty */
		}

		public CB5(long timeout) {
			super(timeout);
		}

		protected abstract void cb(CBResult result, T1 arg1, T2 arg2, T3 arg3, T4 arg4, T5 arg5);

		public void callCBResult(CBResult result) {
			cb(result, null, null, null, null, null);
		}

		public void call(CBResult result, T1 arg1, T2 arg2, T3 arg3, T4 arg4, T5 arg5) {
			if (!checkCancelled(result))
				cb(result, arg1, arg2, arg3, arg4, arg5);
		}
	}

	public abstract class CB6<T1, T2, T3, T4, T5, T6> extends AbstractCB {
		public CB6() { /* emtpty */
		}

		public CB6(long timeout) {
			super(timeout);
		}

		protected abstract void cb(CBResult result, T1 arg1, T2 arg2, T3 arg3, T4 arg4, T5 arg5, T6 arg6);

		public void callCBResult(CBResult result) {
			cb(result, null, null, null, null, null, null);
		}

		public void call(CBResult result, T1 arg1, T2 arg2, T3 arg3, T4 arg4, T5 arg5, T6 arg6) {
			if (!checkCancelled(result))
				cb(result, arg1, arg2, arg3, arg4, arg5, arg6);
		}
	}

	public abstract class CB7<T1, T2, T3, T4, T5, T6, T7> extends AbstractCB {
		public CB7() { /* emtpty */
		}

		public CB7(long timeout) {
			super(timeout);
		}

		protected abstract void cb(CBResult result, T1 arg1, T2 arg2, T3 arg3, T4 arg4, T5 arg5,
				T6 arg6, T7 arg7);

		public void callCBResult(CBResult result) {
			cb(result, null, null, null, null, null, null, null);
		}

		public void call(CBResult result, T1 arg1, T2 arg2, T3 arg3, T4 arg4, T5 arg5, T6 arg6, T7 arg7) {
			if (!checkCancelled(result))
				cb(result, arg1, arg2, arg3, arg4, arg5, arg6, arg7);
		}
	}

	public abstract class CB0R<R> extends AbstractCB {
		public CB0R() { /* emtpty */
		}

		public CB0R(long timeout) {
			super(timeout);
		}

		protected abstract R cb(CBResult result);

		public void callCBResult(CBResult result) {
			cb(result);
		}

		public R call(CBResult result) {
			if (!checkCancelled(result))
				return cb(result);
			return null;
		}

		public void callOK() {
			if (!checkCancelled(CBResult.OK()))
				cb(CBResult.OK());
		}
	}

	public abstract class CBR<R, T1> extends AbstractCB {
		public CBR() { /* emtpty */
		}

		public CBR(long timeout) {
			super(timeout);
		}

		protected abstract R cb(CBResult result, T1 arg1);

		public void callCBResult(CBResult result) {
			cb(result, null);
		}

		public R call(CBResult result, T1 arg1) {
			if (!checkCancelled(result))
				return cb(result, arg1);
			return null;
		}
	}

	public abstract class CB1R<R, T1> extends AbstractCB {
		public CB1R() { /* emtpty */
		}

		public CB1R(long timeout) {
			super(timeout);
		}

		protected abstract R cb(CBResult result, T1 arg1);

		public void callCBResult(CBResult result) {
			cb(result, null);
		}

		public R call(CBResult result, T1 arg1) {
			if (!checkCancelled(result))
				return cb(result, arg1);
			return null;
		}
	}

	public abstract class CB2R<R, T1, T2> extends AbstractCB {
		public CB2R() { /* emtpty */
		}

		public CB2R(long timeout) {
			super(timeout);
		}

		protected abstract R cb(CBResult result, T1 arg1, T2 arg2);

		public void callCBResult(CBResult result) {
			cb(result, null, null);
		}

		public R call(CBResult result, T1 arg1, T2 arg2) {
			if (!checkCancelled(result))
				return cb(result, arg1, arg2);
			return null;
		}
	}

	public abstract class CB3R<R, T1, T2, T3> extends AbstractCB {
		public CB3R() { /* emtpty */
		}

		public CB3R(long timeout) {
			super(timeout);
		}

		protected abstract R cb(CBResult result, T1 arg1, T2 arg2, T3 arg3);

		public void callCBResult(CBResult result) {
			cb(result, null, null, null);
		}

		public R call(CBResult result, T1 arg1, T2 arg2, T3 arg3) {
			if (!checkCancelled(result))
				return cb(result, arg1, arg2, arg3);
			return null;
		}
	}

	public abstract class CB4R<R, T1, T2, T3, T4> extends AbstractCB {
		public CB4R() { /* emtpty */
		}

		public CB4R(long timeout) {
			super(timeout);
		}

		protected abstract R cb(CBResult result, T1 arg1, T2 arg2, T3 arg3, T4 arg4);

		public void callCBResult(CBResult result) {
			cb(result, null, null, null, null);
		}

		public R call(CBResult result, T1 arg1, T2 arg2, T3 arg3, T4 arg4) {
			if (!checkCancelled(result))
				return cb(result, arg1, arg2, arg3, arg4);
			return null;
		}
	}

}
