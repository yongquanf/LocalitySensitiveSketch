/*
 * SBON
 * 
 * @author Last modified by $Author: prp $
 * @version $Revision: 1.2 $ on $Date: 2007/08/14 11:18:13 $
 * @since Jan 12, 2005
 */
package edu.harvard.syrah.sbon.async.comm.xmlrpc;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Vector;

import org.apache.xmlrpc.XmlRpcException;

import edu.harvard.syrah.prp.Log;
import edu.harvard.syrah.sbon.async.CallbacksIF.CB1;

/**
 * 
 * This is an XMLRPC invoker that knows about callbacks. It is adapted from the
 * Invoker class in Apache XMLRPC.
 * 
 */
class XMLRPCCallbackInvoker implements XMLRPCCallbackHandlerIF {
	private static final Log log = new Log(XMLRPCCallbackInvoker.class);

	private Object handler;
	@SuppressWarnings("unchecked")
	private Class handlerClass;

	XMLRPCCallbackInvoker(Object handler) {
		this.handler = handler;
		this.handlerClass = handler.getClass();
	}

	/*
	 * @see edu.harvard.syrah.sbon.comm.XMLRPCCallbackHandler#execute(java.lang.String,
	 *      java.util.Vector, edu.harvard.syrah.sbon.async.CallbacksIF.CB)
	 */
	@SuppressWarnings("unchecked")
	public void execute(String methodName, Vector params, final CB1<Object> cbObject) throws Exception {

		// Create array with classtype; create ObjectAry with values
		Class[] argClasses = null;
		Object[] argValues = null;
		if (params != null) {

			argClasses = new Class[params.size() + 1];
			argValues = new Object[params.size() + 1];

			for (int i = 0; i < params.size(); i++) {
				argValues[i] = params.elementAt(i);
				if (argValues[i] instanceof Integer) {
					argClasses[i] = Integer.TYPE;
				} else
					if (argValues[i] instanceof Double) {
						argClasses[i] = Double.TYPE;
					} else
						if (argValues[i] instanceof Boolean) {
							argClasses[i] = Boolean.TYPE;
						} else {
							argClasses[i] = argValues[i].getClass();
						}
			}
		}

		// Add callback parameter
		argClasses[argClasses.length - 1] = CB1.class;
		argValues[argValues.length - 1] = cbObject; 
			
		Method method = null;

		// The last element of the XML-RPC method name is the Java method name.
		int dot = methodName.lastIndexOf('.');
		if (dot > -1 && dot + 1 < methodName.length()) {
			methodName = methodName.substring(dot + 1);
		}

		try {
			method = handlerClass.getMethod(methodName, argClasses);
		}
		catch (NoSuchMethodException e) {
			throw e;
		} catch (SecurityException e) {
			throw e;
		}

		// Our policy is to make all public methods callable except the ones defined in java.lang.Object.
		if (method.getDeclaringClass() == Object.class) {
			throw new XmlRpcException(0, "Invoker can't call methods " + "defined in java.lang.Object");
		}

		try {
			log.debug("Invoking method=" + method + " of handler=" + handler + " with args=" + argValues);
			method.invoke(handler, argValues);
		} catch (IllegalAccessException e) {
			throw e;
		} catch (IllegalArgumentException e) {
			throw e;
		} catch (InvocationTargetException e) {
			log.warn("Got an exception=" + e.getTargetException());
			e.getTargetException().printStackTrace();
			// check whether the thrown exception is XmlRpcException
			Throwable t = e.getTargetException();
			if (t instanceof XmlRpcException) {
				throw (XmlRpcException) t;
			}
			// It is some other exception
			throw e;
		}
	}

	/*
	 * @see org.apache.xmlrpc.XmlRpcHandler#execute(java.lang.String,
	 *      java.util.Vector)
	 */
	@SuppressWarnings("unchecked")
	public Object execute(String method, Vector params) throws Exception {
		throw new UnsupportedOperationException();
	}

}
