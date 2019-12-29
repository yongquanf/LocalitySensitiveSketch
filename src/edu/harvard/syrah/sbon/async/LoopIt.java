/*
 * @author Last modified by $Author: prp $
 * @version $Revision: 1.1 $ on $Date: 2007/06/04 17:55:41 $
 * @since Aug 11, 2005
 */
package edu.harvard.syrah.sbon.async;

import java.util.ConcurrentModificationException;
import java.util.Iterator;

import edu.harvard.syrah.prp.Log;
import edu.harvard.syrah.sbon.async.CallbacksIF.CB0;
import edu.harvard.syrah.sbon.async.CallbacksIF.CB0R;
import edu.harvard.syrah.sbon.async.CallbacksIF.CB1;
import edu.harvard.syrah.sbon.async.CallbacksIF.CB2;
import edu.harvard.syrah.sbon.async.EL.Priority;


public class LoopIt<T> {
  protected static final Log log = new Log(LoopIt.class);
  
  // Maximum time of single loop iteration in nanoseconds
  private static final long MAX_SINGLE_ITERATION_TIME = 1 * 1000000;
  
  private String name;
  protected CB2<T, CB0R<Boolean>> cbIteratorRecursion;
  private Priority priority;
  protected Iterator<T> it;
  private boolean remove; 
  private boolean smartScheduling;
  
  protected Barrier loopBarrier;
  
  public LoopIt(Iterable<T> it, final CB1<T> cbRecursion) {
  	this(null, it, false, false, new CB2<T, CB0R<Boolean>>() {
			protected void cb(CBResult result, T item, CB0R<Boolean> cbMore) {
				cbRecursion.call(result, item);
				cbMore.callOK();
			}  		
  	});
  }
  
  public LoopIt(Iterable<T> it, final CB2<T, CB0> cbRecursion) {
  	this(null, it, false, false, new CB2<T, CB0R<Boolean>>() {
			protected void cb(CBResult result, T item, final CB0R<Boolean> cbMore) {
				cbRecursion.call(result, item, new CB0() {
					protected void cb(CBResult result) {
						cbMore.callOK();
					}					
				});
			}  		
  	});
  }
  
  public LoopIt(String name, Iterable<T> it, CB2<T, CB0R<Boolean>> cbIteratorRecursion) {		
    this(name, it, true, false, cbIteratorRecursion, Priority.NORMAL);
  }
  
  public LoopIt(String name, Iterable<T> it, boolean remove, CB2<T, CB0R<Boolean>> cbIteratorRecursion) {   
    this(name, it, remove, false, cbIteratorRecursion, Priority.NORMAL);
  }
  
  public LoopIt(String name, Iterable<T> it, boolean remove, boolean smartScheduling, 
    CB2<T, CB0R<Boolean>> cbIteratorRecursion) {
    this(name, it, remove, smartScheduling, cbIteratorRecursion, Priority.NORMAL);    
  }
  
  public LoopIt(String name, Iterable<T> it, boolean remove, boolean smartScheduling, 
    CB2<T, CB0R<Boolean>> cbIteratorRecursion, Priority priority) {
    
    this.name = name;
    this.it = it.iterator();
    this.remove = remove;
    this.smartScheduling = smartScheduling;
    this.cbIteratorRecursion = cbIteratorRecursion;
    this.priority = priority;
    loopBarrier = new Barrier(1);
  }
  
  public void execute(CB0 cbDone) {
    EL.get().registerTimerCB(execute(), cbDone);
  }
  
  public Barrier execute() {
    EL.get().registerTimerCB(new CB0(name) {
      protected void cb(CBResult resultOK) { 
      	recurseIterator(); 
      }
    });
    return loopBarrier;
  }
    
  long startTime;
  long runningTime;
  
  public void recurseIterator() {
    if (startTime == 0)
      startTime = System.nanoTime();
    
    if (it.hasNext()) {
      try {
        final T item = it.next();
        
        runningTime = System.nanoTime() - startTime;
        if (smartScheduling && runningTime < MAX_SINGLE_ITERATION_TIME) {
          
          cbIteratorRecursion.call(CBResult.OK(), item, new CB0R<Boolean>() {
            protected Boolean cb(CBResult result) {
              // Try to remove the object after an iteration to reclaim memory
              if (remove) {
                try {
                  it.remove();
                } catch (UnsupportedOperationException e) { /* ignore */ }
              }
              
              boolean hasNext = it.hasNext();
              recurseIterator();
              return hasNext;
            }
          });
          
        } else {
          
          EL.get().registerTimerCB(new CB0(name) {
            protected void cb(CBResult result) {
              runningTime = 0;
              
              cbIteratorRecursion.call(CBResult.OK(), item, new CB0R<Boolean>() {
                protected Boolean cb(CBResult result) {
                  // Try to remove the object after an iteration to reclaim memory
                  if (remove) {
                    try {
                      it.remove();
                    } catch (UnsupportedOperationException e) { /* ignore */ }
                  }
                  
                  boolean hasNext = it.hasNext();
                  recurseIterator();
                  return hasNext;
                }
              });
            }       
          }, priority);
        }
        
      } catch (ConcurrentModificationException e) {
        log.error("Concurrent loop modification: name=" + name + " it=" + it + " e=" + e);
      }      
      
    } else {
      loopBarrier.join();
      startTime = 0;
    }
  }
}
