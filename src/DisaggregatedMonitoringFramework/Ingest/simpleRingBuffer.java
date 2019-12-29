package DisaggregatedMonitoringFramework.Ingest;


import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReferenceArray;

public class simpleRingBuffer<T> implements Iterable<T> {

  private AtomicReferenceArray<T> buffer;          // queue elements
  private AtomicInteger count = new AtomicInteger(0);// = 0;          // number of elements on queue
  private AtomicInteger indexOut= new AtomicInteger(0);// = 0;       // index of first element of queue
  private AtomicInteger indexIn= new AtomicInteger(0);// = 0;       // index of next available slot

  // cast needed since no generic array creation in Java
  public simpleRingBuffer(int capacity) {
    buffer = new AtomicReferenceArray<T>(capacity);
  }

  public boolean isEmpty() {
    return count.get() == 0;
  }

  public int size() {
    return count.get();
  }

  public void push(T item) {
    if (count.get() == buffer.length()) {
    	//return
        throw new RuntimeException("Ring buffer overflow");
    }
    buffer.set(indexIn.get(), item);
    
    int val = (indexIn.get()+ 1) % buffer.length(); 
    indexIn.set(val);
    //indexIn = (indexIn + 1) % buffer.length();     // wrap-around
    count.incrementAndGet();//++;
  }

  public T pop() {
    if (isEmpty()) {
        throw new RuntimeException("Ring buffer underflow");
    }
    T item = buffer.get(indexOut.get());
    buffer.set(indexOut.get(), null);                  // to help with garbage collection
    count.decrementAndGet();//--;
    
    int val = (indexOut.get()+1) % buffer.length();
    indexOut.set(val);
    //indexOut = (indexOut + 1) % buffer.length(); // wrap-around
    return item;
  }

  public Iterator<T> iterator() {
    return new RingBufferIterator();
  }

  // an iterator, doesn't implement remove() since it's optional
  private class RingBufferIterator implements Iterator<T> {

    private AtomicInteger i = new AtomicInteger(0);

    public boolean hasNext() {
        return i.get() < count.get();
    }

    public void remove() {
        throw new UnsupportedOperationException();
    }

    public T next() {
        if (!hasNext()) {
            throw new NoSuchElementException();
        }
        //return buffer.get(i++);
         T val = buffer.get(i.get());
        i.incrementAndGet();
        return val;
        
    }
  }

  /**
   * test overflow
   * @return
   */
  public boolean isOverflowing() {
	// TODO Auto-generated method stub
		return count.get() <=(0.8* buffer.length());
  }
}