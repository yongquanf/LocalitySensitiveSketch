package util.async;

import java.io.Serializable;


public class triple<T> implements Serializable {

	
	
	  public T first;
	  public T second;
	  public T third;  
	  
	/**
	 * 
	 */
	private static final long serialVersionUID = -2321423265141283937L;

	
	public triple(T _first, T _second, T _third){
		this.first = _first;
		this.second = _second;
		this.third = _third;
	}
	

	public boolean equals(Object obj) {
		
		if (!(obj instanceof triple))
		   return false;

		// Consider the first element

		 triple cmpPair = ( triple) obj;

		 //abc, acb
		if (cmpPair.first.equals(first)){
		if(cmpPair.second.equals(this.second)){
			if(cmpPair.third.equals(this.third)){
				return true;
			}else{
				return false;
			}
		}else if(cmpPair.third.equals(this.second)){
			if(cmpPair.second.equals(this.third)){
				return true;
			}else{
				return false;
			}
		}
		} else//bca bac
		if(cmpPair.second.equals(this.first)){
			
			if(cmpPair.third.equals(this.second)){
				if(cmpPair.first.equals(this.third)){
					return true;
				}else{
					return false;
				}
			}else if(cmpPair.first.equals(this.second)){
				if(cmpPair.third.equals(this.third)){
					return true;
				}else{
					return false;
				}
			}
			
		}else//cab cba
			if(cmpPair.third.equals(this.first)){
			
			if(cmpPair.first.equals(this.second)){
				if(cmpPair.second.equals(this.third)){
					return true;
				}else{
					return false;
				}
			}else if(cmpPair.second.equals(this.second)){
				if(cmpPair.first.equals(this.third)){
					return true;
				}else{
					return false;
				}
					
			}
		}
		
		return false;
		
	}
	
	
	

@Override
	public int hashCode() {
		// TODO Auto-generated method stub
	
		return super.hashCode();
	}


public static void main(String[] args){
	
	
	triple<Double> t=new triple<Double>(-1., -2., -3.);
	triple<Double> t2=new triple<Double>(-3., -2., -3.);
	
	System.out.println(t.equals(t2));
}
}
