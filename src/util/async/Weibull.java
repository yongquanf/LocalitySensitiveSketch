package util.async;

import java.util.Random;

import org.apache.commons.math3.distribution.WeibullDistribution;
import org.apache.commons.math3.random.ISAACRandom;

public class Weibull{
	ISAACRandom rnd=null;
	  private long seed;
	  //private Sim_random_obj source;
	  private double scale, shape;
	  private String name;

	  WeibullDistribution wd;
	  /**
	   * Constructor with which <code>Sim_system</code> is allowed to set the random number
	   * generator's seed
	   * @param name The name to be associated with this instance
	   * @param scale The scale of the distribution
	   * @param shape The shape of the distribution
	   */
	  public Weibull(String name, double scale, double shape) {
	    //source = new Sim_random_obj("Internal PRNG");
	    seed=System.currentTimeMillis();
	    
	    this.scale = scale;
	    this.shape = shape;
	    this.name = name;
	    rnd = new ISAACRandom(seed);
	    wd = new WeibullDistribution(rnd,scale,shape);
	  }

	  /**
	   * The constructor with which a specific seed is set for the random
	   * number generator
	   * @param name The name to be associated with this instance
	   * @param scale The scale of the distribution
	   * @param shape The shape of the distribution
	   * @param seed The initial seed for the generator, two instances with
	   *             the same seed will generate the same sequence of numbers
	   */
	  public Weibull(String name, double scale, double shape, long seed) {
	    //source = new Sim_random_obj("Internal PRNG", seed);
	    this.seed=seed;
	    
	    this.scale = scale;
	    this.shape = shape;
	    this.name = name;
	    
	    rnd = new ISAACRandom(seed);
	    wd = new WeibullDistribution(rnd,scale,shape);
	  }

	  /**
	   * Generate a new random number.
	   * @return The next random number in the sequence
	   */
/*	  public double sample() {
		 
		  double xx=rnd.nextDouble();
		  double v=0;
		  while(xx<=0||xx==1||v<=0){
			  xx=rnd.nextDouble();
			  v=scale*Math.pow(-Math.log(1-xx),(1.0/shape));
		  }*/

	  public double sample() {
		  return Math.abs(wd.sample());
		  
	  }
		  
//%function [WeibullRandomNumbers]=WeibullRNG(scale,shape,noOfRandomNumbers)
//%Written by David Vannucci 01 June 2003 devannucci@yahoo.com
//WeibullRandomNumbers = scale.*( -log(1-rand(noOfRandomNumbers,1))).^(1/shape)

		  
		  
//		  double xx = Math.log(rnd.nextDouble());
//		  //System.out.println("xx: "+xx+", shape: "+shape);
//		  double uu= 1.0/(shape+0.0);
//		  //System.out.println("uu: "+uu);
//		  double yy=Math.pow(xx,uu);
//		  System.out.println("yy: "+yy);
//		  double zz = scale * yy;
//		  System.out.println("zz: "+zz);
//	    return zz;
	  

	  // Used by other distributions that rely on the Weibull distribution
/*	  static double sample(Sim_random_obj source, double scale, double shape) {
	    return scale * Math.pow(Math.log(rnd.nextDouble()), 1/shape);
	  }
*/
	  /**
	   * Set the random number generator's seed.
	   * @param seed The new seed for the generator
	   */
	  public void set_seed(long seed) {
	     this.seed=seed;
	  }

	  /**
	   * Get the random number generator's seed.
	   * @return The generator's seed
	   */
	  public long get_seed() {
	    return seed;
	  }

	  /**
	   * Get the random number generator's name.
	   * @return The generator's name
	   */
	  public String get_name() {
	    return name;
	  }

	  public static void main(String[]args){
		  
		  double scale=0.16;
		  double shape=0.8;
		  long seed=100;
		  Weibull weibuller = new Weibull("Delay",scale,shape,seed);
		  weibuller.set_seed(seed);
		  for(int i=0;i<100;i++){
			  System.out.println("next: "+weibuller.sample());
		  }
	  }
	}
