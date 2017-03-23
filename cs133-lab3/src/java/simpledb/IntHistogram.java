package simpledb;

import java.util.ArrayList;
import java.util.HashMap;

/** A class to represent a fixed-width histogram over a single integer-based field.
 */
public class IntHistogram {
	private int[] buckets;
	private int bwidth;
	private int min;
	private int max;
	private int ntups;
    /**
     * Create a new IntHistogram.
     * 
     * This IntHistogram should maintain a histogram of integer values that it receives.
     * It should split the histogram into "buckets" buckets.
     *
     * Note: if the number of buckets exceeds the number of distinct integers between min and max, 
     * some buckets may remain empty (don't create buckets with non-integer widths).
     * 
     * The values that are being histogrammed will be provided one-at-a-time through the "addValue()" function.
     * 
     * Your implementation should use space and have execution time that are both
     * constant with respect to the number of values being histogrammed.  For example, you shouldn't 
     * simply store every value that you see in a sorted list.
     * 
     * @param buckets The number of buckets to split the input value into.
     * @param min The minimum integer value that will ever be passed to this class for histogramming
     * @param max The maximum integer value that will ever be passed to this class for histogramming
     */
    public IntHistogram(int buckets, int min, int max) {
    	this.buckets = new int[buckets];
    	this.ntups = 0;
    	this.min = min;
    	this.max = max;
    	//width of each bucket
    	this.bwidth = (int) Math.ceil(((double)(max - min))/buckets);
    }
    //helper method to get index of bucket to put v into
    public int getBucket(int v){
    	int bucketToAdd;
    	if(v <= this.min){
    		bucketToAdd = 0;
    	}
    	else if((v- this.min) % this.bwidth == 0 && this.bwidth !=1){
    		bucketToAdd = ((v - this.min) / this.bwidth) - 1;
    	}
    	else{
    		bucketToAdd = (v - this.min) / this.bwidth;
    	}
    	return bucketToAdd;
    }
    /**
     * Add a value to the set of values that you are keeping a histogram of.
     * @param v Value to add to the histogram
     */
    public void addValue(int v) {
    	this.ntups++;
    	int bucketToAdd = getBucket(v);
    	buckets[bucketToAdd] = buckets[bucketToAdd] + 1;
    }
    /**
     * Estimate the selectivity of a particular predicate and operand on this table.
     * 
     * For example, if "op" is "GREATER_THAN" and "v" is 5, 
     * return your estimate of the fraction of elements that are greater than 5.
     * 
     * @param op Operator
     * @param v Value
     * @return Predicted selectivity of this particular operator and value
     */
    public double estimateSelectivity(Predicate.Op op, int v) {
    	if(op.equals(Predicate.Op.EQUALS)){
    		int bucket = getBucket(v);
    		int height = buckets[bucket];
    		return (double)(height/this.bwidth)/ this.ntups;
    	}
    	else if(op.equals(Predicate.Op.NOT_EQUALS)){
    		int bucket = getBucket(v);
    		int height = buckets[bucket];
    		return 1.0 - (height/this.bwidth)/ this.ntups;
    	}
    	else if(op.equals(Predicate.Op.GREATER_THAN)){
    		/*To estimate the selectivity of a range expression f>const, compute the 
    		 * bucket b that const is in, with width w_b and height h_b. Then, b contains
    		 *  a fraction b_f = h_b / ntups of the total tuples. Assuming tuples are uniformly
    		 *   distributed throughout b, the fraction b_part of b that is > const is (b_right - const) / w_b,
    		 *    where b_right is the right endpoint of b's bucket. Thus, bucket b contributes (b_f x b_part) 
    		 *    selectivity to the predicate. In addition, buckets b+1...NumB-1 contribute all of their selectivity 
    		 *    (which can be computed using a formula similar to b_f above). Summing the selectivity contributions 
    		 *    of all the buckets will yield the overall selectivity of the expression. Figure 2 illustrates this process.
    		 */
    		if(v<this.min){
    			return 1.0;
    		}
    		if(v>this.max){
    			return 0.0;
    		}
    		int bucket = getBucket(v);
    		double h_b = (double) buckets[bucket];
    		double b_f = h_b / (double) this.ntups;
    		double b_right = (double)(bucket+1)*this.bwidth;
    		double b_part = (b_right - v) / (double) this.bwidth;
    		double selectivity = b_f * b_part;
    		for(int temp = bucket+1; temp<buckets.length; temp++){
    			selectivity += (buckets[temp]/(double)this.ntups);
    		}
    		return selectivity;
    	}
    	else if(op.equals(Predicate.Op.GREATER_THAN_OR_EQ)){
    		if(v<this.min){
    			return 1.0;
    		}
    		if(v>this.max){
    			return 0.0;
    		}
    		int bucket = getBucket(v);
    		double h_b = (double) buckets[bucket];
    		double b_f = h_b / (double) this.ntups;
    		double b_right = (double)(bucket+1)*this.bwidth;
    		double b_part = ((b_right - v) + 1.0) / (double) this.bwidth;
    		double selectivity = b_f * b_part;
    		for(int temp = bucket+1; temp<buckets.length; temp++){
    			selectivity += (buckets[temp]/(double)this.ntups);
    		}
    		return selectivity;
    	}
    	else if(op.equals(Predicate.Op.LESS_THAN)){
    		if(v<this.min){
    			return 0.0;
    		}
    		if(v>this.max){
    			return 1.0;
    		}
    		int bucket = getBucket(v);
    		double h_b = (double) buckets[bucket];
    		double b_f = h_b / (double) this.ntups;
    		double b_left = (double)(bucket)*this.bwidth;
    		double b_part = (v - b_left) / (double) this.bwidth;
    		double selectivity = b_f * b_part;
    		for(int temp = bucket; temp>-1; temp--){
    			selectivity += (buckets[temp]/(double)this.ntups);
    		}
    		return selectivity;
    	}
    	else if(op.equals(Predicate.Op.LESS_THAN_OR_EQ)){
    		if(v<this.min){
    			return 0.0;
    		}
    		if(v>this.max){
    			return 1.0;
    		}
    		int bucket = getBucket(v);
    		double h_b = (double) buckets[bucket];
    		double b_f = h_b / (double) this.ntups;
    		double b_left = (double)(bucket)*this.bwidth;
    		double b_part = (v - b_left) + 1.0 / (double) this.bwidth;
    		double selectivity = b_f * b_part;
    		for(int temp = bucket; temp>-1; temp--){
    			selectivity += (buckets[temp]/(double)this.ntups);
    		}
    		return selectivity;
    	}
    	else{
    		return -1.0;
    	}
    }
    
    /**
     * @return
     *     the average selectivity of this histogram.
     *     
     *     This is not an indispensable method to implement the basic
     *     join optimization. It could be used to
     *     implement a more efficient optimization
     *
     * Not necessary for lab 3
     * */
    public double avgSelectivity()
    {
        return 0.5;
    }
    
    /**
     * (Optional) A String representation of the contents of this histogram
     * @return A string describing this histogram, for debugging purposes
     */
    public String toString() {
        return this.buckets.toString();
    }
}
