package simpledb.optimizer;

import simpledb.execution.Predicate;

/** A class to represent a fixed-width histogram over a single integer-based field.
 */
public class IntHistogram {
    private int min;
    private int max;
    private int buckets;
    public int ntups;
    private int width;
    private int[] histogram;

    /**
     * Create a new IntHistogram.
     * 
     * This IntHistogram should maintain a histogram of integer values that it receives.
     * It should split the histogram into "buckets" buckets.
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
    	// some code goes here
        this.min=min;
        this.max=max;
        this.buckets=buckets;
        double range=(double)(max-min+1)/buckets;
        width=(int)Math.ceil(range);
        ntups=0;//行数量
        histogram=new int[buckets];
    }

    private int value2Index(int v){
        if(v==max)
            return buckets-1;
        else
            return (v-min)/width;
    }
    /**
     * Add a value to the set of values that you are keeping a histogram of.
     * @param v Value to add to the histogram
     */
    public void addValue(int v) {
    	// some code goes here
        int index=value2Index(v);
        histogram[index]++;
        ntups++;
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

    	// some code goes here
        int bucketIndex=value2Index(v);
        int height;
        int left=bucketIndex*width+min;
        int right=bucketIndex*width+min+width-1;
        switch (op){
            case EQUALS:
                if(v<min||v>max)
                    return 0.0;
                else {
                    height=histogram[bucketIndex];
                    return (height*1.0/width)/ntups;
                }
            case GREATER_THAN:
                if(v<min)
                    return 1.0;
                if(v>max)
                    return 0.0;
                height=histogram[bucketIndex];
                double p1=((right-v)/width*1.0)*(height*1.0/ntups);
                int allInRight=0;
                for(int i=bucketIndex+1;i<buckets;i++){
                    allInRight+=histogram[i];
                }
                double p2=allInRight*1.0/ntups;
                return p1+p2;
            case LESS_THAN:
                if(v<min)
                    return 0.0;
                if(v>max)
                    return 1.0;
                height=histogram[bucketIndex];
                double pp1=((v-left)/width*1.0)*(height*1.0/ntups);
                int allInLeft=0;
                for(int i=bucketIndex-1;i>=0;i--){
                    allInLeft+=histogram[i];
                }
                double pp2=allInLeft*1.0/ntups;
                return pp1+pp2;
            case GREATER_THAN_OR_EQ:
                return estimateSelectivity(Predicate.Op.GREATER_THAN,v)+estimateSelectivity(Predicate.Op.EQUALS,v);
            case LESS_THAN_OR_EQ:
                return estimateSelectivity(Predicate.Op.LESS_THAN,v)+estimateSelectivity(Predicate.Op.EQUALS,v);
            case LIKE:
                return avgSelectivity();
            case NOT_EQUALS:
                return 1-estimateSelectivity(Predicate.Op.EQUALS,v);
            default:
                throw new RuntimeException("Should not reach hear");
        }
    }
    
    /**
     * @return
     *     the average selectivity of this histogram.
     *     
     *     This is not an indispensable method to implement the basic
     *     join optimization. It may be needed if you want to
     *     implement a more efficient optimization
     * */
    public double avgSelectivity()
    {
        // some code goes here
        int sum=0;
        for(int b:histogram)
            sum+=b;
        return 1.0*sum/ntups;
    }
    
    /**
     * @return A string describing this histogram, for debugging purposes
     */
    public String toString() {
        // some code goes here
        return null;
    }
}
