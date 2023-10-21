package simpledb.optimizer;

import simpledb.execution.Predicate;

import java.util.Arrays;
import java.util.Optional;

/** A class to represent a fixed-width histogram over a single integer-based field.
 */
public class IntHistogram {
    private MyGram[] grams;
    private int buckets;//每个柱状图里桶的数量
    private int max;
    private int min;
    private double avg;
    private int nTuples;//元组总数
    public class MyGram{
        private double left;
        private double right;
        private int count;
        private double width;
        public MyGram(double left,double right){
            this.left = left;
            this.right = right;
            this.width = right-left;
            this.count = 0;
        }
        public boolean isInRange(int tmp){
            return (tmp>=left && tmp<=right);
        }
        public String toString(){
            return "MyGram{"+"left="+left+" ,Right="+right+" ,width="+width+" ,count="+count+"}";
        }
    }
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
        // done
        this.buckets = buckets;
        this.max = max;
        this.min = min;
        this.grams = new MyGram[buckets];
        this.avg = ((max-min)*1.0)/(buckets*1.0);
        this.nTuples= 0;
        if(avg %1 !=0)
            avg = (int) (avg+1)/1;
        int l = min;
        for(int i=0;i<buckets;i++){
            grams[i] =new MyGram(l,l+avg);
            l+=avg;
        }
    }

    /**
     * Add a value to the set of values that you are keeping a histogram of.
     * @param v Value to add to the histogram
     */
    public int BinarySearch(int v){
        int lo= 0;
        int hi = buckets-1;
       while(lo<=hi){
           int mid=lo+(hi-lo)/2;
           if(grams[mid].isInRange(v))
               return mid;
           else if(grams[mid].left>v)
               hi =mid-1;
           else
               lo = mid+1;
       }
       return -1;
    }
    public void addValue(int v) {
        // done
        int location = BinarySearch(v);
        if(location != -1){
            grams[location].count++;
            nTuples++;
        }
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
        //done
        int location = BinarySearch(v);
        MyGram cur;
        if(location!=-1)
            cur = grams[location];
        else cur = null;
        if (op == Predicate.Op.EQUALS){
            if(cur == null)
                return 0.0;
            else
                return (cur.count/cur.width)/(nTuples*1.0);
        }
        else if(op == Predicate.Op.GREATER_THAN){
            if(v<min)
                return 1.0;
            else if(v>=max)
                return  0.0;
            else if(cur != null)
            {
                double res = ((cur.count*1.0)*(cur.right-v)/cur.width)/nTuples;
                for(int i=location+1;i<buckets;i++)
                    res +=(grams[i].count*1.0)/(nTuples*1.0);
                return res;
            }
        }
        else if(op == Predicate.Op.LESS_THAN){
            if(v<=min)
                return 0.0;
            else if(v>max)
                return 1.0;
            else if(cur != null){
                double res = ((cur.count*1.0)*(v-cur.left)/cur.width)/nTuples;
                for(int i=0;i<location;i++)
                    res +=(grams[i].count*1.0)/(nTuples*1.0);
                return res;
            }
        }
        else if(op == Predicate.Op.NOT_EQUALS){
            if(cur == null)
                return 1.0;
            else
                return 1.0-(cur.count/cur.width)/(nTuples*1.0);
        }
        else if(op == Predicate.Op.GREATER_THAN_OR_EQ){
            if(v<=min)
                return 1.0;
            else if(v>max)
                return 0.0;
            else if(cur != null){
                double res =( (cur.count*1.0)*(cur.right-v+1)/cur.width)/nTuples;
                for(int i=location+1;i<buckets;i++){
                    res+=(grams[i].count*1.0)/nTuples;
                }
                return res;
            }
            else if(op == Predicate.Op.LESS_THAN_OR_EQ){
                if(v>=max)
                    return 1.0;
                else if(v<min)
                    return 0.0;
                else if(cur != null){
                    double res = ((cur.count*1.0)*(v- cur.left+1)/ cur.width)/nTuples;
                    for(int i=0;i<location;i++)
                        res+=(grams[i].count*1.0)/nTuples;
                }
            }

        }
        return 0.0;
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
        // done
        return avg;
    }
    
    /**
     * @return A string describing this histogram, for debugging purposes
     */
    public String toString() {
        // done
        return "IntHistogram{" +
                "buckets=" + buckets +
                ", min=" + min +
                ", max=" + max +
                ", avg=" + avg +
                ", myGrams=" + Arrays.toString(grams) +
                '}';
    }
}
