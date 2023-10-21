package simpledb.execution;

import simpledb.common.Type;
import simpledb.storage.Field;
import simpledb.storage.IntField;
import simpledb.storage.Tuple;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Knows how to compute some aggregate over a set of IntFields.
 */
public class IntegerAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;
    private int gbfield;
    private Type gbfieldtype;
    private int afield;
    private Op what;
    private Map<Field, List<Field>> group;
    /**
     * Aggregate constructor
     * 
     * @param gbfield
     *            the 0-based index of the group-by field in the tuple, or
     *            NO_GROUPING if there is no grouping
     * @param gbfieldtype
     *            the type of the group by field (e.g., Type.INT_TYPE), or null
     *            if there is no grouping
     * @param afield
     *            the 0-based index of the aggregate field in the tuple
     * @param what
     *            the aggregation operator
     */

    public IntegerAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        // done
        this.gbfield = gbfield;
        this. gbfieldtype = gbfieldtype;
        this.afield = afield ;
        this.what = what;
        this.group = new HashMap<>();
    }

    /**
     * 将一个新的元组合并到聚合中，按照构造函数中的指示进行分组。
     * Merge a new tuple into the aggregate, grouping as indicated in the
     * constructor
     * 
     * @param tup
     *            the Tuple containing an aggregate field and a group-by field
     */
    //现在知道group by的字段索引-->fieldToGB -> 按照相同fieldGB，把fieldAggre聚集
    public void mergeTupleIntoGroup(Tuple tup) {
        // done
        Field fieldToAggre = tup.getField(afield);
        Field fieldToGB = null;
        if(gbfield != -1 )
            fieldToGB = tup.getField(gbfield);
        if(this.group.containsKey(fieldToGB))
            group.get(fieldToGB).add(fieldToAggre);
        else {
            List<Field> list = new ArrayList<>();
            list.add(fieldToAggre);
            group.put(fieldToGB,list);
        }
    }

    /**
     * Create a OpIterator over group aggregate results.
     * 
     * @return a OpIterator whose tuples are the pair (groupVal, aggregateVal)
     *         if using group, or a single (aggregateVal) if no grouping. The
     *         aggregateVal is determined by the type of aggregate specified in
     *         the constructor.
     */
    public OpIterator iterator() {
        // done
        return new AggregateIter(group,gbfield,gbfieldtype,what);
    }

}
