package simpledb.optimizer;

import simpledb.common.Catalog;
import simpledb.common.Database;
import simpledb.common.DbException;
import simpledb.common.Type;
import simpledb.execution.Predicate;
import simpledb.execution.SeqScan;
import simpledb.storage.*;
import simpledb.transaction.Transaction;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * TableStats represents statistics (e.g., histograms) about base tables in a
 * query. 
 * 
 * This class is not needed in implementing lab1 and lab2.
 */
public class TableStats {
    private static final ConcurrentMap<String, TableStats> statsMap = new ConcurrentHashMap<>();//Map<表名，Tablestats>

    static final int IOCOSTPERPAGE = 1000;//加载一个page需要的IO消耗
    /**
     * Number of bins for the histogram. Feel free to increase this value over
     * 100, though our tests assume that you have at least 100 bins in your
     * histograms.
     */
    static final int NUM_HIST_BINS = 100;//直方图bucket个数
    private int tableId;
    private int ioCostperpage;
    private Catalog catalog = Database.getCatalog();
    private TupleDesc tupleDesc;
    private  DbFileIterator dbFileIterator;
    private int pageNum;
    private int total = 0;
    private Map<Integer,IntHistogram> intHistogramMap;//Map<字段索引，直方图>
    private Map<Integer,StringHistogram> stringHistogramMap;
    public static TableStats getTableStats(String tablename) {
        return statsMap.get(tablename);
    }

    public static void setTableStats(String tablename, TableStats stats) {
        statsMap.put(tablename, stats);
    }
    
    public static void setStatsMap(Map<String,TableStats> s)
    {
        try {
            java.lang.reflect.Field statsMapF = TableStats.class.getDeclaredField("statsMap");
            statsMapF.setAccessible(true);
            statsMapF.set(null, s);
        } catch (NoSuchFieldException | IllegalAccessException | IllegalArgumentException | SecurityException e) {
            e.printStackTrace();
        }

    }

    public static Map<String, TableStats> getStatsMap() {
        return statsMap;
    }

    public static void computeStatistics() {
        Iterator<Integer> tableIt = Database.getCatalog().tableIdIterator();

        System.out.println("Computing table stats.");
        while (tableIt.hasNext()) {
            int tableid = tableIt.next();
            TableStats s = new TableStats(tableid, IOCOSTPERPAGE);
            setTableStats(Database.getCatalog().getTableName(tableid), s);
        }
        System.out.println("Done.");
    }


    /**
     * Create a new TableStats object, that keeps track of statistics on each
     * column of a table
     * 
     * @param tableid
     *            The table over which to compute statistics
     * @param ioCostPerPage
     *            The cost per page of IO. This doesn't differentiate between
     *            sequential-scan IO and disk seeks.
     */
    public TableStats(int tableid, int ioCostPerPage) {
        // For this function, you'll have to get the
        // DbFile for the table in question,
        // then scan through its tuples and calculate
        // the values that you need.
        // You should try to do this reasonably efficiently, but you don't
        // necessarily have to (for example) do everything
        // in a single scan of the table.
        // done
        this.tableId = tableid;
        this.ioCostperpage = ioCostPerPage;
        HeapFile heapFile = (HeapFile) catalog.getDatabaseFile(tableid);
        this.tupleDesc = heapFile.getTupleDesc();
        this.dbFileIterator = heapFile.iterator(new TransactionId());
        this.intHistogramMap = new ConcurrentHashMap<>();
        this.stringHistogramMap = new ConcurrentHashMap<>();
        try {
                initHistogram(tableId);
        }catch(TransactionAbortedException | DbException e){
            e.printStackTrace();
        }
    }


        private void initHistogram(int tableId) throws TransactionAbortedException,DbException{
        //第一次遍历，找最大最小值
            int size = tupleDesc.numFields();
            TransactionId tid = new TransactionId();
            SeqScan seqScan = new SeqScan(tid,tableId);
            seqScan.open();
            Map<Integer,Integer> minMap = new HashMap<>();//Map<index i ， i对应的字段的；最小值>
            Map<Integer,Integer> maxMap = new HashMap<>();
            while(seqScan.hasNext()){
                Tuple tuple = seqScan.next();
                total++;
                //遍历当前元组的每个字段
                for(int i=0;i<size;i++){
                    if(tupleDesc.getFieldType(i)==Type.INT_TYPE){
                        IntField field = (IntField) tuple.getField(i);
                        Integer minVal = minMap.getOrDefault(i,Integer.MAX_VALUE);
                        minMap.put(i,Math.min(minVal,field.getValue()));
                        Integer maxVal = maxMap.getOrDefault(i,Integer.MIN_VALUE);
                        maxMap.put(i,Math.max(maxVal, field.getValue()));
                    }
                    else {
                        StringHistogram histogram = stringHistogramMap.getOrDefault(i,new StringHistogram(NUM_HIST_BINS));
                        StringField field =(StringField) tuple.getField(i);
                        histogram.addValue(field.getValue());
                        stringHistogramMap.put(i,histogram);
                    }
                }
                //根据min，max，给每一字段初始化一个直方图
                for(int i=0;i<size;i++){
                    if(minMap.get(i) != null){
                        Integer min = minMap.get(i);
                        Integer max = maxMap.get(i);
                        intHistogramMap.put(i,new IntHistogram(NUM_HIST_BINS,min,max));
                    }
                }
                //第二次遍历，给直方图addvalue
                 seqScan.rewind();
                while(seqScan.hasNext()){
                    Tuple tuple1 = seqScan.next();
                    for(int i=0;i<size;i++){
                        if(tupleDesc.getFieldType(i).equals(Type.INT_TYPE)){
                            IntField field = (IntField) tuple1.getField(i);
                            IntHistogram intHistogram= intHistogramMap.get(i);
                            if(intHistogram == null)
                                throw new IllegalArgumentException();
                            intHistogram.addValue(field.getValue());
                            intHistogramMap.put(i,intHistogram);
                        }
                    }
                }
                seqScan.close();
            }
    }
    /**
     * Estimates the cost of sequentially scanning the file, given that the cost
     * to read a page is costPerPageIO. You can assume that there are no seeks
     * and that no pages are in the buffer pool.
     * 
     * Also, assume that your hard drive can only read entire pages at once, so
     * if the last page of the table only has one tuple on it, it's just as
     * expensive to read as a full page. (Most real hard drives can't
     * efficiently address regions smaller than a page at a time.)
     * 
     * @return The estimated cost of scanning the table.
     */
    public double estimateScanCost() {
        // done
        return this.pageNum*ioCostperpage*2;//扫了两遍
    }

    /**
     * This method returns the number of tuples in the relation, given that a
     * predicate with selectivity selectivityFactor is applied.
     * 
     * @param selectivityFactor
     *            The selectivity of any predicates over the table
     * @return The estimated cardinality of the scan with the specified
     *         selectivityFactor
     */
    public int estimateTableCardinality(double selectivityFactor) {
        // done
        return (int) (total*selectivityFactor);
    }

    /**
     * The average selectivity of the field under op.
     * @param field
     *        the index of the field
     * @param op
     *        the operator in the predicate
     * The semantic of the method is that, given the table, and then given a
     * tuple, of which we do not know the value of the field, return the
     * expected selectivity. You may estimate this value from the histograms.
     * */
    public double avgSelectivity(int field, Predicate.Op op) {
        // done
        if(tupleDesc.getFieldType(field).equals(Type.INT_TYPE))
            return intHistogramMap.get(field).avgSelectivity();
        else
            return stringHistogramMap.get(field).avgSelectivity();
    }

    /**
     * Estimate the selectivity of predicate <tt>field op constant</tt> on the
     * table.
     * 
     * @param field
     *            The field over which the predicate ranges
     * @param op
     *            The logical operation in the predicate
     * @param constant
     *            The value against which the field is compared
     * @return The estimated selectivity (fraction of tuples that satisfy) the
     *         predicate
     */
    public double estimateSelectivity(int field, Predicate.Op op, Field constant) {
        // done
        if(tupleDesc.getFieldType(field).equals(Type.INT_TYPE))
            return intHistogramMap.get(field).estimateSelectivity(op,((IntField)constant).getValue());
        else
            return stringHistogramMap.get(field).estimateSelectivity(op,((StringField)constant).getValue());
    }

    /**
     * return the total number of tuples in this table
     * */
    public int totalTuples() {
        // done
        return total;
    }

}
