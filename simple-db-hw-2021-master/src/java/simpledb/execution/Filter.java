package simpledb.execution;

import simpledb.transaction.TransactionAbortedException;
import simpledb.common.DbException;
import simpledb.storage.Tuple;
import simpledb.storage.TupleDesc;

import java.util.*;

/**
 * Filter is an operator that implements a relational select.
 */
public class Filter extends Operator {
//子操作符child返回元组，通过predicate过滤这些元组
    private static final long serialVersionUID = 1L;
    private final Predicate predicate;
    private OpIterator child;
    private TupleDesc tupleDesc ;
    /**
     * Constructor accepts a predicate to apply and a child operator to read
     * tuples to filter from.
     * 
     * @param p
     *            The predicate to filter tuples with
     * @param child
     *            The child operator
     */
    public Filter(Predicate p, OpIterator child) {
        // done
        this.predicate = p;
        this.child = child;
    }

    public Predicate getPredicate() {
        // done
        return predicate;
    }

    public TupleDesc getTupleDesc() {
        // done
        return child.getTupleDesc();
    }

    public void open() throws DbException, NoSuchElementException,
            TransactionAbortedException {
        // done
        child.open();

        super.open();
    }

    public void close() {
        // done
        child.close();
        super.close();
    }

    public void rewind() throws DbException, TransactionAbortedException {
        // done
        this.child.rewind();
    }

    /**
     * AbstractDbIterator.readNext implementation. Iterates over tuples from the
     * child operator, applying the predicate to them and returning those that
     * pass the predicate (i.e. for which the Predicate.filter() returns true.)
     * 
     * @return The next tuple that passes the filter, or null if there are no
     *         more tuples
     * @see Predicate#filter
     */
    protected Tuple fetchNext() throws NoSuchElementException,
            TransactionAbortedException, DbException {
        // done
        while(this.child.hasNext()){
            Tuple tuple = child.next();
            if(predicate.filter(tuple))
                return tuple;
        }
        return null;
    }

    @Override
    public OpIterator[] getChildren() {
        // done
        return new OpIterator[]{this.child};//使用了初始化数组的一种方式，数组仅一个元素
    }

    @Override
    public void setChildren(OpIterator[] children) {
        // done
        this.child = children[0];
    }

}
