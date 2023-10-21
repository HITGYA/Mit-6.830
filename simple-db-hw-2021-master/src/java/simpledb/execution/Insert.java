package simpledb.execution;

import simpledb.common.Database;
import simpledb.common.DbException;
import simpledb.storage.*;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;
import simpledb.common.Type;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;

/**
 * Inserts tuples read from the child operator into the tableId specified in the
 * constructor
 */
public class Insert extends Operator {

    private static final long serialVersionUID = 1L;
    private TransactionId transactionId;
    private OpIterator child;
    private int tableId;
    private ArrayList<Tuple> tupleList = new ArrayList<>();
    private Iterator<Tuple> iterator;
    /**
     * Constructor.
     *
     * @param t
     *            The transaction running the insert.
     * @param child
     *            The child operator from which to read tuples to be inserted.
     * @param tableId
     *            The table in which to insert tuples.
     * @throws DbException
     *             if TupleDesc of child differs from table into which we are to
     *             insert.
     */
    public Insert(TransactionId t, OpIterator child, int tableId)
            throws DbException {
        //done
        transactionId = t;
        this.child = child;
        this.tableId = tableId;
        if(! child.getTupleDesc().equals(Database.getCatalog().getTupleDesc(tableId)))
            throw  new DbException("insert tupledesc not equal");
    }

    public TupleDesc getTupleDesc() {
        // done
        return new TupleDesc(new Type[]{Type.INT_TYPE});
    }

    public void open() throws DbException, TransactionAbortedException {
        // done
        child.open();
        int count=0;
        while(child.hasNext()){
            Tuple next = child.next();
            count++;
            try{
                Database.getBufferPool().insertTuple(transactionId,tableId,next);//负责实际的插入操作
            }catch(IOException e){
                e.printStackTrace();
            }
        }
        Tuple tuple = new Tuple(getTupleDesc());
        tuple.setField(0, new IntField(count));
        tupleList.add(tuple);//tupleList维护一个字段的元组链表，a one field tuple containing the number of inserted records
        iterator = tupleList.iterator();
        super.open();
    }

    public void close() {
        // done
        child.close();
        iterator=null;
        super.close();
    }

    public void rewind() throws DbException, TransactionAbortedException {
        // done
        child.rewind();
        iterator = tupleList.iterator();
    }
    /**
     * Inserts tuples read from child into the tableId specified by the
     * constructor. It returns a one field tuple containing the number of
     * inserted records. Inserts should be passed through BufferPool. An
     * instances of BufferPool is available via Database.getBufferPool(). Note
     * that insert DOES NOT need check to see if a particular tuple is a
     * duplicate before inserting it.
     *
     * @return A 1-field tuple containing the number of inserted records, or
     *         null if called more than once.
     * @see Database#getBufferPool
     * @see BufferPool#insertTuple
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        // done
        if(iterator != null && iterator.hasNext()){
            return iterator.next();
        }
        return null;
    }


    @Override
    public OpIterator[] getChildren() {
        // done
        return new OpIterator[]{child};
    }

    @Override
    public void setChildren(OpIterator[] children) {
        // done
        this.child = children[0];
    }
}
