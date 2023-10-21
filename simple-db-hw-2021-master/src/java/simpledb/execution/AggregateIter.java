package simpledb.execution;

import java.util.*;

import simpledb.common.DbException;
import simpledb.storage.Field;
import simpledb.storage.IntField;
import simpledb.storage.Tuple;
import simpledb.storage.TupleDesc;
import simpledb.common.Type;
import simpledb.transaction.TransactionAbortedException;

public class AggregateIter implements  OpIterator{
    private Iterator<Tuple> tupleIterator;//元组迭代器
    private Map<Field, List<Field> > group;//Field是分组字段，每个Field映射一个List，List是根据field分类后，待聚合的field组成的链表
    // 如何构造，见IntegerAggregator和StringAggregator
    private List<Tuple> resultSet;
    private Aggregator.Op op ;
    private TupleDesc tupleDesc ;
    private int gbField;
    private Type gbFieldType;

    public AggregateIter(Map<Field,List<Field>> group, int gbField , Type gbFieldType , Aggregator.Op op){
        this.group = group;
        this.gbField = gbField;
        this.gbFieldType =gbFieldType;
        this.op = op;
        if(gbField!=-1){//需要group
            Type[] type = new Type[2];
            type[0] = gbFieldType;
            type[1] = Type.INT_TYPE;
            this.tupleDesc=new TupleDesc(type);
        }
        else {
            Type[] type = new Type[1];
            type[0] = Type.INT_TYPE;
            this.tupleDesc = new TupleDesc(type);
        }
    }
    public void open() throws DbException,NoSuchElementException{
            this.resultSet = new ArrayList<>();
            if(op == Aggregator.Op.COUNT){
                //对于每一个不同的分组字段，声明新的tuple。根据IntegerAggregator的构造，没有分组字段的，group为<null，list>的形式
                for(Field field : group.keySet()){
                    Tuple tuple = new Tuple(tupleDesc);
                    if(field != null){
                        tuple.setField(0,field);
                        tuple.setField(1, new IntField(group.get(field).size()));//IntField is an Instance of Field that stores a single integer.
                        //每个field，对应的list长度就是count值
                    }
                    else
                        tuple.setField(0,new IntField(group.get(field).size()));
                    this.resultSet.add(tuple);
                }
            }
            else if (op == Aggregator.Op.MIN){
                for(Field field : group.keySet()){
                    int min = Integer.MIN_VALUE;
                    Tuple tuple = new Tuple(tupleDesc);
                    for(int i=0;i<this.group.get(field).size();i++){
                        IntField intField =(IntField) group.get(field).get(i);
                        if(intField.getValue()<min) min=intField.getValue();
                    }
                    if(field != null ){
                        tuple.setField(0,field);
                        tuple.setField(1,new IntField(min));
                    }
                    else
                        tuple.setField(0,new IntField(min));
                    this.resultSet.add(tuple);
                }
            }
            else if(op == Aggregator.Op.MAX){
                for(Field field : group.keySet()){
                    int max= Integer.MAX_VALUE;
                    Tuple tuple = new Tuple(tupleDesc);
                    for(int i=0;i<this.group.get(field).size();i++){
                        IntField intField = (IntField) group.get(field).get(i);
                        if(intField.getValue()>max) max = intField.getValue();
                    }
                    if(field != null){
                        tuple.setField(0,field);
                        tuple.setField(1,new IntField(max));
                    }
                    else
                        tuple.setField(0,new IntField(max));
                    this.resultSet.add(tuple);
                }
            }
            else if(op == Aggregator.Op.SUM){
                for(Field field : group.keySet()){
                    int sum =0;
                    Tuple tuple = new Tuple(tupleDesc);
                    for(int i=0 ; i<this.group.get(field).size();i++){
                        IntField intField = (IntField) group.get(field).get(i);
                        sum+=intField.getValue();
                    }
                    if(field != null) {
                        tuple.setField(0,field);
                        tuple.setField(1,new IntField(sum));
                    }
                    else
                        tuple.setField(0,new IntField(sum));
                    this.resultSet.add(tuple);
                }
            }
            else if(op == Aggregator.Op.AVG){
                for(Field field : group.keySet()){
                    int sum = 0 ;
                    int size = group.get(field).size();
                    Tuple tuple = new Tuple(tupleDesc);
                    for(int i=0;i < this.group.get(field).size();i++){
                        IntField intField = (IntField) group.get(field).get(i);
                        sum+= intField.getValue();
                    }
                    if( field != null ){
                        tuple.setField(0,field);
                        tuple.setField(1,new IntField(sum/size));
                    }
                    else
                        tuple.setField(0,new IntField(sum/size));
                    resultSet.add(tuple);
                }
            }
      this.tupleIterator = resultSet.iterator();
    }
    public boolean hasNext() throws DbException, TransactionAbortedException{
        if(tupleIterator == null) return false;
        return tupleIterator.hasNext();
    }
    public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
        return tupleIterator.next();
    }

    @Override
    public void rewind() throws DbException, TransactionAbortedException {
        if(resultSet!=null){
            tupleIterator = resultSet.iterator();
            //通过将`tupleIterator`重新赋值为`resultSet`的迭代器，我们实际上创建了一个新的迭代器对象，并将其指针设置为结果集的开头位置。
//           这样，当再次调用`next()`方法时，新的迭代器将从结果集的开头开始返回元素。
        }
    }

    @Override
    public TupleDesc getTupleDesc() {
        return this.tupleDesc;
    }

    @Override
    public void close() {
        this.tupleIterator = null;
    }

}
