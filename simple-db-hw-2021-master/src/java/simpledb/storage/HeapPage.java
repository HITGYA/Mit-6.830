package simpledb.storage;

import simpledb.common.Database;
import simpledb.common.DbException;
import simpledb.common.Debug;
import simpledb.common.Catalog;
import simpledb.transaction.TransactionId;

import java.util.*;
import java.io.*;
//每个页包含一个header；header由一个bitmap组成，每个bit代表对应的槽
//如果bit为1代表槽中元组可用，bit为0代表槽中元组不可用(被删除了、或者未初始化)
/**
 * Each instance of HeapPage stores data for one page of HeapFiles and 
 * implements the Page interface that is used by BufferPool.
 *
 * @see HeapFile
 * @see BufferPool
 *
 */
public class HeapPage implements Page {

    final HeapPageId pid;
    final TupleDesc td;
    final byte[] header;
    final Tuple[] tuples;
    final int numSlots;
    private boolean dirty;
    private TransactionId tid;
    byte[] oldData;
    private final Byte oldDataLock= (byte) 0;//oldDataLock 是一个私有的 final 字段，类型为 Byte，并将其初始化为 (byte) 0。

    /**
     * Create a HeapPage from a set of bytes of data read from disk.
     * The format of a HeapPage is a set of header bytes indicating
     * the slots of the page that are in use, some number of tuple slots.
     *  Specifically, the number of tuples is equal to: <p>
     *          floor((BufferPool.getPageSize()*8) / (tuple size * 8 + 1))
     * <p> where tuple size is the size of tuples in this
     * database table, which can be determined via {@link Catalog#getTupleDesc}.
     * The number of 8-bit header words is equal to:
     * <p>
     *      ceiling(no. tuple slots / 8)
     * <p>
     * @see Database#getCatalog
     * @see Catalog#getTupleDesc
     * @see BufferPool#getPageSize()
     */
    public HeapPage(HeapPageId id, byte[] data) throws IOException {
        this.pid = id;
        this.td = Database.getCatalog().getTupleDesc(id.getTableId());
        this.numSlots = getNumTuples();
        DataInputStream dis = new DataInputStream(new ByteArrayInputStream(data));

        // allocate and read the header slots of this page
        header = new byte[getHeaderSize()];
        for (int i=0; i<header.length; i++)
            header[i] = dis.readByte();
        
        tuples = new Tuple[numSlots];
        try{
            // allocate and read the actual records of this page
            for (int i=0; i<tuples.length; i++)
                tuples[i] = readNextTuple(dis,i);
        }catch(NoSuchElementException e){
            e.printStackTrace();
        }
        dis.close();

        setBeforeImage();
    }

    /** Retrieve the number of tuples on this page.
        @return the number of tuples on this page
    */
    private int getNumTuples() {
        //done
        int pageSize = BufferPool.getPageSize();
        int sizeOfTuple = td.getSize();
        return ((int)Math.floor(pageSize * 8 * 1.0 / sizeOfTuple * 8 +1));
    }

    /**
     * Computes the number of bytes in the header of a page in a HeapFile with each tuple occupying tupleSize bytes
     * @return the number of bytes in the header of a page in a HeapFile with each tuple occupying tupleSize bytes
     */
    private int getHeaderSize() {
        //done
        int headerSize = (int) Math.ceil(getNumTuples() / 8.0);
        return headerSize;
                 
    }
    
    /** Return a view of this page before it was modified
        -- used by recovery */
    public HeapPage getBeforeImage(){
        try {
            byte[] oldDataRef = null;
            synchronized(oldDataLock)
            {
                oldDataRef = oldData;
            }
//            // 同步方法
//            public synchronized void increment() {
//                count++;
//            }
//           介绍两种同步方案，同一时间只有一个线程能够执行同步方法或进入同步代码块
//            一旦一个线程获取到了一个锁对象，它将持有该锁对象并执行相应的同步代码。在同步代码块或同步方法执行期间，其他线程将被阻塞，无法获取同一个锁对象。
//            锁的状态是由线程自动获取和释放锁来控制的，无需显式编写特定的代码来控制锁的状态。
//            // 同步代码块
//            public void decrement() {
//                synchronized (lock) {
//                    count--;
//                }
//            }
            return new HeapPage(pid,oldDataRef);
        } catch (IOException e) {
            e.printStackTrace();
            //should never happen -- we parsed it OK before!
            System.exit(1);
        }
        return null;
    }
    //为什么声明oldDataRef：为了避免
    // 1.并发修改：如果多个线程同时访问 oldData，并且其中某个线程在构造 HeapPage 对象之前修改了 oldData 的内容，
    //   那么其他线程可能会在构造函数中使用修改后的数据，从而导致结果不一致或出现错误
    // 2.不一致状态：如果在构造函数执行期间，oldData 发生了修改，那么构造函数可能会使用部分修改后的数据和部分未修改的数据来创建 HeapPage 对象，导致对象状态不一致。
    
    public void setBeforeImage() {
        synchronized(oldDataLock)
        {
        oldData = getPageData().clone();
        }
    }

    /**
     * @return the PageId associated with this page.
     */
    public HeapPageId getId() {
    // done
         return pid;
    }

    /**
     * Suck up tuples from the source file.
     */
    private Tuple readNextTuple(DataInputStream dis, int slotId) throws NoSuchElementException {
        // if associated bit is not set, read forward to the next tuple, and
        // return null.
        if (!isSlotUsed(slotId)) {
            for (int i=0; i<td.getSize(); i++) {
                try {
                    dis.readByte();
                } catch (IOException e) {
                    throw new NoSuchElementException("error reading empty tuple");
                }
            }
            return null;
        }

        // read fields in the tuple
        Tuple t = new Tuple(td);
        RecordId rid = new RecordId(pid, slotId);
        t.setRecordId(rid);
        try {
            for (int j=0; j<td.numFields(); j++) {
                Field f = td.getFieldType(j).parse(dis);
                t.setField(j, f);
            }
        } catch (java.text.ParseException e) {
            e.printStackTrace();
            throw new NoSuchElementException("parsing error!");
        }

        return t;
    }

    /**
     * Generates a byte array representing the contents of this page.
     * Used to serialize this page to disk.
     * <p>
     * The invariant here is that it should be possible to pass the byte
     * array generated by getPageData to the HeapPage constructor and
     * have it produce an identical HeapPage object.
     *
     * @see #HeapPage
     * @return A byte array correspond to the bytes of this page.
     */
    public byte[] getPageData() {
        int len = BufferPool.getPageSize();
        ByteArrayOutputStream baos = new ByteArrayOutputStream(len);
//        ByteArrayOutputStream 的主要作用是在内存中创建一个缓冲区（即字节数组），并将数据写入该缓冲区
//        ByteArrayOutputStream 负责提供存储数据的缓冲区。数据被写入 ByteArrayOutputStream 的缓冲区中，以字节数组的形式存储。
//        baos会自动增长：当写入的数据超出当前缓冲区的容量时，ByteArrayOutputStream 会自动扩展内部缓冲区的大小
        DataOutputStream dos = new DataOutputStream(baos);//构造函数把两者关联 DataOutputStream m负责将数据转换为二进制格式并写入到 ByteArrayOutputStream 中
        // create the header of the page
        for (byte b : header) {//遍历页面的头部数组 header，并将每个字节写入到 dos 中，以创建页面的头部。
            try {
                dos.writeByte(b);
            } catch (IOException e) {
                // this really shouldn't happen
                e.printStackTrace();
            }
        }

        // create the tuples
        for (int i=0; i<tuples.length; i++) {

            // empty slot
            //通过循环遍历字段数量 td.getSize()，将字节值 0 的数据写入 dos。
            if (!isSlotUsed(i)) {
                for (int j=0; j<td.getSize(); j++) {
                    try {
                        dos.writeByte(0);
                    } catch (IOException e) {
                        e.printStackTrace();
                    }

                }
                continue;
            }

            // non-empty slot，遍历字段，把字段写入
            for (int j=0; j<td.numFields(); j++) {
                Field f = tuples[i].getField(j);
                try {
                    f.serialize(dos);
                
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }

        // padding
        int zerolen = BufferPool.getPageSize() - (header.length + td.getSize() * tuples.length); //- numSlots * td.getSize();计算剩余空间大小
        byte[] zeroes = new byte[zerolen];
        try {
            dos.write(zeroes, 0, zerolen);
        } catch (IOException e) {
            e.printStackTrace();
        }

//        调用 dos.flush() 将缓冲区的数据刷新到 baos，并返回 baos.toByteArray()，即页面数据的字节数组表示
        try {
            dos.flush();//DataOutputStream 的 flush() 方法将缓冲区中的数据刷新到底层的输出流中，但并不直接将数据写入磁盘。
            //FileOutputStream 的 flush() 或 FileOutputStream 对象的 close() 方法可以确保数据被及时地写入磁盘。
        } catch (IOException e) {
            e.printStackTrace();
        }

        return baos.toByteArray();
    }

    /**
     * Static method to generate a byte array corresponding to an empty
     * HeapPage.
     * Used to add new, empty pages to the file. Passing the results of
     * this method to the HeapPage constructor will create a HeapPage with
     * no valid tuples in it.
     *
     * @return The returned ByteArray.
     */
    public static byte[] createEmptyPageData() {
        int len = BufferPool.getPageSize();
        return new byte[len]; //all 0
    }

    /**
     * Delete the specified tuple from the page; the corresponding header bit should be updated to reflect
     *   that it is no longer stored on any page.
     * @throws DbException if this tuple is not on this page, or tuple slot is
     *         already empty.
     * @param t The tuple to delete
     */
    public void deleteTuple(Tuple t) throws DbException {
        // done
        RecordId recordId = t.getRecordId();
        int slotId = recordId.getTupleNumber();
        if(recordId.getPageId() != pid || !isSlotUsed(slotId))
            throw new DbException("tuple is not in this page");
        markSlotUsed(slotId,false);
        tuples[slotId] = null;
    }

    /**
     * Adds the specified tuple to the page;  the tuple should be updated to reflect
     *  that it is now stored on this page.
     * @throws DbException if the page is full (no empty slots) or tupledesc
     *         is mismatch.
     * @param t The tuple to add.
     */
    public void insertTuple(Tuple t) throws DbException {
        // done
        if(getNumTuples()==0 || !t.getTupleDesc().equals(td))
            throw new DbException("full page or tupledesc mismatch");
        for(int i = 0;i<numSlots;i++){
            if(!isSlotUsed(i)){
                markSlotUsed(i,true);
                t.setRecordId(new RecordId(pid,i));
                tuples[i] = t;
                break;
            }
        }
    }

    /**
     * Marks this page as dirty/not dirty and record that transaction
     * that did the dirtying
     */
    public void markDirty(boolean dirty, TransactionId tid) {
        //done
        this.dirty = dirty;
        this.tid = tid;
    }

    /**
     * Returns the tid of the transaction that last dirtied this page, or null if the page is not dirty
     */
    public TransactionId isDirty() {
        //done
       if(!dirty){
            return null;
        }
        return this.tid;
    }

    /**
     * Returns the number of empty slots on this page.
     */
    public int getNumEmptySlots() {
        // done
        int num=0;
        for(int i=0;i<numSlots;i++)
            if(!isSlotUsed(i))
                num++;
        return num;
    }

    /**
     * Returns true if associated slot on this page is filled.
     */
    public boolean isSlotUsed(int i) {
        // done
//        数组 header 中的每个字节对应 8 个槽位的使用情况。如果位的值为 1，则表示对应的槽位被使用；为 0，则未被使用。
//        例如，header[0] 表示第 0-7 个槽位的使用情况，其中最右边的位表示第 0 号槽位，最左边的位表示第 7 号槽位。
        // header是字节数组,i是位图中第几个槽,所以要限定为到处于哪个字节,再定位位于第几位
        int Index = i/8;//header数组第几位
        int offset = i%8; //一个Byte中的那一个bit
        return ((header[Index]>>offset & 1) == 1);//通过右移位操作，将待检查bit移动到最右
    }

    /**
     * Abstraction to fill or clear a slot on this page.
     */
    private void markSlotUsed(int i, boolean value) {
        //done
        int index = i/8;
        int offset = i%8;
        int tmp = 1<< (offset);//假设 i 的值为 3，则offset 的值为 3
        // 则左移操作 i << offset 将 1 左移 3 位，得到 00001000，生成除了offset对应位全0的数
        byte b = header[index];//例如b为00110011
        if(value)
            header[index]= (byte)(b|tmp);//value为true时。header[index] = 00110011 | 00001000 = 00111011 从右往左编第四个由0变1
        else
            header[index]= (byte)(b & ~tmp);//value为false时，header[index]=00110011 & 11110111 = 00110011
    }

    /**
     * @return an iterator over all tuples on this page (calling remove on this iterator throws an UnsupportedOperationException)
     * (note that this iterator shouldn't return tuples in empty slots!)------注意
     */
    public Iterator<Tuple> iterator() {
        // done
        List<Tuple> list = new ArrayList<>();
        for(int i=0;i<numSlots;i++){
            if(isSlotUsed(i))
                list.add(tuples[i]);//一个slot存一个tuple
        }
        return list.iterator();
    }

}

