package simpledb.storage;

import simpledb.common.Database;
import simpledb.common.DbException;
import simpledb.common.Debug;
import simpledb.common.Permissions;
import simpledb.transaction.Transaction;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

import java.io.*;
import java.util.*;

/**
 * HeapFile is an implementation of a DbFile that stores a collection of tuples
 * in no particular order. Tuples are stored on pages, each of which is a fixed
 * size, and the file is simply a collection of those pages. HeapFile works
 * closely with HeapPage. The format of HeapPages is described in the HeapPage
 * constructor.
 * 
 * @see HeapPage#HeapPage
 * @author Sam Madden
 */


//HeapFile中的页是实现了Page接口HeapPage类。页存储在buffer pool中但是通过HeapFile类进行读取或者写入
public class HeapFile implements DbFile {
    private File file;
    private TupleDesc tupleDesc;
    /**
     * Constructs a heap file backed by the specified file.
     * 
     * @param f
     *            the file that stores the on-disk backing store for this heap
     *            file.
     */
    public HeapFile(File f, TupleDesc td) {
        // done
        this.file = f;
        this.tupleDesc = td;
    }

    /**
     * Returns the File backing this HeapFile on disk.
     * 
     * @return the File backing this HeapFile on disk.
     */
    public File getFile() {
        // done
        return file;
    }

    /**
     * Returns an ID uniquely identifying this HeapFile. Implementation note:
     * you will need to generate this tableid somewhere to ensure that each
     * HeapFile has a "unique id," and that you always return the same value for
     * a particular HeapFile. We suggest hashing the absolute file name of the
     * file underlying the heapfile, i.e. f.getAbsoluteFile().hashCode().
     * 
     * @return an ID uniquely identifying this HeapFile.
     */
    public int getId() {
        // done
        return this.file.getAbsoluteFile().hashCode();
    }

    /**
     * Returns the TupleDesc of the table stored in this DbFile.
     * 
     * @return TupleDesc of this DbFile.
     */
    public TupleDesc getTupleDesc() {
        // done
        return tupleDesc;
    }

    // see DbFile.java for javadocs
    public Page readPage(PageId pid) {
        // done
        //计算page对应的偏移量
        int pageSize = BufferPool.getPageSize();
        int pageNumber = pid.getPageNumber();
        HeapPage heapPage = null ;
        int offset = pageNumber * pageSize;
        try {
            RandomAccessFile randomAccessFile = new RandomAccessFile(file, "r");
//          "r"：是一个字符串参数，表示以只读（read-only）模式打开文件。
            byte [] buf = new byte[pageSize];
            randomAccessFile.seek(offset);//将文件指针移动到指定的偏移量，以便从该位置开始读取数据。
            randomAccessFile.read(buf);//从文件中读取数据，并将数据存储到字节数组 buf 中
            heapPage = new HeapPage((HeapPageId) pid , buf);
            randomAccessFile.close();
        }catch (IOException a){
            a.printStackTrace();
        }
        return heapPage;
    }

    // see DbFile.java for javadocs
    public void writePage(Page page) throws IOException {
        // done
        HeapPageId heapPageId = (HeapPageId) page.getId();
        int size = BufferPool.getPageSize();
        int pageNumber = heapPageId.getPageNumber();
        byte[] pageData = page.getPageData();
        RandomAccessFile randomAccessFile = new RandomAccessFile(file, "rw");
        randomAccessFile.seek(pageNumber* size);
        randomAccessFile.write(pageData);
        randomAccessFile.close();
    }

    /**
     * Returns the number of pages in this HeapFile.
     */
    public int numPages() {
        // done
        long length = this.file.length();//以字节为单位
        return (int)Math.ceil(length*1.0/BufferPool.getPageSize());
    }

    // see DbFile.java for javadocs
    public List<Page> insertTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        //done
        if(!getFile().canRead() || !getFile().canWrite())
            throw new IOException();
        List<Page> modified = new ArrayList<>();
        for(int i=0; i<numPages();i++){//把每页都拿出来看看
            HeapPageId heapPageId = new HeapPageId(getId(),i);
            HeapPage page = (HeapPage) Database.getBufferPool().getPage(tid,heapPageId,Permissions.READ_WRITE);
            if(page == null){
                Database.getBufferPool().unsafeReleasePage(tid,heapPageId);
                continue;
            }
            if(page.getNumEmptySlots()==0){
                //没有空的slot，应该释放锁，避免占用page
                Database.getBufferPool().unsafeReleasePage(tid,heapPageId);
                continue;
            }
            page.insertTuple(t);
//           不需要 page.markDirty(true,tid);因为bufferpool是最上层的管理者，在它那边，已经做了markDirty
            modified.add(page);
            return modified;
        }
        //每页都满，需要创建新的页
        HeapPageId heapPageId = new HeapPageId(getId(), numPages());
        HeapPage heapPage = new HeapPage(heapPageId, HeapPage.createEmptyPageData());
        heapPage.insertTuple(t);
        writePage(heapPage);
        modified.add(heapPage);
        return modified;
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> deleteTuple(TransactionId tid, Tuple t) throws DbException,
            TransactionAbortedException {
        // done
        ArrayList<Page> modified = new ArrayList<>();
        HeapPageId heapPageId  = (HeapPageId) t.getRecordId().getPageId();
        HeapPage page = (HeapPage) Database.getBufferPool().getPage(tid,heapPageId,Permissions.READ_WRITE);
        if(page==null){
            throw  new DbException("null");
        }
        page.deleteTuple(t);
//       不需要 page.markDirty(true,tid);，因为bufferpool是最上层的管理者，在它那边，已经做了markDirty
        modified.add(page);
        return modified;
    }

    // see DbFile.java for javadocs
    public DbFileIterator iterator(TransactionId tid) {
        // done
        return new HeapFileIterator(tid ,Permissions.READ_ONLY);
    }

    public  class HeapFileIterator implements DbFileIterator{
        Permissions permissions;
        TransactionId tid;
        private Iterator<Tuple> iterator;//每个元组的迭代器
        private int pageNumber;
        BufferPool bufferPool = Database.getBufferPool();
        public HeapFileIterator (TransactionId tid , Permissions permissions){this.tid=tid;this.permissions=permissions;}
        public void open() throws DbException, TransactionAbortedException{
            pageNumber = 0 ;
            HeapPageId heapPageId = new HeapPageId(getId(),pageNumber);
            HeapPage heapPage = (HeapPage) this.bufferPool.getPage(tid,heapPageId,permissions);
            if(heapPage == null ) throw new DbException("page null");
            else iterator = heapPage.iterator();
        }
        public boolean hasNextPage() throws DbException , TransactionAbortedException{
            while(true){
                pageNumber ++;
                if(pageNumber>numPages())
                    return false;
                HeapPageId heapPageId = new HeapPageId(getId(),pageNumber);
                HeapPage heapPage = (HeapPage) this.bufferPool.getPage(tid,heapPageId,permissions);
                if(heapPage == null ) continue;
                iterator = heapPage.iterator();
                return true;
            }
        }
        public boolean hasNext() throws  DbException,TransactionAbortedException{
            if(iterator == null ) return false;
            if(iterator.hasNext()) return true;
            else return hasNextPage();
        }
        public Tuple next()throws DbException , TransactionAbortedException , NoSuchElementException{
            if(iterator == null ) throw  new NoSuchElementException();
            return iterator.next();
        }
        public void rewind() throws DbException , TransactionAbortedException{
            close();
            open();
        }
        public void close(){ iterator =null ;}
    }

}

