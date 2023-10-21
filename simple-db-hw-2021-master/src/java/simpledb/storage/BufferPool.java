package simpledb.storage;

import simpledb.common.Database;
import simpledb.common.Permissions;
import simpledb.common.DbException;
import simpledb.common.DeadlockException;
import simpledb.transaction.LockManager;
import simpledb.transaction.TransactionAbortedException;
import simpledb.storage.LRUCache;
import simpledb.transaction.TransactionId;
import java.util.*;
import java.io.*;

import java.util.concurrent.ConcurrentHashMap;

/**
 * BufferPool manages the reading and writing of pages into memory from
 * disk. Access methods call into it to retrieve pages, and it fetches
 * pages from the appropriate location.
 * <p>
 * The BufferPool is also responsible for locking;  when a transaction fetches
 * a page, BufferPool checks that the transaction has the appropriate
 * locks to read/write the page.
 * 
 * @Threadsafe, all fields are final
 */
public class BufferPool {
    /** Bytes per page, including header. */
    private static final int DEFAULT_PAGE_SIZE = 4096;
    private static int pageSize = DEFAULT_PAGE_SIZE;
    
    /** Default number of pages passed to the constructor. This is used by
    other classes. BufferPool should use the numPages argument to the
    constructor instead. */
    public static final int DEFAULT_PAGES = 50;

    /**
     * Creates a BufferPool that caches up to numPages pages.
     *
     * @param numPages maximum number of pages in this buffer pool.
     */
    private Integer numPages;
    private LRUCache<PageId,Page> buffer;
    private LockManager lockManager;
    public BufferPool(int numPages) {
        // done
        this.numPages =numPages;
        buffer = new LRUCache<>(numPages);
        this.lockManager = new LockManager();
    }
    
    public static int getPageSize() {
      return pageSize;
    }
    
    // THIS FUNCTION SHOULD ONLY BE USED FOR TESTING!!
    public static void setPageSize(int pageSize) {
        BufferPool.pageSize = pageSize;
    }
    
    // THIS FUNCTION SHOULD ONLY BE USED FOR TESTING!!
    public static void resetPageSize() {
        BufferPool.pageSize = DEFAULT_PAGE_SIZE;
    }

    /**
     * Retrieve the specified page with the associated permissions.
     * Will acquire a lock and may block if that lock is held by another
     * transaction.
     * <p>
     * The retrieved page should be looked up in the buffer pool.  If it
     * is present, it should be returned.  If it is not present, it should
     * be added to the buffer pool and returned.  If there is insufficient
     * space in the buffer pool, a page should be evicted and the new page
     * should be added in its place.
     *
     * @param tid the ID of the transaction requesting the page
     * @param pid the ID of the requested page
     * @param perm the requested permissions on the page
     */
    public  Page getPage(TransactionId tid, PageId pid, Permissions perm)
        throws TransactionAbortedException, DbException {
        // done
        //找到就返回page，没找到要新增
       lockManager.grantLock(tid,pid,perm);
        if (this.buffer.get(pid)==null) {
            // find the right page in DBFiles
            DbFile dbFile = Database.getCatalog().getDatabaseFile(pid.getTableId());
            Page page = dbFile.readPage(pid);
            if (buffer.getSize() >= numPages) {
                evictPage();
            }
            buffer.put(pid, page);
            return page;
        }
        return this.buffer.get(pid);
    }

    /**
     * Releases the lock on a page.
     * Calling this is very risky, and may result in wrong behavior. Think hard
     * about who needs to call this and why, and why they can run the risk of
     * calling it.
     *
     * @param tid the ID of the transaction requesting the unlock
     * @param pid the ID of the page to unlock
     */
    public  void unsafeReleasePage(TransactionId tid, PageId pid) {
        // done
        lockManager.releaseLock(tid,pid);
    }

    /**
     * Release all locks associated with a given transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     */
    public void transactionComplete(TransactionId tid) {
        // done
        transactionComplete(tid,true);
    }

    /** Return true if the specified transaction has a lock on the specified page */
    public boolean holdsLock(TransactionId tid, PageId p) {
        // done
        return lockManager.holdLock(tid,p);
    }

    /**
     * Commit or abort a given transaction; release all locks associated to
     * the transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     * @param commit a flag indicating whether we should commit or abort
     */
    public void transactionComplete(TransactionId tid, boolean commit) {
        // done
        Set<LockManager.PageLock> locks = lockManager.getPages(tid);
        if(commit){
            try{
                flushPages(tid);
            }catch (Exception e){
                e.printStackTrace();
            }
        }
        else {
            rollback(tid);
            }
        for(LockManager.PageLock plk:locks){
            unsafeReleasePage(tid,plk.pid);
        }
    }

    /**
     * Add a tuple to the specified table on behalf of transaction tid.  Will
     * acquire a write lock on the page the tuple is added to and any other 
     * pages that are updated (Lock acquisition is not needed for lab2). 
     * May block if the lock(s) cannot be acquired.
     * 
     * Marks any pages that were dirtied by the operation as dirty by calling
     * their markDirty bit, and adds versions of any pages that have 
     * been dirtied to the cache (replacing any existing versions of those pages) so 
     * that future requests see up-to-date pages. 
     *
     * @param tid the transaction adding the tuple
     * @param tableId the table to add the tuple to
     * @param t the tuple to add
     */
    public void insertTuple(TransactionId tid, int tableId, Tuple t)
        throws DbException, IOException, TransactionAbortedException {
       //done
        DbFile dbFile = Database.getCatalog().getDatabaseFile(tableId);
        List<Page> pages = dbFile.insertTuple(tid,t);//@return An ArrayList contain the pages that were modified
        for(Page page: pages){
            page.markDirty(true,tid);
            buffer.put(page.getId(),page);
        }
    }

    /**
     * Remove the specified tuple from the buffer pool.
     * Will acquire a write lock on the page the tuple is removed from and any
     * other pages that are updated. May block if the lock(s) cannot be acquired.
     *
     * Marks any pages that were dirtied by the operation as dirty by calling
     * their markDirty bit, and adds versions of any pages that have 
     * been dirtied to the cache (replacing any existing versions of those pages) so 
     * that future requests see up-to-date pages. 
     *
     * @param tid the transaction deleting the tuple.
     * @param t the tuple to delete
     */
    public  void deleteTuple(TransactionId tid, Tuple t)
        throws DbException, IOException, TransactionAbortedException {
        // done
        DbFile dbFile = Database.getCatalog().getDatabaseFile(t.getRecordId().getPageId().getTableId());
        List<Page> pages =dbFile.deleteTuple(tid,t);
        for(Page page: pages){
            page.markDirty(true , tid);
        }
    }

    /**
     * Flush all dirty pages to disk.
     * NB: Be careful using this routine -- it writes dirty data to disk so will
     *     break simpledb if running in NO STEAL mode.
     */
    public synchronized void flushAllPages() throws IOException {
        // done
        LRUCache<PageId, Page>.DLinkedNode head = buffer.getHead();
        LRUCache<PageId, Page>.DLinkedNode tail = buffer.getTail();
        while(head!=tail){
            Page page = head.value;
            if(page!=null && page.isDirty()!=null){
                DbFile dbFile = Database.getCatalog().getDatabaseFile(page.getId().getTableId());
                //记录日志
                try{
                    Database.getLogFile().logWrite(page.isDirty(),page.getBeforeImage(),page);
                    Database.getLogFile().force();//强制把log日志写到磁盘
                    dbFile.writePage(page);//将页面写入磁盘
                }catch (IOException e){
                    e.printStackTrace();
                }
            }
            head = head.next;
        }
    }

    /** Remove the specific page id from the buffer pool.
        Needed by the recovery manager to ensure that the
        buffer pool doesn't keep a rolled back page in its
        cache.
        
        Also used by B+ tree files to ensure that deleted pages
        are removed from the cache so they can be reused safely
    */
    public synchronized void discardPage(PageId pid) {
        // done
        LRUCache<PageId, Page>.DLinkedNode head = buffer.getHead();
        LRUCache<PageId, Page>.DLinkedNode tail = buffer.getTail();
        while(head!=tail){
            PageId key = head.key;
            if(key!=null && key.equals(pid)){
                buffer.remove(head);
                return;
            }
            head = head.next;
        }
    }

    /**
     * Flushes a certain page to disk
     * @param pid an ID indicating the page to flush
     */
    private synchronized  void flushPage(PageId pid) throws IOException {
        //done
        Page page = buffer.get(pid);
        if (page.isDirty() != null) {
            DbFile dbFile = Database.getCatalog().getDatabaseFile(page.getId().getTableId());
            try {
                Database.getLogFile().logWrite(page.isDirty(), page.getBeforeImage(), page);
                Database.getLogFile().force();
                page.markDirty(false, null);
                dbFile.writePage(page);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }
    /** Write all pages of the specified transaction to disk.
     */
    public synchronized  void flushPages(TransactionId tid) throws IOException {
        // done
        Set<LockManager.PageLock> locks = lockManager.getPages(tid);
        for(LockManager.PageLock lock:locks){
            if(     (lock.perm.equals(Permissions.READ_ONLY) && lock.holdNum==1)//1.有这一页。2.是写锁或就一个读锁
                    ||lock.perm.equals(Permissions.READ_WRITE)
                    &&buffer.get(lock.pid)!=null)
            {
                flushPage(lock.pid);
            }

        }
    }

    /**
     * Discards a page from the buffer pool.
     * Flushes the page to disk to ensure dirty pages are updated on disk.
     */
    private synchronized  void evictPage() throws DbException {
        // done
         Page page = buffer.getTail().pre.value;//取出最近最少使用的page
         if(page != null && page.isDirty()!=null)//是空的或者是脏的，不能去这一页
         {findNotDirty();}
         else
         {buffer.discard();}//不是脏页不需要写磁盘,直接扔
    }

    private void findNotDirty() throws DbException {
            LRUCache<PageId,Page>.DLinkedNode head = buffer.getHead();
            LRUCache<PageId,Page>.DLinkedNode tail = buffer.getTail();
            while(head!=tail){
                Page page = tail.value;
                if(page != null && page.isDirty() ==null){//当前tail对应的page不脏也不空，可以去除
                    buffer.remove(tail);
                    return ;
                }
            }
            throw new DbException("no dirty pages");
    }

    private synchronized void rollback(TransactionId tid){
        LRUCache<PageId, Page>.DLinkedNode head = buffer.getHead();
        LRUCache<PageId, Page>.DLinkedNode tail = buffer.getTail();
        while(head!=tail){
            Page page = head.value;
            LRUCache<PageId, Page>.DLinkedNode next = head.next;
            if(page!=null && page.isDirty()!=null && page.isDirty().equals(tid)){//page不空，脏，且就是tid导致的
                buffer.remove(head);//从bufferpool去除
                Page page1 = null;
                try {
                    page1 = Database.getBufferPool().getPage(tid, page.getId(), Permissions.READ_ONLY);//读修改前(磁盘里)的page，实现干净的替换脏的
                    page1.markDirty(false,null);
                } catch (TransactionAbortedException e) {
                    e.printStackTrace();
                } catch (DbException e) {
                    e.printStackTrace();
                }

            }
            head = next;
        }
    }
}
