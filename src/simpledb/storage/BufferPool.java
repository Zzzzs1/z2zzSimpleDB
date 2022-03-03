package simpledb.storage;

import simpledb.common.Database;
import simpledb.common.Permissions;
import simpledb.common.DbException;
import simpledb.common.DeadlockException;
import simpledb.transaction.LockManager;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

import java.io.*;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

/**
 * BufferPool manages the reading and writing of pages into memory from
 * disk. Access methods call into it to retrieve pages, and it fetches
 * pages from the appropriate location.
 * <p>
 * The BufferPool is also responsible for locking;  when a transaction fetches
 * a page, BufferPool checks that the transaction has the appropriate
 * locks to read/write the page.
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
    //页最大数量
    public final int Pages_NUM;
    //当前缓存页
    private PageLruCache  lruPagesPool;
    private final LockManager lockManager;
    //获取不到锁的等待时长
    private final long SLEEP_INTERVAL;

    /**
     * Creates a BufferPool that caches up to numPages pages.
     *
     * @param numPages maximum number of pages in this buffer pool.
     */
    public BufferPool(int numPages) {
        // some code goes here
        Pages_NUM=numPages;
        lruPagesPool=new PageLruCache(Pages_NUM);
        lockManager=new LockManager();
        SLEEP_INTERVAL=500;
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
//    public  Page getPage(TransactionId tid, PageId pid, Permissions perm)
//            throws Exception {
//        // some code goes here
//        boolean res=(perm==Permissions.READ_ONLY)?lockManager.grantSLock(tid,pid)
//                :lockManager.grantXLock(tid,pid);
//        while (!res){
//            if(lockManager.deadlockOccurred(tid,pid)){
//                throw new TransactionAbortedException();
//            }
//            Thread.sleep(SLEEP_INTERVAL);
//            res=(perm==Permissions.READ_ONLY)?lockManager.grantSLock(tid,pid)
//                    :lockManager.grantXLock(tid,pid);
//        }
//        HeapPage page = (HeapPage) lruPagesPool.get(pid);
//        if (page != null) {//直接命中
//            return page;
//        }
//        //未命中，访问磁盘并将其缓存
//        HeapFile table = (HeapFile) Database.getCatalog().getDatabaseFile(pid.getTableId());
//        HeapPage newPage = (HeapPage) table.readPage(pid);
//        Page removedPage = lruPagesPool.put(pid, newPage);
//        if (removedPage != null) {
//            try {
//                flushPage(removedPage);
//            } catch (IOException e) {
//                e.printStackTrace();
//            }
//        }
//        return newPage;
//    }
    public Page getPage(TransactionId tid, PageId pid, Permissions perm)
            throws TransactionAbortedException, DbException, InterruptedException {
        // some code goes here
        boolean result = (perm == Permissions.READ_ONLY) ? lockManager.grantSLock(tid, pid)
                : lockManager.grantXLock(tid, pid);
        //下面的while循环就是在模拟等待过程，隔一段时间就检查一次是否申请到锁了，还没申请到就检查是否陷入死锁
        while (!result) {
            if (lockManager.deadlockOccurred(tid, pid)) {
                throw new TransactionAbortedException();
            }
            Thread.sleep(SLEEP_INTERVAL);
            //sleep之后再次判断result
            result = (perm == Permissions.READ_ONLY) ? lockManager.grantSLock(tid, pid)
                    : lockManager.grantXLock(tid, pid);
        }

        Page page = lruPagesPool.get(pid);
        if (page != null) {//直接命中
            return page;
        }
        //未命中，访问磁盘并将其缓存
        DbFile table =  Database.getCatalog().getDatabaseFile(pid.getTableId());
        Page newPage = table.readPage(pid);
        Page removedPage = lruPagesPool.put(pid, newPage);
        if (removedPage != null) {
            try {
                flushPage(removedPage);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        return newPage;

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
    public  synchronized void unsafeReleasePage(TransactionId tid, PageId pid) {
        // some code goes here
        // not necessary for lab1|lab2
        if(!lockManager.unlock(tid,pid))
            throw new IllegalArgumentException();
    }

    /**
     * Release all locks associated with a given transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     */
    public void transactionComplete(TransactionId tid) {
        // some code goes here
        // not necessary for lab1|lab2
        transactionComplete(tid,true);
    }

    /** Return true if the specified transaction has a lock on the specified page */
    public boolean holdsLock(TransactionId tid, PageId p) {
        // some code goes here
        // not necessary for lab1|lab2
        return lockManager.getLockState(tid,p)!=null;
    }

    /**
     * Commit or abort a given transaction; release all locks associated to
     * the transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     * @param commit a flag indicating whether we should commit or abort
     */
    public void transactionComplete(TransactionId tid, boolean commit) {
        // some code goes here
        // not necessary for lab1|lab2
        lockManager.releaseTransactionLocks(tid);
        if(commit){
            try {
                flushPages(tid);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }else{
            revertTransactionAction(tid);
        }
    }
    /**
     * 事务回滚时，撤销该事务对page造成的改变
     * @param tid
     */
    public synchronized void revertTransactionAction(TransactionId tid){
        Iterator<Page> it=lruPagesPool.iterator();
        while (it.hasNext()){
            Page p=it.next();
            if(p.isDirty()!=null&&p.isDirty().equals(tid)){
                lruPagesPool.reCachePage(p.getId());
            }
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
        // some code goes here
        // not necessary for lab1
        DbFile table=Database.getCatalog().getDatabaseFile(tableId);
        List<Page> affectedPages=table.insertTuple(tid,t);
        for(Page page:affectedPages){
            page.markDirty(true,tid);
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
        // some code goes here
        // not necbessary for lab1
        int tableId=t.getRecordId().getPageId().getTableId();
        DbFile table=Database.getCatalog().getDatabaseFile(tableId);
        List<Page> affectedPages=table.deleteTuple(tid,t);
        for(Page page:affectedPages){
            page.markDirty(true,tid);
        }
    }

    /**
     * Flush all dirty pages to disk.
     * NB: Be careful using this routine -- it writes dirty data to disk so will
     *     break simpledb if running in NO STEAL mode.
     */
    public synchronized void flushAllPages() throws IOException {
        // some code goes here
        // not necessary for lab1
        Iterator<Page> it=lruPagesPool.iterator();
        while (it.hasNext()){
            Page p=it.next();
            if(p.isDirty()!=null)
            flushPage(p);
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
        // some code goes here
        // not necessary for lab1
    }

    /**
     * Flushes a certain page to disk
     * @param page an ID indicating the page to flush
     */
    private synchronized  void flushPage(Page page) throws IOException {
        // some code goes here
        // not necessary for lab1
        Page dirty_page=page;
        DbFile table=Database.getCatalog().getDatabaseFile(page.getId().getTableId());
        table.writePage(dirty_page);
        dirty_page.markDirty(false,null);
    }

    /** Write all pages of the specified transaction to disk.
     * 将事务相关的脏页刷新到磁盘
     */
    public synchronized  void flushPages(TransactionId tid) throws IOException {
        // some code goes here
        // not necessary for lab1|lab2
        Iterator<Page> it=lruPagesPool.iterator();
        while (it.hasNext()){
            Page p=it.next();
            if(p.isDirty()!=null&&p.isDirty().equals(tid)){
                flushPage(p);
                if(p.isDirty()==null){
                    p.setBeforeImage();
                }
            }
        }
    }

    /**
     * Discards a page from the buffer pool.
     * Flushes the page to disk to ensure dirty pages are updated on disk.
     * 这个方法不实现了，具体的替换策略已经在LruCache中体现了，标志deprecated好了
     */
    @Deprecated
    private synchronized  void evictPage() throws DbException {
        // some code goes here
        // not necessary for lab1
    }

}
