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
public class HeapFile implements DbFile {

    private TupleDesc tupleDesc;
    private File file;
    private int numPage;
    /**
     * Constructs a heap file backed by the specified file.
     * 
     * @param f
     *            the file that stores the on-disk backing store for this heap
     *            file.
     */
    public HeapFile(File f, TupleDesc td) {
        // some code goes here
        file=f;
        numPage=(int)(file.length()/BufferPool.getPageSize());
        tupleDesc=td;
    }

    /**
     * Returns the File backing this HeapFile on disk.
     * 
     * @return the File backing this HeapFile on disk.
     */
    public File getFile() {
        // some code goes here
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
        // some code goes here
        return file.getAbsoluteFile().hashCode();
    }

    /**
     * Returns the TupleDesc of the table stored in this DbFile.
     * 
     * @return TupleDesc of this DbFile.
     */
    public TupleDesc getTupleDesc() {
        // some code goes here
        return tupleDesc;
    }

    // see DbFile.java for javadocs
    public Page readPage(PageId pid) throws IllegalArgumentException {
        // some code goes here
        if(pid.getTableId()!=getId()) {
            throw new IllegalArgumentException();
        }
        Page page=null;
        byte[] data=new byte[BufferPool.getPageSize()];
        try(RandomAccessFile raf=new RandomAccessFile(getFile(),"r")) {
            //page在HeapFile的偏移量
            int pos=pid.getPageNumber()*BufferPool.getPageSize();
            raf.seek(pos);
            raf.read(data,0,data.length);
            page=new HeapPage((HeapPageId)pid,data);
        }catch (IOException e) {
            e.printStackTrace();
        }
        return page;
    }

    // see DbFile.java for javadocs
    public void writePage(Page page) throws IOException {
        // some code goes here
        // not necessary for lab1
        try(RandomAccessFile raf=new RandomAccessFile(file,"rw")) {
            raf.seek(page.getId().getPageNumber()*BufferPool.getPageSize());
            byte[] data=page.getPageData();
            raf.write(data);
        }
    }

    /**
     * Returns the number of pages in this HeapFile.
     */
    public int numPages() {
        // some code goes here
        return numPage;
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> insertTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        ArrayList<Page> affectedPages=new ArrayList<>();
        for(int i=0;i<numPages();i++){
            HeapPageId heapPageId=new HeapPageId(getId(),i);
            HeapPage page= null;
            try {
                page = (HeapPage) Database.getBufferPool().getPage(tid,heapPageId, Permissions.READ_WRITE);
            } catch (Exception e) {
                e.printStackTrace();
            }
            if(page.getNumEmptySlots()!=0){
                //page的insertTuple已经负责修改tuple信息表明其存储在该page上
                page.insertTuple(t);
                page.markDirty(true,tid);
                affectedPages.add(page);
                break;
            }
        }
        if(affectedPages.size()==0){
            //说明page都已经满了
            //创建新的page
            HeapPageId newHeapPage=new HeapPageId(getId(),numPages());
            HeapPage blankPage=new HeapPage(newHeapPage,HeapPage.createEmptyPageData());
            numPage++;
            writePage(blankPage);//写入磁盘
            //通过BufferPool来访问该新的page
            HeapPage newPage= null;
            try {
                newPage = (HeapPage) Database.getBufferPool().getPage(tid,newHeapPage, Permissions.READ_WRITE);
            } catch (Exception e) {
                e.printStackTrace();
            }
            newPage.insertTuple(t);
            newPage.markDirty(true,tid);
            affectedPages.add(newPage);
        }
        return affectedPages;
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> deleteTuple(TransactionId tid, Tuple t) throws DbException,
            TransactionAbortedException {
        // some code goes here
        PageId pid=t.getRecordId().getPageId();
        ArrayList<Page> affectedPages=new ArrayList<>();
        HeapPage affectedPage=null;
        for(int i=0;i<numPages();i++){
            if(i==pid.getPageNumber()){
                try {
                    affectedPage=(HeapPage)Database.getBufferPool().getPage(tid,pid,Permissions.READ_WRITE);
                } catch (Exception e) {
                    e.printStackTrace();
                }
                affectedPage.deleteTuple(t);
                affectedPages.add(affectedPage);
            }
        }
        if(affectedPage==null)
            throw new DbException("tuple " + t + " is not in this table");
        return affectedPages;
    }

    // see DbFile.java for javadocs
    public DbFileIterator iterator(TransactionId tid) {
        // some code goes here
        return new HeapFileIterator(tid);
    }

    /**
     * tableid就是heapfile的id，即通过getId.
     * PageId是从0开始的
     * transactionId由调用者提供
     * 存储一个当前正在遍历页的tuples迭代器的引用，这样一页页来遍历
     */
    private class HeapFileIterator implements DbFileIterator{

        private int pagePos;
        private Iterator<Tuple> tupleInPage;
        private TransactionId tid;
        public HeapFileIterator(TransactionId tid){
            this.tid=tid;
        }
        public Iterator<Tuple> getTupleInPage(HeapPageId pid) throws TransactionAbortedException,DbException{
            //不能直接使用HeapFile的readPage方法，而是通过BufferPool来获得page,
            HeapPage page= null;
            try {
                page = (HeapPage) Database.getBufferPool().getPage(tid,pid, Permissions.READ_ONLY);
            } catch (Exception e) {
                e.printStackTrace();
            }
            return page.iterator();
        }
        @Override
        public void open() throws DbException, TransactionAbortedException {

            pagePos=0;
            HeapPageId pid=new HeapPageId(getId(),pagePos);
            //加载第一页的tuples
            tupleInPage=getTupleInPage(pid);
        }

        @Override
        public boolean hasNext() throws DbException, TransactionAbortedException {
            if(tupleInPage==null)
                return false;
            if(tupleInPage.hasNext())
                return true;
            if(pagePos<numPages()-1){
                pagePos++;
                HeapPageId pid=new HeapPageId(getId(),pagePos);
                tupleInPage=getTupleInPage(pid);
                //这时不能直接return true,有可能返回的新的迭代器不含有tuple的
                return tupleInPage.hasNext();
            }else return false;
        }

        @Override
        public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
            if(!hasNext())
                throw new NoSuchElementException("not open or no tuple remained");
            return tupleInPage.next();
        }

        @Override
        public void rewind() throws DbException, TransactionAbortedException {

            //初始化一次
            open();
        }

        @Override
        public void close() {

            pagePos=0;
            tupleInPage=null;
        }
    }

}

