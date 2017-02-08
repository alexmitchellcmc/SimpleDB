package simpledb;

import java.io.*;
import java.util.*;

/**
 * HeapFile is an implementation of a DbFile that stores a collection of tuples
 * in no particular order. Tuples are stored on pages, each of which is a fixed
 * size, and the file is simply a collection of those pages. HeapFile works
 * closely with HeapPage. The format of HeapPages is described in the HeapPage
 * constructor.
 * 
 * @see simpledb.HeapPage#HeapPage
 * @author Sam Madden
 */
public class HeapFile implements DbFile {

    /**
     * Constructs a heap file backed by the specified file.
     * 
     * @param f
     *            the file that stores the on-disk backing store for this heap
     *            file.
     */
	
	public File f; 
	public TupleDesc td;
	public int id; 
	public HashMap<PageId,Page> pages;
	
	
    public HeapFile(File f, TupleDesc td) {
        // some code goes here
    	this.f = f;
    	this.td = td;
    	this.pages = new HashMap<PageId,Page>();
    }

    /**
     * Returns the File backing this HeapFile on disk.
     * 
     * @return the File backing this HeapFile on disk.
     */
    public File getFile() {
        // some code goes here
    	return f; 
       
    }

    /**
     * Returns an ID uniquely identifying this HeapFile. Implementation note:
     * you will need to generate this tableid somewhere ensure that each
     * HeapFile has a "unique id," and that you always return the same value for
     * a particular HeapFile. We suggest hashing the absolute file name of the
     * file underlying the heapfile, i.e. f.getAbsoluteFile().hashCode().
     * 
     * @return an ID uniquely identifying this HeapFile.
     */
    public int getId() {
        // some code goes her

    	return f.getAbsoluteFile().hashCode();
        
    }

    /**
     * Returns the TupleDesc of the table stored in this DbFile.
     * 
     * @return TupleDesc of this DbFile.
     */
    public TupleDesc getTupleDesc() {
        return td; 
    }

    // see DbFile.java for javadocs
    public Page readPage(PageId pid) throws IllegalArgumentException {
        // some code goes here
    	
    	 
    	int PageNumber = pid.pageNumber();
    	int pageSize = BufferPool.PAGE_SIZE;
    	int heapFileSize = (int) f.length(); 
    	
    	
		RandomAccessFile disk;
		byte[] page = new byte[pageSize];
		try {
			disk = new RandomAccessFile(f, "r");
			disk.read(page, PageNumber*pageSize, pageSize);
			disk.close();
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		HeapPageId id = new HeapPageId((pid.getTableId()), PageNumber);
		HeapPage forReturn;
		try {
			return new HeapPage(id,page);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return null; 
				
	    	
		
		
    	
    }

    // see DbFile.java for javadocs
    public void writePage(Page page) throws IOException {
        // some code goes here
        // not necessary for lab1
    }

    /**
     * Returns the number of pages in this HeapFile.
     */
    public int numPages() {
    	
    	int pageSize = BufferPool.PAGE_SIZE;
    	int heapFileSize = (int) f.length(); 
    	
    	return heapFileSize/pageSize;
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> insertTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        return null;
        // not necessary for lab1
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> deleteTuple(TransactionId tid, Tuple t) throws DbException,
            TransactionAbortedException {
        // some code goes here
        return null;
        // not necessary for lab1
    }

    
private class HeapFileIterator<Page> implements DbFileIterator{
    	
    	public int index;
    	public int length; 
    	public int tuplesPerPage;
    	public RandomAccessFile raf; 
		@Override
		public void open() throws DbException, TransactionAbortedException {
			// TODO Auto-generated method stub
			try {
				raf = new RandomAccessFile(f, "r");
			} catch (FileNotFoundException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		@Override
		public boolean hasNext() throws DbException,
				TransactionAbortedException {
			// TODO Auto-generated method stub
			return false;
		}
		@Override
		public Tuple next() throws DbException, TransactionAbortedException,
				NoSuchElementException {
			// TODO Auto-generated method stub
			return null;
		}
		@Override
		public void rewind() throws DbException, TransactionAbortedException {
			// TODO Auto-generated method stub
			
		}
		@Override
		public void close() {
			// TODO Auto-generated method stub
			
		}
    	
    	
		
    }
    // see DbFile.java for javadocs
    public DbFileIterator iterator(TransactionId tid)  {
        // some code goes here
        return new HeapFileIterator<Page>();
    }

}

