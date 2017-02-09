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
	public int tableId; 
	public Page[] pages;
	public RandomAccessFile raf; 
	
    public HeapFile(File f, TupleDesc td) {
        // some code goes here
    	this.f = f;
    	this.td = td;
    	this.pages = new Page[numPages()];
    	this.tableId = f.getAbsoluteFile().hashCode(); 
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
    	this.tableId = f.getAbsoluteFile().hashCode();
    	return tableId;
        
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
    public Page readPage(PageId pid) throws IllegalArgumentException, IndexOutOfBoundsException  {
        // some code goes here
    	
    	 
    	int PageNumber = pid.pageNumber();
    	int pageSize = BufferPool.PAGE_SIZE;
    	 
    	
    	
		RandomAccessFile disk;
		byte[] page = new byte[pageSize];
		try {
			if(raf == null){
				raf = new RandomAccessFile(f, "r");
				raf.read(page, 0, pageSize);
				raf.close();
			}
			else{

				raf.read(page, 0, pageSize);
			 
			}
			
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		HeapPageId id = new HeapPageId((pid.getTableId()), PageNumber);
		
		try {
			HeapPage temp = new HeapPage(id,page);
			System.out.println("returning page");
			return temp;
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

    
public class HeapFileIterator<Page> implements DbFileIterator{
    	
    	public int index;
    	public Iterator<Tuple> pageIt;
    	HeapPage p; 
    	public int pageNumbers = numPages();
    	public int pageSize = BufferPool.PAGE_SIZE;
    	int tupleIndex = 0; 
    	public TransactionId tid; 
    	boolean open = false; 
    	
		public HeapFileIterator(TransactionId tid) {
			// TODO Auto-generated constructor stub
			this.tid = tid;
			this.index = 0; 
			this.pageIt = null;
		}
		@Override
		public void open() throws DbException, TransactionAbortedException {
			// TODO Auto-generated method stub
			try {
				raf = new RandomAccessFile(f, "r");
				open = true; 
			} catch (FileNotFoundException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		@Override
		public boolean hasNext() throws DbException,
				TransactionAbortedException {
			 		
			if(open){
				System.out.print(index + ", ");
				System.out.println(pageNumbers);
				System.out.println("its open");
				//create a HeapPageId for page #index
				PageId id = new HeapPageId(getId(), index);
				//get this page from the BufferPool
			    p = (HeapPage) Database.getBufferPool().getPage(tid, id, Permissions.READ_ONLY);
			    //make new tuples iterator if necessary
			    if (pageIt == null){
			    	System.out.println("making new tuples iterator");
			    	this.pageIt =  p.iterator();
			    }
				//if there is a tuple return true
				if (this.pageIt.hasNext()){
					System.out.println("it has next tuple");
					return true; 	
				}
				//if there is not another tuple on this page go to the next page in the file
				index++;
				//if the current page is larger than the number of pages in this file return false
				if (index > pageNumbers - 1){
					return false; 
				}
				//otherwise set pageIt to null and recurse
				else {	
					System.out.println("recursing");
					pageIt = null;
					return hasNext();		
				}
			}
			//return false if iterator is not open
			else{
				return false; 
			}
		}
		@Override
		public Tuple next() throws DbException, TransactionAbortedException,NoSuchElementException {
			if(pageIt == null){
				throw new NoSuchElementException();
			}
			else if(pageIt.hasNext()){
				
				Tuple forReturn = pageIt.next();
				System.out.println("returning tuple");
				System.out.println(forReturn);
				return forReturn;
			}
			else{
				return null;
			}
			 
		}
		@Override
		public void rewind() throws DbException, TransactionAbortedException {
			index = 0; 
			pageIt = null; 
			
		}
		@Override
		public void close() {
			// TODO Auto-generated method stub
			try {
				raf.close();
				open = false; 
				pageIt = null;
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
    	
    	
		
    }
    // see DbFile.java for javadocs
    public DbFileIterator iterator(TransactionId tid)  {
        // some code goes here
        return new HeapFileIterator<Page>(tid);
    }

}

