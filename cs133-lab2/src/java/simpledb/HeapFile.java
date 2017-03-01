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
    private final File f;
    private final TupleDesc td;
    private final int tableid ;
    /**
     * Constructs a heap file backed by the specified file.
     * 
     * @param f
     *            the file that stores the on-disk backing store for this heap
     *            file.
     */
    public HeapFile(File f, TupleDesc td) {
        this.f = f;
        this.tableid = f.getAbsoluteFile().hashCode();
        this.td = td;
    }
    /**
     * Returns the File backing this HeapFile on disk.
     * 
     * @return the File backing this HeapFile on disk.
     */
    public File getFile() {
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
    	return tableid;
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
    public Page readPage(PageId pid) {
    	HeapPageId id = (HeapPageId) pid;
        BufferedInputStream bis = null;
        try {
            bis = new BufferedInputStream(new FileInputStream(f));
            byte pageBuf[] = new byte[BufferPool.PAGE_SIZE];
            if (bis.skip(id.pageNumber() * BufferPool.PAGE_SIZE) != id
                    .pageNumber() * BufferPool.PAGE_SIZE) {
                throw new IllegalArgumentException(
                        "Unable to seek to correct place in heapfile");
            }
            int retval = bis.read(pageBuf, 0, BufferPool.PAGE_SIZE);
            if (retval == -1) {
                throw new IllegalArgumentException("Read past end of table");
            }
            if (retval < BufferPool.PAGE_SIZE) {
                throw new IllegalArgumentException("Unable to read "
                        + BufferPool.PAGE_SIZE + " bytes from heapfile");
            }
            HeapPage p = new HeapPage(id, pageBuf);
            return p;
        } catch (IOException e) {
            throw new RuntimeException(e);
        } finally {
            // Close the file on success or error
            try {
                if (bis != null)
                    bis.close();
            } catch (IOException ioe) {
                // Ignore failures closing the file
            }
        }
    }
    // see DbFile.java for javadocs
    public void writePage(Page page) throws IOException {
    	RandomAccessFile rf = new RandomAccessFile(this.f, "rw");
    	int offset = ((HeapPage)page).getId().pageNumber();
    	byte[] pageRet = HeapPage.createEmptyPageData();
    	pageRet = page.getPageData();
    	rf.write(pageRet, offset, pageRet.length);
    	rf.close();
    }
	/**
     * Returns the number of pages in this HeapFile.
     */
    public int numPages() {
       
    	return (int) (f.length() / BufferPool.PAGE_SIZE);
    }
    // see DbFile.java for javadocs
    public ArrayList<Page> insertTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
    	//create Arraylist of modified pages
    	ArrayList<Page> modifiedPages = new ArrayList<Page>();
    	//get bufferpool
    	BufferPool buf = Database.getBufferPool();
    	//find a page to insert tuple into
    	for(int i=0; i<numPages();i++){
    		//create pid's of pages
    		HeapPageId pid = new HeapPageId(this.tableid,i);
    		//create perm with read/write permission
    		Permissions perm = new Permissions(1);
    		//get page
    		HeapPage p = (HeapPage) buf.getPage(tid, pid, perm);
    		//check if there is free space on the page
    		//if there is update the tuple's recordId and insert it
    		if(p.getNumEmptySlots()>0){
    			//insert method updates recordid
    			p.insertTuple(t);
    			//add page to arraylist
    			modifiedPages.add(p);
    			return modifiedPages;
    		}
    	}
    	//if checked all the pages and they are all full
    	//write empty page to end of file and call method again
    	byte[] empty = HeapPage.createEmptyPageData();
    	RandomAccessFile raf = new RandomAccessFile(this.f, "rw");
    	//set pointer to end of file
    	raf.seek(raf.length());
    	raf.write(empty);
    	raf.close();
    	return insertTuple(tid,t);
    }
    // see DbFile.java for javadocs
    public ArrayList<Page> deleteTuple(TransactionId tid, Tuple t) throws DbException,
            TransactionAbortedException {
    	if(t == null || tid == null){
    		throw new DbException("");
    	}
    	HeapPage p = (HeapPage) Database.getBufferPool().getPage(tid, t.getRecordId().getPageId(), null );
    	p.deleteTuple(t);
    	ArrayList<Page> modifiedPages = new ArrayList<Page>();
    	modifiedPages.add(p);
    	return modifiedPages;
    }
    // see DbFile.java for javadocs
    public DbFileIterator iterator(TransactionId tid) {
    	return new HeapFileIterator(this, tid);
    }
}
class HeapFileIterator implements DbFileIterator {
	private Tuple next = null;
    Iterator<Tuple> it = null;
    int curpgno = 0;
    TransactionId tid;
    HeapFile hf;
    public HeapFileIterator(HeapFile hf, TransactionId tid) {
        this.hf = hf;
        this.tid = tid;
    }
    public void open() throws DbException, TransactionAbortedException {
        curpgno = -1;
    }
	public boolean hasNext() throws DbException, TransactionAbortedException {
        if (next == null) next = readNext();
        return next != null;
    }
    public Tuple next() throws DbException, TransactionAbortedException,
            NoSuchElementException {
        if (next == null) {
            next = readNext();
            if (next == null) throw new NoSuchElementException();
        }
        Tuple result = next;
        next = null;
        return result;
    }
    Tuple readNext() throws TransactionAbortedException, DbException {
        if (it != null && !it.hasNext())
            it = null;
        while (it == null && curpgno < hf.numPages() - 1) {
            curpgno++;
            HeapPageId curpid = new HeapPageId(hf.getId(), curpgno);
            HeapPage curp = (HeapPage) Database.getBufferPool().getPage(tid,
                    curpid, Permissions.READ_ONLY);
            it = curp.iterator();
            if (!it.hasNext())
                it = null;
        }
        if (it == null)
            return null;
        return it.next();
    }
    public void rewind() throws DbException, TransactionAbortedException {
        close();
        open();
    }
    public void close() {
    	next = null;
        it = null;
        curpgno = Integer.MAX_VALUE;
    }
}