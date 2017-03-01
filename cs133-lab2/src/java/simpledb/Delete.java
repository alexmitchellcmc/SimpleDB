package simpledb;
import java.io.IOException;
/**
 * The delete operator. Delete reads tuples from its child operator and removes
 * them from the table they belong to.
 */
public class Delete extends Operator {
    private static final long serialVersionUID = 1L;
    private DbIterator child;
    private TransactionId t;
    private boolean firstTime;
    /**
     * Constructor specifying the transaction that this delete belongs to as
     * well as the child to read from.
     * 
     * @param t
     *            The transaction this delete runs in
     * @param child
     *            The child operator from which to read tuples for deletion
     */
    public Delete(TransactionId t, DbIterator child) {
        this.t = t;
        this.child = child;
        this.firstTime = true;
    }
    public TupleDesc getTupleDesc() {
    	Type[] typ = new Type[1];
    	typ[0] = Type.INT_TYPE;
    	return new TupleDesc(typ);
    }
    public void open() throws DbException, TransactionAbortedException {
        super.open();
        child.open();
    }
    public void close() {
        super.close();
        child.close();
    }
    /**
     * You can just close and then open the child
     */
    public void rewind() throws DbException, TransactionAbortedException {
        super.close();
        child.close();
        super.open();
        child.open();
    }
    /**
     * Deletes tuples as they are read from the child operator. Deletes are
     * processed via the buffer pool (which can be accessed via the
     * Database.getBufferPool() method. You can pass along the TransactionId from the constructor.
     * This operator should keep track of whether its fetchNext() method has been called already. 
     * 
     * @return A 1-field tuple containing the number of deleted records (even if there are 0)
     *          or null if called more than once.
     * @see Database#getBufferPool
     * @see BufferPool#deleteTuple
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
    	if(firstTime){
    		firstTime = false;
	    	int numRecords = 0;
	        while(child.hasNext()){
	        	//get tuple and delete it
	        	Tuple next = child.next();
				try {
					Database.getBufferPool().deleteTuple(this.t, next);
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				numRecords++;
			} 
	    	Type[] typ = new Type[1];
	    	typ[0] = Type.INT_TYPE;
	    	TupleDesc td = new TupleDesc(typ);
	    	Tuple tup = new Tuple(td);
	    	IntField field = new IntField(numRecords);
	    	tup.setField(0, field);
	    	return tup;
    	}
    	return null;
    }

    @Override
    public DbIterator[] getChildren() {
    	DbIterator[] its = new DbIterator[1];
        its[0] = this.child;
        return its;
    }

    @Override
    public void setChildren(DbIterator[] children) {
    	this.child = children[0];
    }
}
