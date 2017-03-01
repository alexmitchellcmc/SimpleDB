package simpledb;

import java.io.IOException;

/**
 * Inserts tuples read from the child operator into the tableid specified in the
 * constructor
 */
public class Insert extends Operator {

    private static final long serialVersionUID = 1L;
    private DbIterator child;
    private TransactionId t;
    private int tableid;
    private boolean firstTime;
    /**
     * Constructor.
     * 
     * @param t
     *            The transaction running the insert.
     * @param child
     *            The child operator from which to read tuples to be inserted.
     * @param tableid
     *            The table in which to insert tuples.
     * @throws DbException
     *             if TupleDesc of child differs from table into which we are to
     *             insert.
     */
    public Insert(TransactionId t,DbIterator child, int tableid) throws DbException{
    	if(!Database.getCatalog().getDatabaseFile(tableid).getTupleDesc().equals(child.getTupleDesc())){
    		throw new DbException("child differs from table into which we are to insert");
    	}
    	this.t = t;
    	this.child = child;
    	this.tableid = tableid;
    	this.firstTime = true;
    }

    public TupleDesc getTupleDesc() {
    	Type[] typ = new Type[1];
    	typ[0] = Type.INT_TYPE;
    	TupleDesc td = new TupleDesc(typ);
        return td;
    }

    public void open() throws DbException, TransactionAbortedException {
        child.open();
        super.open();
    }

    public void close() {
        child.close();
        super.close();
    }

    /**
     * You can just close and then open the child
     */
    public void rewind() throws DbException, TransactionAbortedException {
        child.close();
        super.close();
        child.open();
        super.open();
    }

    /**
     * Inserts tuples read from child into the relation with the tableid specified by the
     * constructor. It returns a one field tuple containing the number of
     * inserted records (even if there are 0!). 
     * Insertions should be passed through BufferPool.insertTuple() with the 
     * TransactionId from the constructor. An instance of BufferPool is available via 
     * Database.getBufferPool(). Note that insert DOES NOT need to check to see if 
     * a particular tuple is a duplicate before inserting it.
     *
     * This operator should keep track if its fetchNext() has already been called, 
     * returning null if called multiple times.
     * 
     * @return A 1-field tuple containing the number of inserted records, or
     *         null if called more than once.
     * @see Database#getBufferPool
     * @see BufferPool#insertTuple
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        if(firstTime){
        	firstTime = false;
        	int numRecords = 0;
        	while(child.hasNext()){
        		Tuple next = child.next();
        		try {
					Database.getBufferPool().insertTuple(t, tableid, next);
					numRecords++;
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
        	}
        	Type[] typ = new Type[1];
        	typ[0] = Type.INT_TYPE;
        	TupleDesc td = new TupleDesc(typ);
        	Tuple tup = new Tuple(td);
        	IntField field = new IntField(numRecords);
        	tup.setField(0, field);
        	return tup;
        }
        else{
        	return null;
        }
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
