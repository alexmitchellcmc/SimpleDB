package simpledb;

import java.util.*;
/**
 * The Join operator implements the relational join operation.
 */
public class Join extends Operator {
    private static final long serialVersionUID = 1L;
    private DbIterator child1;
    private boolean firstTime;
    private Tuple nextChild;
    private DbIterator child2;
    private JoinPredicate p; 
    private TupleDesc joinedTupleDesc;
    /**
     * Constructor. Accepts to children to join and the predicate to join them
     * on
     * 
     * @param p
     *            The predicate to use to join the children
     * @param child1
     *            Iterator for the left(outer) relation to join
     * @param child2
     *            Iterator for the right(inner) relation to join
     */
    public Join(JoinPredicate p, DbIterator child1, DbIterator child2) {
        this.p = p;
        this.child1 = child1;
        this.firstTime = true;
        this.child2 = child2;
        joinedTupleDesc = TupleDesc.merge(child1.getTupleDesc(), child2.getTupleDesc());
    }

    public JoinPredicate getJoinPredicate() {
        return p; 
    }
    /**
     * @return
     *       the field name of join field1. Should be quantified by
     *       alias or table name. Can be taken from the appropriate child's TupleDesc.
     * */
    public String getJoinField1Name() {
        //return joinedTupleDesc.getFieldName(0);
    	return child1.getTupleDesc().getFieldName(p.getField1());
    }
    /**
     * @return
     *       the field name of join field2. Should be quantified by
     *       alias or table name. Can be taken from the appropriate child's TupleDesc.
     * */
    public String getJoinField2Name() {
        // some code goes here
    	//return joinedTupleDesc.getFieldName(1);
    	return child2.getTupleDesc().getFieldName(p.getField2());
    }
    /**
     * Should return a TupleDesc that represents the schema for the joined tuples. 
     *@see simpledb.TupleDesc#merge(TupleDesc, TupleDesc) for possible
     *      implementation logic.
     */
    public TupleDesc getTupleDesc() {
        // some code goes here
        return joinedTupleDesc;
    }
    public void open() throws DbException, NoSuchElementException,
            TransactionAbortedException {
        // some code goes here
    	child1.open();
    	child2.open();
        super.open();
    }
    public void close() {
        // some code goes here
    	child1.close();
    	child2.close();
        super.close();
    }

    public void rewind() throws DbException, TransactionAbortedException {
        // some code goes here
    	child1.rewind();
    	child2.rewind();
    	
    }
    /**
     * Returns the next tuple generated by the join, or null if there are no
     * more tuples. Logically, this is the next tuple in r1 cross r2 that
     * satisfies the join predicate. There are many possible implementations;
     * the simplest is a nested loops join.
     * <p>
     * Note that the tuples returned from this particular implementation of Join
     * are simply the concatenation of joining tuples from the left and right
     * relation. Therefore, if an equality predicate is used there will be two
     * copies of the join attribute in the results. (Removing such duplicate
     * columns can be done with an additional projection operator later on if needed.)
     * <p>
     * For example, if one tuple is {1,2,3} and the other tuple is {1,5,6},
     * joined on equality of the first column, then this returns {1,2,3,1,5,6}.
     * 
     * @return The next matching tuple.
     * @see JoinPredicate#filter
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
    	if(this.firstTime){
    		this.nextChild = child1.next();
    		this.firstTime =false;
    	}
        while (child1.hasNext()){ 
        	if(!child2.hasNext()){
        		this.nextChild = child1.next();
            	child2.rewind();
        	}
        	while (child2.hasNext()){
        		Tuple c2Tuple = child2.next();
        		if(p.filter(this.nextChild, c2Tuple)){
	        		int counter = 0;
	        		Tuple concat = new Tuple(this.joinedTupleDesc);
	        		Iterator<Field> t1it = this.nextChild.fields();
	        		Iterator<Field> t2it = c2Tuple.fields();
	        		while(t1it.hasNext()){
	        			concat.setField(counter, t1it.next());
	        			counter++;
	        		}
	        		while(t2it.hasNext()){
	        			concat.setField(counter, t2it.next());
	        			counter++;
	        		}
	            	return concat;
        		}
        	}
        	
        }
        return null; 
    }
    /**
     * See Operator.java for additional notes
     */
    @Override
    public DbIterator[] getChildren() {
        DbIterator[] its = new DbIterator[2];
        its[0] = this.child1;
        its[1] = this.child2;
        return its;
    }
    /**
     * See Operator.java for additional notes
     */
    @Override
    public void setChildren(DbIterator[] children) {
        this.child1 = children[0];
        this.child2 = children[1];
    }
}
