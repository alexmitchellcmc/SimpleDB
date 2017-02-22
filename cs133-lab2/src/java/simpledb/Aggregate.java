package simpledb;
import java.util.*;
/**
 * The Aggregation operator that computes an aggregate (e.g., sum, avg, max,
 * min). Note that we only support aggregates over a single column, grouped by a
 * single column.
 */
public class Aggregate extends Operator {
    private static final long serialVersionUID = 1L;
    private DbIterator child;
    private int afield;
    private int gfield;
    private Aggregator.Op aop;
    /**
     * Constructor.
     * 
     * Implementation hint: depending on the type of afield, you will want to
     * construct an {@link IntAggregator} or {@link StringAggregator} to help
     * you with your implementation of fetchNext().
     * 
     * 
     * @param child
     *            The DbIterator that is feeding us tuples.
     * @param afield
     *            The column over which we are computing an aggregate.
     * @param gfield
     *            The column over which we are grouping the result, or -1 if
     *            there is no grouping
     * @param aop
     *            The aggregation operator to use
     */
    public Aggregate(DbIterator child, int afield, int gfield, Aggregator.Op aop) {
    	this.child = child;
    	this.afield = afield;
    	this.gfield = gfield;
    	this.aop = aop;
    }
    /**
     * @return If this aggregate is accompanied by a groupby, return the groupby
     *         field index in the <b>INPUT</b> tuples. If not, return
     *         {@link simpledb.Aggregator#NO_GROUPING}
     * */
    public int groupField() {
    	if(gfield == -1){
    		return simpledb.Aggregator.NO_GROUPING;
    	}
    	return gfield;
    }
    /**
     * @return If this aggregate is accompanied by a group by, return the name
     *         of the groupby field in the <b>OUTPUT</b> tuples If not, return
     *         null;
     * */
    public String groupFieldName() {
    	if(gfield == -1){
    		return null;
    	}
    	return child.getTupleDesc().getFieldName(gfield);
    }
    /**
     * @return the aggregate field
     * */
    public int aggregateField() {
    	return this.afield;
    }
    /**
     * @return return the name of the aggregate field in the <b>OUTPUT</b>
     *         tuples
     * */
    public String aggregateFieldName() {
	return child.getTupleDesc().getFieldName(afield);
    }
    /**
     * @return return the aggregate operator
     * */
    public Aggregator.Op aggregateOp() {
	return aop;
    }
    public static String nameOfAggregatorOp(Aggregator.Op aop) {
	return aop.toString();
    }
    public void open() throws NoSuchElementException, DbException,
	    TransactionAbortedException {
    	child.open();
    }
    /**
     * Returns the next tuple. If there is a group by field, then the first
     * field is the field by which we are grouping, and the second field is the
     * result of computing the aggregate, If there is no group by field, then
     * the result tuple should contain one field representing the result of the
     * aggregate. Should return null if there are no more tuples.
     *
     * Hint: think about how many tuples you have to process from the child operator
     * before this method can return its first tuple.
     * Hint: notice that you each Aggregator class has an iterator() method
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
    	//if it is the first time calling fetchNext set nextChild to child1.next()
    	// needs to be stored outside fetchNext() so it doesn't get overwritten every time you call the method
    	if(this.firstTime){
    		this.nextChild = child1.next();
    		this.firstTime =false;
    	}
    	//join each tuple in child1 to each tuple in child2
        while (true){ 
        	//if there are no more tuples in child2
        	if(!child2.hasNext()){
        		//check if there are no more tuples in child1
        		//if true break the loop because all joins have been made
        		if(!child1.hasNext()){
        			break;
        		}
        		//if child does have more tuples set nextChild to child1.next() and rewind child2
        		else{
	        		this.nextChild = child1.next();
	            	child2.rewind();
        		}
        	}
        	//iterate through child2's tuples and join them with nextChild tuple from child1
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
    }

    public void rewind() throws DbException, TransactionAbortedException {
    	child.rewind();
    }
    /**
     * Returns the TupleDesc of this Aggregate. If there is no group by field,
     * this will have one field - the aggregate column. If there is a group by
     * field, the first field will be the group by field, and the second will be
     * the aggregate value column.
     * 
     * The name of an aggregate column should be informative. For example:
     * "aggName(aop) (child_td.getFieldName(afield))" where aop and afield are
     * given in the constructor, and child_td is the TupleDesc of the child
     * iterator.
     */
    public TupleDesc getTupleDesc() {
	// some code goes here
	return null;
    }

    public void close() {
    	child.close();
    }

    /**
     * See Operator.java for additional notes
     */
    @Override
    public DbIterator[] getChildren() {
	// some code goes here
	return null;
    }

    /**
     * See Operator.java for additional notes
     */
    @Override
    public void setChildren(DbIterator[] children) {
	// some code goes here
    }
    
}
