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
    private IntegerAggregator intAg;
    private StringAggregator sAg;
    private TupleIterator it; 
    private ArrayList<Tuple> list; 
    private Type aType;
    private Type gbType;
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
    	this.aType = child.getTupleDesc().getFieldType(afield);
    	if(gfield == -1){
    		this.gbType = null;
    	}
    	else{
    		this.gbType = child.getTupleDesc().getFieldType(gfield);
    	}
    	if(aType == Type.INT_TYPE){
    		this.intAg = new IntegerAggregator(gfield, gbType, afield, aop);
    	}
    	else{
    		this.sAg = new StringAggregator(gfield, gbType, afield, aop);
    	}
    	try {
    		child.open();
			while(child.hasNext()){
				Tuple nextTup = child.next();
				if(this.aType == Type.INT_TYPE){
					System.out.println("Merged: " + nextTup);
					intAg.mergeTupleIntoGroup(nextTup);
				}
				else {
					System.out.println("SMerged: " + nextTup);
					sAg.mergeTupleIntoGroup(nextTup);
				}
			}
			if(this.aType == Type.INT_TYPE){
				this.it = (TupleIterator) intAg.iterator();
			}
			else{
				this.it = (TupleIterator) sAg.iterator();
			}
		} catch (NoSuchElementException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (DbException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (TransactionAbortedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
    /**
     * @return If this aggregate is accompanied by a groupby, return the groupby
     *         field index in the <b>INPUT</b> tuples. If not, return
     *         {@link simpledb.Aggregator#NO_GROUPING}
     * */
    public int groupField() {
    	if(gfield == -1){
    		return Aggregator.NO_GROUPING;
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
    	return it.getTupleDesc().getFieldName(gfield);
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
    	return child.getTupleDesc().getFieldName(0);
    }
    /**
     * @return return the aggregate operator
     * */
    public Aggregator.Op aggregateOp() {
    	return aop;
    }
    public static String nameOfAggregatorOp(Aggregator.Op aop) {
    	return aop.name();
    }
    public void open() throws NoSuchElementException, DbException,
	    TransactionAbortedException {
    	super.open();
    	it.open();
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
    	if(it.hasNext()){
    		Tuple x = it.next();
    		return x;
    	}
		return null;
    }
    public void rewind() throws DbException, TransactionAbortedException {
    	child.rewind();
    	it.rewind();
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
	 return child.getTupleDesc();
    }
    public void close() {
    	super.close();
    	child.close();
    }
    /**
     * See Operator.java for additional notes
     */
    @Override
    public DbIterator[] getChildren() {
    	DbIterator[] its = new DbIterator[2];
        its[0] = this.child;
        return its;
    }
    /**
     * See Operator.java for additional notes
     */
    @Override
    public void setChildren(DbIterator[] children){
    	this.child = children[0];
    }
}

