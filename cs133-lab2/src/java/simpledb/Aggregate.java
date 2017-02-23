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
    private DbIterator it; 
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
    	try {
			while(child.hasNext()){
				Tuple nextTup = child.next();
				//get gbType of tup
				Type gbType = nextTup.getField(gfield).getType();
<<<<<<< HEAD
				//if afield is an intfield aggreate into IntegerAggregator
				if(nextTup.getField(afield).getType() == Type.INT_TYPE){
					this.intAg = new IntegerAggregator(gfield, gbType, afield, aop);
					System.out.println(nextTup.getField(0)+" "+nextTup.getField(1));
=======
				//if afield is an intfield, make IntegerAggregator
				if(nextTup.getField(afield).getType().equals(Type.INT_TYPE)){
					this.intAg = new IntegerAggregator(afield, gbType, afield, aop);
>>>>>>> f354c4c4ea6696d483f85fcecb83ae8083eb63fb
					intAg.mergeTupleIntoGroup(nextTup);
					System.out.println("merged tuple");
					
					
				}
				else if(nextTup.getField(afield).getType().equals(Type.STRING_TYPE)){
					this.sAg = new StringAggregator(afield, gbType, afield, aop);
					sAg.mergeTupleIntoGroup(nextTup);
					
					
				}
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
    	if(intAg != null){
    		System.out.println("making iterator");
    		this.it = intAg.iterator();
    		try {
    			
				while(it.hasNext()){
					System.out.println("in neer");
					Tuple x = it.next();
					System.out.println(x.getField(0) + " " + x.getField(1));
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
    	else{
    		System.out.println("making sAg");
    		this.it = sAg.iterator();
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
    	return null;
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
    	System.out.println("here");
    	while(it.hasNext()){
    		
    		Tuple x = it.next();
    		System.out.println(x.getField(0)+" "+x.getField(1));
    	}
    	if(it.hasNext()){
    		System.out.println("returning tuple");
    		Tuple x = it.next();
    		//System.out.println(x.getField(0)+" "+x.getField(1));
    		return x;
    	}
		return null;
    }

    public void rewind() throws DbException, TransactionAbortedException {
    	
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
	 return it.getTupleDesc();
    }

    public void close() {
    	it.close();
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

