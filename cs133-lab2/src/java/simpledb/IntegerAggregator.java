package simpledb;
import java.util.*;
/**
 * Computes some aggregate over a set of IntFields.
 */
public class IntegerAggregator implements Aggregator {
    private static final long serialVersionUID = 1L;
    private int gbfield;
    private Type gbfieldtype;
    private int afield;
    private Op what;
    private HashMap<Integer, Tuple> grouping;
    private int countAvg;
    /**
     * Aggregate constructor
     * 
     * @param gbfield
     *            the 0-based index of the group-by field in the tuple, or
     *            NO_GROUPING if there is no grouping
     * @param gbfieldtype
     *            the type of the group by field (e.g., Type.INT_TYPE), or null
     *            if there is no grouping
     * @param afield
     *            the 0-based index of the aggregate field in the tuple
     * @param what
     *            the aggregation operator
     */
    public IntegerAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        this.gbfield = gbfield;
        this.gbfieldtype = gbfieldtype;
        this.afield = afield;
        this.what = what;
        this.grouping = new HashMap<Integer, Tuple>();
        this.countAvg = 0;
    }
    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the
     * constructor. See Aggregator.java for more.
     * 
     * @param tup
     *            the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
    	//get gb value
    	int gbval;
    	if(this.gbfield == -1){
    		gbval = -1;
    	}
    	else{
	    	IntField gbf = (IntField) tup.getField(gbfield);
	    	gbval = gbf.getValue();
    	}
    	//count
    	if (what == Op.COUNT){
    		//if tuple is already there add 1
    		if(grouping.containsKey(gbval)){
    			Tuple countTup = grouping.get(gbval);
    			int curAgVal = ((IntField)countTup.getField(afield)).getValue();
    			countTup.setField(this.afield, new IntField(curAgVal + 1));
    			grouping.put(gbval, countTup);
    		}
    		//if tuple is not there put it there
    		else{
    			IntField one = new IntField(1);
    			tup.setField(afield, one);
    			grouping.put(gbval, tup);
    		}
    	}
    	//max
    	if (what == Op.MAX){
    		//if tuple is already there check if current tuple's afield is larger
    		if(grouping.containsKey(gbval)){
    			Tuple maxTup = grouping.get(gbval);
    			int curAgVal = ((IntField)tup.getField(afield)).getValue();
    			int maxTupAgVal = ((IntField)maxTup.getField(afield)).getValue();
    			IntField max = new IntField(Math.max(curAgVal, maxTupAgVal));
    			maxTup.setField(this.afield, max);
    			grouping.put(gbval, maxTup);
    		}
    		//if tuple is not there put it there
    		else{
    			grouping.put(gbval, tup);
    		}
    	}
    	//min
    	if (what == Op.MIN){
    		//if tuple is already check if current tuple's afield is smaller
    		if(grouping.containsKey(gbval)){
    			Tuple minTup = grouping.get(gbval);
    			int curAgVal = ((IntField)tup.getField(afield)).getValue();
    			int minTupAgVal = ((IntField)minTup.getField(afield)).getValue();
    			IntField min = new IntField(Math.min(curAgVal, minTupAgVal));
    			minTup.setField(this.afield, min);
    			grouping.put(gbval, minTup);
    		}
    		//if tuple is not there put it there
    		else{
    			grouping.put(gbval, tup);
    		}
    	}
    	//sum
    	if (what == Op.SUM){
    		//if tuple is already there add current tuples field to it
    		if(grouping.containsKey(gbval)){
    			Tuple sumTup = grouping.get(gbval);
    			int curAgVal = ((IntField)tup.getField(afield)).getValue();
    			int sumTupAgVal = ((IntField)sumTup.getField(afield)).getValue();
    			IntField sum = new IntField(curAgVal + sumTupAgVal);
    			sumTup.setField(this.afield, sum);
    			grouping.put(gbval, sumTup);
    		}
    		//if tuple is not there put it there
    		else{
    			grouping.put(gbval, tup);
    		}
    	}
    	//avg
    	if(what == Op.AVG){
    		//if tuple is already there calc new avg
    		if(grouping.containsKey(gbval)){
    			Tuple avgTup = grouping.get(gbval);
    			int avgTupAgVal = ((IntField)avgTup.getField(afield)).getValue();
    			int oldSum = avgTupAgVal * this.countAvg;
    			int curAgVal = ((IntField)tup.getField(afield)).getValue();
    			int newSum = oldSum + curAgVal;
    			this.countAvg++;
    			int newAvg = newSum/this.countAvg;
    			IntField avg = new IntField(newAvg);
    			avgTup.setField(this.afield, avg );
    			grouping.put(gbval, avgTup);
    		}
    		//if tuple is not there put it there
    		else{
    			grouping.put(gbval, tup);
    			this.countAvg++;
    		}
    	}
    }
    /**
     * Returns a DbIterator over group aggregate results.
     * 
     * @return a DbIterator whose tuples are the pair (groupVal, aggregateVal)
     *         if using group, or a single (aggregateVal) if no grouping. The
     *         aggregateVal is determined by the type of aggregate specified in
     *         the constructor.
     */
    private class intAgIt implements DbIterator{
    	private Iterator<Tuple> it;
		@Override
		public void open() throws DbException, TransactionAbortedException {
			it = grouping.values().iterator();
		}
		@Override
		public boolean hasNext() throws DbException, TransactionAbortedException {
			// TODO Auto-generated method stub
			return it.hasNext();
		}
		@Override
		public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
			// TODO Auto-generated method stub
			return it.next();
		}
		@Override
		public void rewind() throws DbException, TransactionAbortedException {
			// TODO Auto-generated method stub
			it = grouping.values().iterator();
		}
		@Override
		public TupleDesc getTupleDesc() {
			// TODO Auto-generated method stub
			return grouping.get(gbfield).getTupleDesc();
		}
		@Override
		public void close() {
			// TODO Auto-generated method stub
			it = null;
		}
    }
    public DbIterator iterator() {
        return new intAgIt();
    }

}