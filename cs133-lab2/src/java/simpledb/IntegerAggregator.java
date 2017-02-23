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
    private boolean grouped;
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
        if(gbfield == NO_GROUPING){
        	this.grouped = false;
        }
        else{
        	this.grouped = true;
        }
    }
    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the
     * constructor. See Aggregator.java for more.
     * 
     * @param tup
     *            the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
    	if(grouped){
    		mergeGrouped(tup);
    	}
    	else{
    		notGrouped(tup);
    	}
    }
    //helper to for grouped
    public void mergeGrouped(Tuple tup){
    	IntField gbf = (IntField) tup.getField(gbfield);
    	int gbval = gbf.getValue();
    	//count
    	if (what == Op.COUNT){
    		//if tuple is already there add 1
    		if(grouping.containsKey(gbval)){
    			Tuple countTup = grouping.get(gbval);
    			int curAgVal = ((IntField)countTup.getField(1)).getValue();
    			countTup.setField(1, new IntField(curAgVal + 1));
    			grouping.put(gbval, countTup);
    		}
    		//if tuple is not there put it there
    		else{
    			String gbfName = tup.getTupleDesc().getFieldName(gbfield);
    			String afName = tup.getTupleDesc().getFieldName(afield);
    			IntField one = new IntField(1);
    			Type[] typ = new Type[2];
    			String [] f = new String[2];
    			typ[0] = Type.INT_TYPE;
    			typ[1] = Type.INT_TYPE;
    			f[0] = gbfName;
    			f[1] = afName;
    			TupleDesc td = new TupleDesc(typ, f);
    			Tuple t = new Tuple(td);
    			t.setField(1, one);
    			t.setField(0, tup.getField(gbfield));
    			grouping.put(gbval, t);
    		}
    	}
    	//max
    	if (what == Op.MAX){
    		//if tuple is already there check if current tuple's afield is larger
    		if(grouping.containsKey(gbval)){
    			Tuple maxTup = grouping.get(gbval);
    			int curAgVal = ((IntField)tup.getField(afield)).getValue();
    			int maxTupAgVal = ((IntField)maxTup.getField(1)).getValue();
    			IntField max = new IntField(Math.max(curAgVal, maxTupAgVal));
    			maxTup.setField(1, max);
    			grouping.put(gbval, maxTup);
    		}
    		//if tuple is not there put it there
    		else{
    			String gbfName = tup.getTupleDesc().getFieldName(gbfield);
    			String afName = tup.getTupleDesc().getFieldName(afield);
    			Type[] typ = new Type[2];
    			String [] f = new String[2];
    			typ[0] = Type.INT_TYPE;
    			typ[1] = Type.INT_TYPE;
    			f[0] = gbfName;
    			f[1] = afName;
    			TupleDesc td = new TupleDesc(typ, f);
    			Tuple t = new Tuple(td);
    			t.setField(1, tup.getField(afield));
    			t.setField(0, tup.getField(gbfield));
    			grouping.put(gbval, t);
    		}
    	}
    	//min
    	if (what == Op.MIN){
    		//if tuple is already check if current tuple's afield is smaller
    		if(grouping.containsKey(gbval)){
    			Tuple minTup = grouping.get(gbval);
    			int curAgVal = ((IntField)tup.getField(afield)).getValue();
    			int minTupAgVal = ((IntField)minTup.getField(1)).getValue();
    			IntField min = new IntField(Math.min(curAgVal, minTupAgVal));
    			minTup.setField(1, min);
    			grouping.put(gbval, minTup);
    		}
    		//if tuple is not there put it there
    		else{
    			String gbfName = tup.getTupleDesc().getFieldName(gbfield);
    			String afName = tup.getTupleDesc().getFieldName(afield);
    			Type[] typ = new Type[2];
    			String [] f = new String[2];
    			typ[0] = Type.INT_TYPE;
    			typ[1] = Type.INT_TYPE;
    			f[0] = gbfName;
    			f[1] = afName;
    			TupleDesc td = new TupleDesc(typ, f);
    			Tuple t = new Tuple(td);
    			t.setField(1, tup.getField(afield));
    			t.setField(0, tup.getField(gbfield));
    			grouping.put(gbval, t);
    		}
    	}
    	//sum
    	if (what == Op.SUM){
    		//if tuple is already there add current tuples field to it
    		if(grouping.containsKey(gbval)){
    			Tuple sumTup = grouping.get(gbval);
    			int curAgVal = ((IntField)tup.getField(afield)).getValue();
    			int sumTupAgVal = ((IntField)sumTup.getField(1)).getValue();
    			IntField sum = new IntField(curAgVal + sumTupAgVal);
    			sumTup.setField(1, sum);
    			grouping.put(gbval, sumTup);
    		}
    		//if tuple is not there put it there
    		else{
    			String gbfName = tup.getTupleDesc().getFieldName(gbfield);
    			String afName = tup.getTupleDesc().getFieldName(afield);
    			Type[] typ = new Type[2];
    			String [] f = new String[2];
    			typ[0] = Type.INT_TYPE;
    			typ[1] = Type.INT_TYPE;
    			f[0] = gbfName;
    			f[1] = afName;
    			TupleDesc td = new TupleDesc(typ, f);
    			Tuple t = new Tuple(td);
    			t.setField(1, tup.getField(afield));
    			t.setField(0, tup.getField(gbfield));
    			grouping.put(gbval, t);
    		}
    	}
    	//avg
    	if(what == Op.AVG){
    		//if tuple is already there calc new avg
    		if(grouping.containsKey(gbval)){
    			Tuple avgTup = grouping.get(gbval);
    			int avgTupAgVal = ((IntField)avgTup.getField(1)).getValue();
    			int oldSum = avgTupAgVal * this.countAvg;
    			int curAgVal = ((IntField)tup.getField(afield)).getValue();
    			int newSum = oldSum + curAgVal;
    			this.countAvg++;
    			int newAvg = newSum/this.countAvg;
    			IntField avg = new IntField(newAvg);
    			avgTup.setField(1, avg );
    			grouping.put(gbval, avgTup);
    		}
    		//if tuple is not there put it there
    		else{
    			String gbfName = tup.getTupleDesc().getFieldName(gbfield);
    			String afName = tup.getTupleDesc().getFieldName(afield);
    			Type[] typ = new Type[2];
    			String [] f = new String[2];
    			typ[0] = Type.INT_TYPE;
    			typ[1] = Type.INT_TYPE;
    			f[0] = gbfName;
    			f[1] = afName;
    			TupleDesc td = new TupleDesc(typ, f);
    			Tuple t = new Tuple(td);
    			t.setField(1, tup.getField(afield));
    			t.setField(0, tup.getField(gbfield));
    			grouping.put(gbval, t);
    			this.countAvg++;
    		}
    	}
    }
    //helper for when not grouped
    public void notGrouped(Tuple tup){
    	int gbval = NO_GROUPING;
    	//count
    	if (what == Op.COUNT){
    		//if tuple is already there add 1
    		if(grouping.containsKey(gbval)){
    			Tuple countTup = grouping.get(gbval);
    			int curAgVal = ((IntField)countTup.getField(0)).getValue();
    			countTup.setField(0, new IntField(curAgVal + 1));
    			grouping.put(gbval, countTup);
    		}
    		//if tuple is not there put it there
    		else{
    			String afName = tup.getTupleDesc().getFieldName(afield);
    			IntField one = new IntField(1);
    			Type[] typ = new Type[1];
    			String [] f = new String[1];
    			typ[0] = Type.INT_TYPE;
    			f[0] = afName;
    			TupleDesc td = new TupleDesc(typ, f);
    			Tuple t = new Tuple(td);
    			t.setField(0, one);
    			grouping.put(gbval, t);
    		}
    	}
    	//max
    	if (what == Op.MAX){
    		//if tuple is already there check if current tuple's afield is larger
    		if(grouping.containsKey(gbval)){
    			Tuple maxTup = grouping.get(gbval);
    			int curAgVal = ((IntField)tup.getField(afield)).getValue();
    			int maxTupAgVal = ((IntField)maxTup.getField(0)).getValue();
    			IntField max = new IntField(Math.max(curAgVal, maxTupAgVal));
    			maxTup.setField(0, max);
    			grouping.put(gbval, maxTup);
    		}
    		//if tuple is not there put it there
    		else{
    			String afName = tup.getTupleDesc().getFieldName(afield);
    			Type[] typ = new Type[1];
    			String [] f = new String[1];
    			typ[0] = Type.INT_TYPE;
    			f[0] = afName;
    			TupleDesc td = new TupleDesc(typ, f);
    			Tuple t = new Tuple(td);
    			t.setField(0, tup.getField(afield));
    			grouping.put(gbval, t);
    		}
    	}
    	//min
    	if (what == Op.MIN){
    		//if tuple is already check if current tuple's afield is smaller
    		if(grouping.containsKey(gbval)){
    			Tuple minTup = grouping.get(gbval);
    			int curAgVal = ((IntField)tup.getField(afield)).getValue();
    			int minTupAgVal = ((IntField)minTup.getField(0)).getValue();
    			IntField min = new IntField(Math.min(curAgVal, minTupAgVal));
    			minTup.setField(0, min);
    			grouping.put(gbval, minTup);
    		}
    		//if tuple is not there put it there
    		else{
    			String afName = tup.getTupleDesc().getFieldName(afield);
    			Type[] typ = new Type[1];
    			String [] f = new String[1];
    			typ[0] = Type.INT_TYPE;
    			f[0] = afName;
    			TupleDesc td = new TupleDesc(typ, f);
    			Tuple t = new Tuple(td);
    			t.setField(0, tup.getField(afield));
    			grouping.put(gbval, t);
    		}
    	}
    	//sum
    	if (what == Op.SUM){
    		//if tuple is already there add current tuples field to it
    		if(grouping.containsKey(gbval)){
    			Tuple sumTup = grouping.get(gbval);
    			int curAgVal = ((IntField)tup.getField(afield)).getValue();
    			int sumTupAgVal = ((IntField)sumTup.getField(0)).getValue();
    			IntField sum = new IntField(curAgVal + sumTupAgVal);
    			sumTup.setField(0, sum);
    			grouping.put(gbval, sumTup);
    		}
    		//if tuple is not there put it there
    		else{
    			String afName = tup.getTupleDesc().getFieldName(afield);
    			Type[] typ = new Type[1];
    			String [] f = new String[1];
    			typ[0] = Type.INT_TYPE;
    			f[0] = afName;
    			TupleDesc td = new TupleDesc(typ, f);
    			Tuple t = new Tuple(td);
    			t.setField(0, tup.getField(afield));
    			grouping.put(gbval, t);
    		}
    	}
    	//avg
    	if(what == Op.AVG){
    		//if tuple is already there calc new avg
    		if(grouping.containsKey(gbval)){
    			Tuple avgTup = grouping.get(gbval);
    			int avgTupAgVal = ((IntField)avgTup.getField(0)).getValue();
    			int oldSum = avgTupAgVal * this.countAvg;
    			int curAgVal = ((IntField)tup.getField(afield)).getValue();
    			int newSum = oldSum + curAgVal;
    			this.countAvg++;
    			int newAvg = newSum/this.countAvg;
    			IntField avg = new IntField(newAvg);
    			avgTup.setField(0, avg );
    			grouping.put(gbval, avgTup);
    		}
    		//if tuple is not there put it there
    		else{
    			String afName = tup.getTupleDesc().getFieldName(afield);
    			Type[] typ = new Type[1];
    			String [] f = new String[1];
    			typ[0] = Type.INT_TYPE;
    			f[0] = afName;
    			TupleDesc td = new TupleDesc(typ, f);
    			Tuple t = new Tuple(td);
    			t.setField(0, tup.getField(afield));
    			grouping.put(gbval, t);
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
    	/**
		 * 
		 */
		
		private Iterator<Tuple> it = grouping.values().iterator();
		
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