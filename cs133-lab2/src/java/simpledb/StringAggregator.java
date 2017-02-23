package simpledb;
import java.util.HashMap;
import java.util.Iterator;
import java.util.NoSuchElementException;
/**
 * Computes some aggregate over a set of StringFields.
 */
public class StringAggregator implements Aggregator {
    private static final long serialVersionUID = 1L;
    private int gbfield;
    private Type gbfieldtype;
    private int afield;
    private Op what;
    private HashMap<Integer, Tuple> grouping;
    /**
     * Aggregate constructor
     * @param gbfield the 0-based index of the group-by field in the tuple, or NO_GROUPING if there is no grouping
     * @param gbfieldtype the type of the group by field (e.g., Type.INT_TYPE), or null if there is no grouping
     * @param afield the 0-based index of the aggregate field in the tuple
     * @param what aggregation operator to use -- only supports COUNT
     * @throws IllegalArgumentException if what != COUNT
     */
    public StringAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
    	this.gbfield = gbfield;
        this.gbfieldtype = gbfieldtype;
        this.afield = afield;
        this.grouping = new HashMap<Integer, Tuple>();
        if(what != Op.COUNT){
        	throw new IllegalArgumentException("Invalid operator");
        }
        else{
        	this.what = what;
        }
    }
    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the constructor
     * @param tup the Tuple containing an aggregate field and a group-by field
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
    	//if tuple is already there add 1
		if(grouping.containsKey(gbval)){
			Tuple countTup = grouping.get(gbval);
			int curAgVal = ((IntField)countTup.getField(afield)).getValue();
			countTup.setField(this.afield, new IntField(curAgVal + 1));
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

    /**
     * Returns a DbIterator over group aggregate results.
     *
     * @return a DbIterator whose tuples are the pair (groupVal,
     *   aggregateVal) if using group, or a single (aggregateVal) if no
     *   grouping. The aggregateVal is determined by the type of
     *   aggregate specified in the constructor.
     */
    private class StringAgIt implements DbIterator{
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
        return new StringAgIt();
    }
}