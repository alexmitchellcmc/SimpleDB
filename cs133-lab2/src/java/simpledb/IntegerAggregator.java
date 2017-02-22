package simpledb;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.NoSuchElementException;

import javax.swing.text.html.HTMLDocument.Iterator;

/**
 * Computes some aggregate over a set of IntFields.
 */
public class IntegerAggregator implements Aggregator {
    private static final long serialVersionUID = 1L;
    private int gbfield;
    private Type gbfieldtype;
    private int afield;
    private Op what;
    private Integer aggr;
    private int count;
    
    
    private ArrayList<Tuple> mapping;
   
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
        if(gbfield == -1){
        	aggr = 0; 
        }
        this.mapping = new ArrayList<Tuple>();
        this.afield = afield;
        
        this.what = what;
        this.count = 0; 
    }
    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the
     * constructor. See Aggregator.java for more.
     * 
     * @param tup
     *            the Tuple containing an aggregate field and a group-by field
     */
    
    public void mergeTupleIntoGroup(Tuple tup) {
        // some code goes here
 
    	Integer grouping = Integer.parseInt(tup.getField(0).toString());
    	Integer value = Integer.parseInt(tup.getField(1).toString());
    	int counter = 0; 
    	for(Tuple t : mapping){
    		
    		Integer mapG = Integer.parseInt(t.getField(0).toString());
    		Integer mapV =  Integer.parseInt(t.getField(1).toString());
    		
    		if( mapG == grouping && mapV == value){
    			Integer temp = mapG;
    		
    	
		    	
		    	
		    	if(gbfieldtype != null){
		    		if (what.name().equals("SUM")){
		    			if (temp == null){
		    				Tuple toPut = new Tuple(tup.getTupleDesc());
		    				toPut.setField(mapG, tup.getField(1));
		    	    		mapping.set(counter, toPut);
		    	    	}else{
		    	    		tup.setField(1, new IntField(temp + value));
		    	    		mapping.set(counter, tup);
		    	    	}
		    			
		    		}
		    		else if (what.name().equals("AVG")){
		    			count++;
		    			if (temp == null){
		    				Tuple toPut = new Tuple(tup.getTupleDesc());
		    				toPut.setField(mapG, tup.getField(1));
		    	    		mapping.set(counter, toPut);
		    	    	}
		    			else{
		    				tup.setField(1, new IntField(((temp + value) / count)));
		    	    		mapping.set(counter, tup);
		    	    	}
		    			
		    		}
		    		else if (what.name().equals("MAX")){
		    			if (temp == null){
		    				Tuple toPut = new Tuple(tup.getTupleDesc());
		    				toPut.setField(mapG, tup.getField(1));
		    	    		mapping.set(counter, toPut);
		    	    	}
		    			if(value > temp){
		    				tup.setField(1, new IntField(value));
		    				mapping.set(grouping, tup);
		    			}
		    			
		    		}
		    		else if (what.name().equals("MIN")){
		    			if (temp == null){
		    				Tuple toPut = new Tuple(tup.getTupleDesc());
		    				toPut.setField(mapG, tup.getField(1));
		    	    		mapping.set(counter, toPut);
		    	    	}
		    			if(value < temp){
		    				tup.setField(1, new IntField(value));
		    				mapping.set(counter, tup);
		    			}
		    			
		    		}
		    		else if (what.name().equals("COUNT")){
		    			if (temp == null){
		    				Tuple toPut = new Tuple(tup.getTupleDesc());
		    				toPut.setField(mapG, tup.getField(1));
		    	    		mapping.set(count, toPut);
		    	    	}
		    			else{
		    				tup.setField(1, new IntField(temp+1));
		    				mapping.set(counter, tup);	
		    			}	
		    		}
		    		else{
		    			//do nothing;
		    		}
		    		counter++;
		    	}
		    	
		    	else{
		    		
		    		if (what.name().equals("SUM")){
		    			if (temp == null){
		    	    		this.aggr = value;
		    	    	}else{
		    	    		this.aggr = temp + value; 
		    	    	}
		    			
		    		}
		    		else if (what.name().equals("AVG")){
		    			count++;
		    			if (temp == null){
		    				this.aggr = value;
		    	    	}
		    			else{
		    				this.aggr = (temp + value) / count;
		    	    		
		    	    	}
		    			
		    		}
		    		else if (what.name().equals("MAX")){
		    			if (temp == null){
		    				this.aggr = value;
		    	    	}
		    			if(value > temp){
		    				this.aggr = value;
		    			}
		    			
		    		}
		    		else if (what.name().equals("MIN")){
		    			if (temp == null){
		    				this.aggr = value;
		    	    	}
		    			if(value < temp){
		    				this.aggr = value;
		    			}
		    			
		    		}
		    		else if (what.name().equals("COUNT")){
		    			if (temp == null){
		    				this.aggr = 1; 
		    	    	}
		    			else{
		    				this.aggr+=1;
		    			}	
		    		}
		    		else{
		    			//do nothing;
		    		}
		    	}
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
    public class it implements DbIterator{
    	int counter;
    	
		@Override
		public void open() throws DbException, TransactionAbortedException {
			// TODO Auto-generated method stub
			counter = 0; 
		}

		@Override
		public boolean hasNext() throws DbException,
				TransactionAbortedException {
			// TODO Auto-generated method stub
			if (counter < mapping.size()){
				return true; 
			}
			return false; 
			
		}

		@Override
		public Tuple next() throws DbException, TransactionAbortedException,
				NoSuchElementException {
			// TODO Auto-generated method stub
			counter++;
			if(hasNext()){ 
				return mapping.get(counter-1);
			}
			return null;
		}

		@Override
		public void rewind() throws DbException, TransactionAbortedException {
			// TODO Auto-generated method stub
			counter = 0;
		}

		@Override
		public TupleDesc getTupleDesc() {
			// TODO Auto-generated method stub
			return mapping.get(0).getTupleDesc();
			
		}

		@Override
		public void close() {
			// TODO Auto-generated method stub
			
		}
    	
    }
    public DbIterator iterator() {
        // some code goes here
    	return new it(); 
    }

}
