package simpledb;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.concurrent.ConcurrentHashMap;

/**
 * TableStats represents statistics (e.g., histograms) about base tables in a
 * query. 
 * 
 * This class is not needed in implementing lab1 and lab2.
 */
public class TableStats {
	private HashMap<Integer, Integer> min;
	private HashMap<Integer, Integer> max;
	private ArrayList<Integer> stringCols;
	private HashMap<Integer, IntHistogram> intHists;
	private HashMap<Integer, StringHistogram> stringHists;
    private static final ConcurrentHashMap<String, TableStats> statsMap = new ConcurrentHashMap<String, TableStats>();

    static final int IOCOSTPERPAGE = 1000;

    public static TableStats getTableStats(String tablename) {
        return statsMap.get(tablename);
    }

    public static void setTableStats(String tablename, TableStats stats) {
        statsMap.put(tablename, stats);
    }
    
    public static void setStatsMap(HashMap<String,TableStats> s)
    {
        try {
            java.lang.reflect.Field statsMapF = TableStats.class.getDeclaredField("statsMap");
            statsMapF.setAccessible(true);
            statsMapF.set(null, s);
        } catch (NoSuchFieldException e) {
            e.printStackTrace();
        } catch (SecurityException e) {
            e.printStackTrace();
        } catch (IllegalArgumentException e) {
            e.printStackTrace();
        } catch (IllegalAccessException e) {
            e.printStackTrace();
        }

    }

    public static Map<String, TableStats> getStatsMap() {
        return statsMap;
    }

    public static void computeStatistics() {
        Iterator<Integer> tableIt = Database.getCatalog().tableIdIterator();

        System.out.println("Computing table stats.");
        while (tableIt.hasNext()) {
            int tableid = tableIt.next();
            TableStats s;
			try {
				s = new TableStats(tableid, IOCOSTPERPAGE);
				setTableStats(Database.getCatalog().getTableName(tableid), s);
			} catch (NoSuchElementException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
        }
        System.out.println("Done.");
    }

    /**
     * Number of bins for the histogram. Feel free to increase this value over
     * 100, though our tests assume that you have at least 100 bins in your
     * histograms.
     */
    static final int NUM_HIST_BINS = 100;

    /**
     * Create a new TableStats object, that keeps track of statistics on each
     * column of a table
     * 
     * @param tableid
     *            The table over which to compute statistics
     * @param ioCostPerPage
     *            The cost per page of IO. This doesn't differentiate between
     *            sequential-scan IO and disk seeks.
     * @throws TransactionAbortedException 
     * @throws NoSuchElementException 
     */
    public TableStats(int tableid, int ioCostPerPage){
        // For this function, you'll have to get the
        // DbFile for the table in question,
        // then scan through its tuples and calculate
        // the values that you need.
        // You should try to do this reasonably efficiently, but you don't
        // necessarily have to (for example) do everything
        // in a single scan of the table.
	// See project description for hint on using a Transaction
	
        // some code goes here
    	this.min = new HashMap<Integer, Integer>();
    	this.max = new HashMap<Integer, Integer>();
    	this.stringCols = new ArrayList<Integer>();
    	this.intHists = new HashMap<Integer, IntHistogram>();
    	this.stringHists = new HashMap<Integer, StringHistogram>();
    	Transaction t = new Transaction(); 
    	t.start(); 
    	SeqScan s = new SeqScan(t.getId(), tableid, "t"); 
    	// do stuff with s 
    	try {
    		s.open();
			while(s.hasNext()){
				//get Tuple
				Tuple tup = s.next();
				Iterator<Field> i = tup.fields();
				//column number
				int col = 0;
				while(i.hasNext()){
					//get field in tuple
					Field fd = i.next();
					//if field is intfield
					if(fd.getType() == Type.INT_TYPE){
						IntField ifd = (IntField) fd;
						int fval = ifd.getValue();
						Integer curMin = min.get(col);
						Integer curMax = max.get(col);
						if(curMin == null || curMin > fval){
							min.put(col, fval);
						}
						if(curMax == null || curMax < fval){
							max.put(col, fval);
						}
					}
					//if field is stringfield
					else{
						stringCols.add(col);
					}
					col++;
				}
			}
	    	//create inthistograms
	    	for(Integer col: min.keySet()){
	    		intHists.put(col, new IntHistogram(NUM_HIST_BINS,min.get(col), max.get(col)));
	    	}
	    	//create stringhistograms
	    	for(Integer col: stringCols){
	    		stringHists.put(col, new StringHistogram(NUM_HIST_BINS));
	    	}
	  	
	    	s.rewind();
	    	
		 catch (DbException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		} catch (NoSuchElementException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (TransactionAbortedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
    	try {
			
		} catch (DbException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		} catch (NoSuchElementException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (TransactionAbortedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
    	try {
			while(s.hasNext()){
				//get Tuple
				Tuple tup = s.next();
				Iterator<Field> i = tup.fields();
				//column number
				int col = 0;
				while(i.hasNext()){
					//get field in tuple
					Field fd = i.next();
					//if field is intfield
					if(fd.getType() == Type.INT_TYPE){
						IntField ifd = (IntField) fd;
						int fval = ifd.getValue();
						IntHistogram iHist = this.intHists.get(col);
						iHist.addValue(fval);
						this.intHists.put(col, iHist);
					}
					//if field is stringfield
					else{
						StringField sfd = (StringField) fd;
						String fval = sfd.getValue();
						StringHistogram sHist = this.stringHists.get(col);
						sHist.addValue(fval);
						this.stringHists.put(col, sHist);
					}
					col++;
				}
			}
		} catch (DbException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		} catch (NoSuchElementException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (TransactionAbortedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
    	try {
			t.commit();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
    }

    /**
     * Estimates the cost of sequentially scanning the file, given that the cost
     * to read a page is costPerPageIO. You can assume that there are no seeks
     * and that no pages are in the buffer pool.
     * 
     * Also, assume that your hard drive can only read entire pages at once, so
     * if the last page of the table only has one tuple on it, it's just as
     * expensive to read as a full page. (Most real hard drives can't
     * efficiently address regions smaller than a page at a time.)
     * 
     * @return The estimated cost of scanning the table.
     */
    public double estimateScanCost() {
        // some code goes here
        return 0;
    }

    /**
     * This method returns the number of tuples in the relation, given that a
     * predicate with selectivity selectivityFactor is applied.
     * 
     * @param selectivityFactor
     *            The selectivity of any predicates over the table
     * @return The estimated cardinality of the scan with the specified
     *         selectivityFactor
     */
    public int estimateTableCardinality(double selectivityFactor) {
        // some code goes here
        return 0;
    }

    /**
     * The average selectivity of the field under op.
     * @param field
     *        the index of the field
     * @param op
     *        the operator in the predicate
     * The semantic of the method is that, given the table, and then given a
     * tuple, of which we do not know the value of the field, return the
     * expected selectivity. You may estimate this value from the histograms.
     *
     * Not necessary for lab 3
     * */
    public double avgSelectivity(int field, Predicate.Op op) {
        return 0.5;
    }

    /**
     * Estimate the selectivity of predicate <tt>field op constant</tt> on the
     * table.
     * 
     * @param field
     *            The field over which the predicate ranges
     * @param op
     *            The logical operation in the predicate
     * @param constant
     *            The value against which the field is compared
     * @return The estimated selectivity (fraction of tuples that satisfy) the
     *         predicate
     */
    public double estimateSelectivity(int field, Predicate.Op op, Field constant) {
    	if(constant.getType() == Type.INT_TYPE){
    		IntHistogram iHist = this.intHists.get(field);
    		IntField ifd = (IntField) constant;
    		return iHist.estimateSelectivity(op, ifd.getValue());
    	}
    	else{
    		StringHistogram sHist = this.stringHists.get(field);
    		StringField sfd = (StringField) constant;
    		return sHist.estimateSelectivity(op, sfd.getValue());
    	}
    }

    /**
     * return the total number of tuples in this table
     * */
    public int totalTuples() {
        // some code goes here
        return 0;
    }

}
