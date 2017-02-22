package simpledb;
import java.io.Serializable;
/**
 * JoinPredicate compares fields of two tuples using a predicate. JoinPredicate
 * is most likely used by the Join operator.
 */
public class JoinPredicate implements Serializable {
    private static final long serialVersionUID = 1L;
    private int field1;
    private Predicate.Op op;
    private int field2;
    /**
     * Constructor -- create a new predicate over two fields of two tuples.
     * 
     * @param field1
     *            The field index into the first tuple in the predicate
     * @param field2
     *            The field index into the second tuple in the predicate
     * @param op
     *            The operation to apply (as defined in Predicate.Op); either
     *            Predicate.Op.GREATER_THAN, Predicate.Op.LESS_THAN,
     *            Predicate.Op.EQUAL, Predicate.Op.GREATER_THAN_OR_EQ, or
     *            Predicate.Op.LESS_THAN_OR_EQ
     * @see Predicate
     */
    public JoinPredicate(int field1, Predicate.Op op, int field2) {
    	this.field1 = field1;
    	this.op = op;
    	this.field2 = field2;
    }
    /**
     * Apply the predicate to the two specified tuples. The comparison can be
     * made through Field's compare method.
     * 
     * @return true if the tuples satisfy the predicate.
     */
    public boolean filter(Tuple t1, Tuple t2) {
    	Field t1f1 = t1.getField(getField1());
    	//Field t1f2 = t1.getField(getField2());
    	Field t2f1 = t2.getField(getField1());
    	//Field t2f2 = t2.getField(getField2());
    	return t1f1.compare(getOperator(), t2f1); //&& t1f2.compare(getOperator(), t2f2);
    }
    public int getField1(){
        return this.field1;
    }
    public int getField2(){
        return this.field2;
    }
    public Predicate.Op getOperator(){
        return this.op;
    }
}