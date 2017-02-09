package simpledb;

import java.io.Serializable;
import java.util.*;

/**
 * TupleDesc describes the schema of a tuple.
 */
public class TupleDesc implements Serializable {

    /**
     * A help class to facilitate organizing the information of each field
     * */
	
    public static class TDItem implements Serializable {

        private static final long serialVersionUID = 1L;

        /**
         * The type of the field
         * */
        public final Type fieldType;
        
        /**
         * The name of the field
         * */
        public final String fieldName;

        public TDItem(Type t, String n) {
            this.fieldName = n;
            this.fieldType = t;
        }

        public String toString() {
            return fieldName + "(" + fieldType + ")";
        }
    }
    
   /*Our fields*/
    private ArrayList<TDItem> tdItems; 
    private Type[] typeAr; 
    public String[] fieldAr;
    /**
     * @return
     *        An iterator which iterates over all the field TDItems
     *        that are included in this TupleDesc
     * */
    public Iterator<TDItem> iterator() {
        return tdItems.iterator();
        
    }

    private static final long serialVersionUID = 1L;

    /**                
     * Create a new TupleDesc with typeAr.length fields with fields of the
     * specified types, with associated named fields.
     * 
     * @param typeAr
     *            array specifying the number of and types of fields in this
     *            TupleDesc. It must contain at least one entry.
     * @param fieldAr
     *            array specifying the names of the fields. Note that names may
     *            be null.
     */
    
    
    
    
    public TupleDesc(Type[] typeAr, String[] fieldAr) {
        // some code goes here
    	
    	if(typeAr != null && fieldAr != null){
    		System.out.print(typeAr.length +", "+fieldAr.length );
    	}
    	this.typeAr = typeAr;  	
    	this.fieldAr = fieldAr;
    	
    	//System.out.println(this.fieldAr[0]);
    }

    /**
     * Constructor. Create a new tuple desc with typeAr.length fields with
     * fields of the specified types, with anonymous (unnamed) fields.
     * 
     * @param typeAr
     *            array specifying the number of and types of fields in this
     *            TupleDesc. It must contain at least one entry.
     */
    public TupleDesc(Type[] typeAr) {
        // some code goes here
    	this.typeAr = typeAr;
    	this.fieldAr = new String[typeAr.length];
    }

    /**
     * @return the number of fields in this TupleDesc
     */
    public int numFields() {
        // some code goes here
    	if(fieldAr == null){
    		return 0;
    	}
    	else{
    		return fieldAr.length;
    	}
    }

    /**
     * Gets the (possibly null) field name of the ith field of this TupleDesc.
     * 
     * @param i
     *            index of the field name to return. It must be a valid index.
     * @return the name of the ith field
     * @throws NoSuchElementException
     *             if i is not a valid field reference.
     */
    public String getFieldName(int i) throws NoSuchElementException {
        // some code goes here
        return this.fieldAr[i];
    }

    /**
     * Gets the type of the ith field of this TupleDesc.
     * 
     * @param i
     *            The index of the field to get the type of. It must be a valid
     *            index.
     * @return the type of the ith field
     * @throws NoSuchElementException
     *             if i is not a valid field reference.
     */
    public Type getFieldType(int i) throws NoSuchElementException {
        // some code goes here
    	return typeAr[i];
    }

    /**
     * Find the index of the field with a given name.
     * 
     * @param name
     *            name of the field.
     * @return the index of the field that is first to have the given name.
     * @throws NoSuchElementException
     *             if no field with a matching name is found.
     */
    public int fieldNameToIndex(String name) throws NoSuchElementException {
    	int counter = 0;
        for (String x : fieldAr){
        	if(x == null){
        		counter++; 	
        	}
        	else if (x.equals(name)){
        		return counter;
        	}
        	else{
        		counter++; 	
        	}
        }
        throw new NoSuchElementException("field not in array");
    }

    /**
     * @return The size (in bytes) of tuples corresponding to this TupleDesc.
     *         Note that tuples from a given TupleDesc are of a fixed size.
     */
    public int getSize() {
        // some code goes here
    	int counter = 0;
        for (Type x : typeAr){
        	counter += x.getLen();   	      	 
        }
        return counter;
    }

    /**
     * Merge two TupleDescs into one, with td1.numFields + td2.numFields fields,
     * with the first td1.numFields coming from td1 and the remaining from td2.
     * 
     * @param td1
     *            The TupleDesc with the first fields of the new TupleDesc
     * @param td2
     *            The TupleDesc with the last fields of the TupleDesc
     * @return the new TupleDesc
     */
    public static TupleDesc merge(TupleDesc td1, TupleDesc td2) {
        // some code goes here
    	ArrayList<String> tempFields = new ArrayList<String>();
    	ArrayList<Type> tempTypes = new ArrayList<Type>();
        for(String x : td1.fieldAr){
        	tempFields.add(x);
        }
        for(Type y : td1.typeAr){
        	tempTypes.add(y);
        }
        for(String z : td2.fieldAr){
        	tempFields.add(z);
        }
        for(Type t : td2.typeAr){
        	tempTypes.add(t);
        }
        return new TupleDesc((Type[])tempTypes.toArray(new Type[tempTypes.size()]) , (String[])tempFields.toArray(new String[tempFields.size()]));
    }

    /**
     * Compares the specified object with this TupleDesc for equality. Two
     * TupleDescs are considered equal if they are the same size and if the n-th
     * type in this TupleDesc is equal to the n-th type in td.
     * 
     * @param o
     *            the Object to be compared for equality with this TupleDesc.
     * @return true if the object is equal to this TupleDesc.
     */
    public boolean equals(Object o) {
        // some code goes here
    	//types and size
    	if(o == null || typeAr == null ){
    		return false;
    	}
    	else{
    		if(o instanceof TupleDesc){
    			TupleDesc temp = (TupleDesc) o;
		    	if (this.getSize() == temp.getSize()){
		    		//System.out.println("right size!");
		    		int counter = 0; 
		    		Type tempType;
		    		for (Type x : temp.typeAr){
		    			tempType = this.typeAr[counter];
		    			//System.out.println(tempType.toString());
		    			//System.out.println(x.toString());
		    			if(tempType == null && x != null){
		    				return false; 
		    			}
		    			if(tempType != null && x == null){
		    				return false; 
		    			}
		    			if (!tempType.equals(x)){
		    				//System.out.println("returning false!");
		    				return false;
		    			}
		    			counter++;
		    			
		    		}
		    		//System.out.println("They're equal returning true");
		    		return true; 
		    	}
		    	
		    }else{
	    		//System.out.println("o not tupleDesc returning false");
		        return false;
		    }
    		return false;
    	}
    }

    public int hashCode() {
        // If you want to use TupleDesc as keys for HashMap, implement this so
        // that equal objects have equals hashCode() results
    	
    	//we didn't make hashcodes equal for equal objects
        return this.hashCode();
    }

    /**
     * Returns a String describing this descriptor. It should be of the form
     * "fieldName[0](fieldType[0]), ..., fieldName[M](fieldType[M])", although
     * the exact format does not matter.
     * 
     * @return String describing this descriptor.
     */
    public String toString() {
        // some code goes here
    	String concatonation = "";
    	Type tempType; 
    	int counter = 0; 
    	
    	for (String field : this.fieldAr){
			tempType = this.typeAr[counter];
			concatonation += field;
			concatonation += tempType; 
			concatonation += ","; 
		}
        
    	//cut off last comma
    	StringBuffer buf = new StringBuffer(concatonation.length()-1);
    	
        return buf.toString();
    }
}
