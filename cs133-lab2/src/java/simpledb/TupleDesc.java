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
        public String fieldName;

        public TDItem(Type t, String n) {
            this.fieldName = n;
            this.fieldType = t;
        }

        public TDItem(Type type) {
			// TODO Auto-generated constructor stub
        	this.fieldType= type;
        	this.fieldName = null; 
		}

		public String toString() {
            return fieldName + "(" + fieldType + ")";
        }
    }
    
   /*Our fields*/
    public ArrayList<TDItem> tdItems; 
    public Type[] typeAr; 
    public String[] fieldAr;
    /**
     * @return
     *        An iterator which iterates over all the field TDItems
     *        that are included in this TupleDesc
     * */
    public Iterator<TDItem> iterator() {
    	System.out.println("getting td iterator");
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

    	
    	this.typeAr = typeAr;  	
    	this.fieldAr = fieldAr;
    	this.tdItems = new ArrayList<TDItem>();
    	
    	
    	for(int i = 0; i < typeAr.length; i++){
    		
    		this.tdItems.add(new TDItem(typeAr[i], fieldAr[i]));
    	}
    	
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
    	
    	
    	this.tdItems = new ArrayList<TDItem>(typeAr.length );
    	for(int i = 0; i < typeAr.length; i++){

    		TDItem temp = new TDItem(typeAr[i]);
    		this.tdItems.add(temp);
    	}
    }

   

	/**
     * @return the number of fields in this TupleDesc
     */
    public int numFields() {
        // some code goes here
    	if(tdItems == null){
    		return 0;
    	}
    	else{
    		return tdItems.size();
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
        return tdItems.get(i).fieldName;
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
    	return  tdItems.get(i).fieldType;
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
    	int index = 0; 
    	for(TDItem t : tdItems){
    		if(t.fieldName != null){
	    		if(t.fieldName.equals(name)){
	    			return index;
	    		}else{
	    			
	    		}
    		}index++;
    	}
    	throw new NoSuchElementException(); 
    }

    /**
     * @return The size (in bytes) of tuples corresponding to this TupleDesc.
     *         Note that tuples from a given TupleDesc are of a fixed size.
     */
    public int getSize() {
        // some code goes here
    	
    	int counter = 0;
    	for(TDItem t : tdItems){
    		counter += t.fieldType.getLen();
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
    	ArrayList<Type> tups = new ArrayList<Type>();
    	ArrayList<String> fields = new ArrayList<String>();
    	
    	for(TDItem temp : td1.tdItems){
    		tups.add(temp.fieldType);	
    	}
    	for(TDItem temp : td2.tdItems){
    		tups.add(temp.fieldType);
    	}
    	for(TDItem temp : td1.tdItems){
    		fields.add(temp.fieldName);	
    	}
    	for(TDItem temp : td2.tdItems){
    		fields.add(temp.fieldName);
    	}
    	
    	return (new TupleDesc((Type[])tups.toArray(new Type[tups.size()]) , (String[])fields.toArray(new String[fields.size()])));
    	
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
    	if (o == null && this.tdItems == null){
    		return true; 
    	}
    	if (o == null){
    		return false; 
    	}
    	
		if (o instanceof TupleDesc){
			TupleDesc temp = (TupleDesc) o;
	    	if(this.getSize() != temp.getSize()){
	    		return false; 
	    	}
	    	
	    	int index = 0;
	    	for(TDItem t : tdItems){
	    		if(t != null && temp != null){
		    		if(!t.fieldType.equals(temp.getFieldType(index))){
		    			return false; 
		    		}
	    		
	    		}
	    		index++; 
	    	}
	    	return true;
		}
		return false;
		
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
