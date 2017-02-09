package simpledb;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;

/**
 * Tuple maintains information about the contents of a tuple. Tuples have a
 * specified schema specified by a TupleDesc object and contain Field objects
 * with the data for each field.
 */
public class Tuple implements Serializable {

    private static final long serialVersionUID = 1L;
    private TupleDesc td; 
    private RecordId rid; 
    public Field[] fields; 
    public  ArrayList<Field> arrayListVersion;
    

    /**
     * Create a new tuple with the specified schema (type).
     * 
     * @param td
     *            the schema of this tuple. It must be a valid TupleDesc
     *            instance with at least one field.
     */
    public Tuple(TupleDesc td) {
    	this.td = td; 
    	this.fields = new Field[td.getSize()/4];
        // some code goes here
    }

    /**
     * @return The TupleDesc representing the schema of this tuple.
     */
    public TupleDesc getTupleDesc() {
    	return td; 
        // some code goes here
        
    }

    /**
     * @return The RecordId representing the location of this tuple on disk. May
     *         be null.
     */
    public RecordId getRecordId() {
    	//some code goes here
        return rid; 
    }

    /**
     * Set the RecordId information for this tuple.
     * 
     * @param rid
     *            the new RecordId for this tuple.
     */
    public void setRecordId(RecordId rid) {
    	this.rid = rid; 
        // some code goes here
    }

    /**
     * Change the value of the ith field of this tuple.
     * 
     * @param i
     *            index of the field to change. It must be a valid index.
     * @param f
     *            new value for the field.
     */
    public void setField(int i, Field f) {
    	if(fields == null){
    		
    	}
    	else{
    		fields[i] = f;
    	}
    	
        // some code goes here
    }

    /**
     * @return the value of the ith field, or null if it has not been set.
     * 
     * @param i
     *            field index to return. Must be a valid index.
     */
    public Field getField(int i) {
        return (Field) fields[i];
    }

    /**
     * Returns the contents of this Tuple as a string. Note that to pass the
     * system tests, the format needs to be as follows:
     * 
     * column1\tcolumn2\tcolumn3\t...\tcolumnN\n
     * 
     * where \t is any whitespace, except newline, and \n is a newline
     */
    
    
    public String toString() {
        // some code goes here
    	String forReturn = new String();
    	for(Field f : fields){
    		forReturn += f.toString() + " ";
    	}
    	
    	return fields.toString();
    	
        //throw new UnsupportedOperationException("Implement this");
    }
    
private class TupleIterator<Field> implements Iterator{
    	
    	public int index;
    	public int length; 
    	public int tuplesPerPage;
    	
    	@SuppressWarnings("unchecked")
		TupleIterator(){
    		index = -1;
    		length = fields.length;  
    		
    	}

		@Override
		public boolean hasNext() {
			//System.out.println(index);
			if (index == -1){
				return true; 
			}
			else{
				index++;
				if(index == length){
					//System.out.println("no more tuples on page");
					return false; 
				}
			    Field t = (Field) fields[index];
				if(t == null){
					return hasNext();
				}
				//System.out.println("decrementing index");
				index--;
				return true; 
			}
		}
			@SuppressWarnings("unchecked")
			@Override
			public Field next() {

				index++;
				return (Field) fields[index];

			}
		}

		
    
    /**
     * @return
     *        An iterator which iterates over all the fields of this tuple
     * */
    public Iterator<Field> fields()
    {
        // some code goes here
    	arrayListVersion = new ArrayList<Field>();
    	for(Field f : fields){
    		arrayListVersion.add(f);
    	}
        return arrayListVersion.iterator();
        
    }
    
    /**
     * Reset the TupleDesc of this tuple
     * Does not need to worry about the fields inside the Tuple
     * */
    public void resetTupleDesc(TupleDesc td)
    {
        // some code goes here
    	this.td = td; 
    }

	
}
