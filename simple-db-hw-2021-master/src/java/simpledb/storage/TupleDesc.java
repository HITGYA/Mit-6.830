package simpledb.storage;

import simpledb.common.Type;

import java.io.Serializable;
import java.util.*;

/**
 * TupleDesc describes the schema of a tuple.
 */
public class TupleDesc implements Serializable {

    /**
     * A help class to facilitate organizing the information of each field
     */

    public static class TDItem implements Serializable {

        private static final long serialVersionUID = 1L;

        /**
         * The type of the field
         */
        public final Type fieldType;

        /**
         * The name of the field
         */
        public final String fieldName;

        public TDItem(Type t, String n) {
            this.fieldName = n;
            this.fieldType = t;
        }

        public String toString() {
            return fieldName + "(" + fieldType + ")";
        }
    }


    /**
     * @return An iterator which iterates over all the field TDItems
     * that are included in this TupleDesc
     */
    private List<TDItem> tupleDescList;

    public Iterator<TDItem> iterator() {
//        done
        return tupleDescList.iterator();
    }

    public TupleDesc() {
    }

    private static final long serialVersionUID = 1L;

    /**
     * Create a new TupleDesc with typeAr.length fields with fields of the
     * specified types, with associated named fields.
     *
     * @param typeAr  array specifying the number of and types of fields in this
     *                TupleDesc. It must contain at least one entry.
     * @param fieldAr array specifying the names of the fields. Note that names may
     *                be null.
     */
    public TupleDesc(Type[] typeAr, String[] fieldAr) {
        // done
        tupleDescList = new ArrayList<>();
        for (int i = 0; i < typeAr.length; i++) {
            TDItem item = new TDItem(typeAr[i], fieldAr[i]);
            tupleDescList.add(item);
        }
    }

    /**
     * Constructor. Create a new tuple desc with typeAr.length fields with
     * fields of the specified types, with anonymous (unnamed) fields.
     *
     * @param typeAr array specifying the number of and types of fields in this
     *               TupleDesc. It must contain at least one entry.
     */
    public TupleDesc(Type[] typeAr) {
        // done
        tupleDescList = new ArrayList<>();
        for (Type type : typeAr) {
            TDItem item = new TDItem(type, null);
            tupleDescList.add(item);
        }
    }

    /**
     * @return the number of fields in this TupleDesc
     */
    public int numFields() {
        // done
        return tupleDescList.size();
    }

    public void setItems(List<TDItem> list) {
        this.tupleDescList = list;
    }

    /**
     * Gets the (possibly null) field name of the ith field of this TupleDesc.
     *
     * @param i index of the field name to return. It must be a valid index.
     * @return the name of the ith field
     * @throws NoSuchElementException if i is not a valid field reference.
     */
    public String getFieldName(int i) throws NoSuchElementException {
        // done
        if (i < 0 || i >= tupleDescList.size()) throw new NoSuchElementException("position" + i + "is not a valid index");
        return tupleDescList.get(i).fieldName;
    }

    /**
     * Gets the type of the ith field of this TupleDesc.
     *
     * @param i The index of the field to get the type of. It must be a valid
     *          index.
     * @return the type of the ith field
     * @throws NoSuchElementException if i is not a valid field reference.
     */
    public Type getFieldType(int i) throws NoSuchElementException {
        // done
        if (i < 0 || i >= tupleDescList.size()) throw new NoSuchElementException("position" + i + "is not a valid index");
        return tupleDescList.get(i).fieldType;
    }

    /**
     * Find the index of the field with a given name.
     *
     * @param name name of the field.
     * @return the index of the field that is first to have the given name.
     * @throws NoSuchElementException if no field with a matching name is found.
     */
    public int fieldNameToIndex(String name) throws NoSuchElementException {
        // done
        if (name == null) throw new NoSuchElementException("fieldName" + name + "is not founded");
        for (int i = 0; i < tupleDescList.size(); i++) {
            if (tupleDescList.get(i).fieldName .equals(name)) return i;
        }
        throw new NoSuchElementException("fieldName" + name + "is not founded");
    }

    /**
     * @return The size (in bytes) of tuples corresponding to this TupleDesc.
     * Note that tuples from a given TupleDesc are of a fixed size.
     */
    public int getSize() {
        // done
        int bytes = 0;
        for (TDItem tdItem : tupleDescList) {
            bytes += tdItem.fieldType.getLen();
        }
        return bytes;
    }

    public List<TDItem> getItems() {
        return tupleDescList;
    }
    public int getItemsLength(){ return tupleDescList.size();}

    /**
     * Merge two TupleDescs into one, with td1.numFields + td2.numFields fields,
     * with the first td1.numFields coming from td1 and the remaining from td2.
     *
     * @param td1 The TupleDesc with the first fields of the new TupleDesc
     * @param td2 The TupleDesc with the last fields of the TupleDesc
     * @return the new TupleDesc
     */
    public static TupleDesc merge(TupleDesc td1, TupleDesc td2) {
        // done
        List<TDItem> items = new ArrayList<>();
        items.addAll(td1.getItems());
        items.addAll(td2.getItems());
        TupleDesc TD = new TupleDesc();
        TD.setItems(items);
        return TD;
    }

    /**
     * Compares the specified object with this TupleDesc for equality. Two
     * TupleDescs are considered equal if they have the same number of items
     * and if the i-th type in this TupleDesc is equal to the i-th type in o
     * for every i.
     *
     * @param o the Object to be compared for equality with this TupleDesc.
     * @return true if the object is equal to this TupleDesc.
     */

    public boolean equals(Object o) {
        // done
        if (!this.getClass().isInstance(o)) return false;
        TupleDesc td = (TupleDesc) o;
        int size = td.getSize();
        if (size != this.tupleDescList.size()) return false;
        for (int i = 0; i < size; i++) {
            TDItem item1 = this.tupleDescList.get(i);
            TDItem item2 = td.tupleDescList.get(i);
            if (!item1.fieldType.equals(item2.fieldType)) return false;
        }
        return true;
    }

    public int hashCode() {
        //done
        int hash = 0;
        for (TDItem tdItem : tupleDescList) {
            hash += tdItem.toString().hashCode();
        }
        return hash;
    }

    /**
     * Returns a String describing this descriptor. It should be of the form
     * "fieldType[0](fieldName[0]), ..., fieldType[M](fieldName[M])", although
     * the exact format does not matter.
     *
     * @return String describing this descriptor.
     */
    public String toString() {
        // done
        StringBuilder builder = new StringBuilder();
        for (int i = 0; i < tupleDescList.size(); i++) {
            TDItem item = tupleDescList.get(i);
            builder.append(item.fieldType).append("(").append(item.fieldName).append(")");
            if (i != tupleDescList.size() - 1) builder.append((", "));
        }
        return builder.toString();
    }

}