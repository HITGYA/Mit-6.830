package simpledb.execution;

import simpledb.storage.Field;
import simpledb.storage.Tuple;

import java.io.Serializable;

/**
 * Predicate compares tuples to a specified Field value.
 */
public class Predicate implements Serializable {

    private static final long serialVersionUID = 1L;
    private final int field;
    private final Op op;
    private final Field operand;
    /** Constants used for return codes in Field.compare */
    public enum Op implements Serializable {
        EQUALS, GREATER_THAN, LESS_THAN, LESS_THAN_OR_EQ, GREATER_THAN_OR_EQ, LIKE, NOT_EQUALS;

        /**
         * Interface to access operations by integer value for command-line
         * convenience.
         * 
         * @param i
         *            a valid integer Op index
         */
        public static Op getOp(int i) {
            return values()[i];
        }

        public String toString() {
            if (this == EQUALS)
                return "=";
            if (this == GREATER_THAN)
                return ">";
            if (this == LESS_THAN)
                return "<";
            if (this == LESS_THAN_OR_EQ)
                return "<=";
            if (this == GREATER_THAN_OR_EQ)
                return ">=";
            if (this == LIKE)
                return "LIKE";
            if (this == NOT_EQUALS)
                return "<>";
            throw new IllegalStateException("impossible to reach here");
        }

    }
    
    /**
     * Constructor.
     * 
     * @param field
     *            field number of passed in tuples to compare against.
     * @param op
     *            operation to use for comparison
     * @param operand
     *            field value to compare passed in tuples to
     */
    public Predicate(int field, Op op, Field operand) {
        // done
        this.field = field ;
        this.op = op ;
        this.operand = operand;
    }

    /**
     * @return the field number
     */
    public int getField()
    {
        // done
        return field;
    }

    /**
     * @return the operator
     */
    public Op getOp()
    {
        // doone
        return op;
    }
    
    /**
     * @return the operand
     */
    public Field getOperand()
    {
        // done
        return operand;
    }
    
    /**
     * 使用构造函数中指定的操作符，将元组t的指定字段号（第field个）和operand比较，按照操作op比较
     *
     * Compares the field number of t specified in the constructor to the
     * operand field specified in the constructor using the operator specific in
     * the constructor. The comparison can be made through Field's compare
     * method.
     * 
     * @param t
     *            The tuple to compare against
     * @return true if the comparison is true, false otherwise.
     */

    public boolean filter(Tuple t) {
        // done
        Field field_= t.getField(this.field);
        return field_.compare(op , operand);
    }

    /**
     * Returns something useful, like "f = field_id op = op_string operand =
     * operand_string"
     */
    public String toString() {
        // done
        return "Predicate{"+
                "field="+this.field+
                "op="+this.op+
                "operand="+this.operand+
                "}";
    }
}
