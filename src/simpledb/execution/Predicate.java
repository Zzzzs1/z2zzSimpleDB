package simpledb.execution;

import simpledb.storage.Field;
import simpledb.storage.Tuple;

import java.io.Serializable;
import java.time.chrono.MinguoDate;

/**
 * Predicate compares tuples to a specified Field value.
 * Op即操作就是比较符的抽象，而predicate是op的更进一步的抽象
 */
public class Predicate implements Serializable {

    private static final long serialVersionUID = 1L;

    private static final long SerialVersionUID=1L;
    private Field operand;
    private Op op;
    private int index;
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

        public static Op getOp(String s){return getOp(Integer.parseInt(s));}
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
        // some code goes here
        this.index= index;
        this.op=op;
        this.operand=operand;
    }

    /**
     * @return the field number
     */
    public int getField()
    {
        // some code goes here
        return index;
    }

    /**
     * @return the operator
     */
    public Op getOp()
    {
        // some code goes here
        return op;
    }
    
    /**
     * @return the operand
     */
    public Field getOperand()
    {
        // some code goes here
        return operand;
    }
    
    /**
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
        // some code goes here
        return t.getField(index).compare(op,operand);
    }

    /**
     * Returns something useful, like "f = field_id op = op_string operand =
     * operand_string"
     */
    public String toString() {
        // some code goes here
        StringBuilder builder=new StringBuilder();
        //阐述了().getField(index) op operand
        builder.append("(tuple x).fields[").append(index).append("]")
                .append(op.toString()).append(" ").append(operand).append(" ?");
        return builder.toString();
    }
}
