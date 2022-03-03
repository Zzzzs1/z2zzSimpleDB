package simpledb.execution;

import simpledb.common.Type;
import simpledb.storage.*;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Knows how to compute some aggregate over a set of StringFields.
 */
public class StringAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;

    //该索引值指定了要使用tuple的哪一个列分组
    int gbField;
    //指定使用tuple哪一个列进行聚合
    int aField;
    //聚合后tuple行描述
    TupleDesc td;
    //指定作为分组依据的那一列的值的类型
    Type gbFieldType;
    //指定使用哪种聚合操作
    Op aggOp;
    //Key:每个不同的分组字段Value:聚合的结果
    HashMap<Field,Integer> groupMap;
    //Key:每个不同的分组字段Value:该分组进行平均值聚合过程处理的所有值的个数以及他们的和
    //这个map仅用于辅助在计算平均值时得到以前聚合过的总数
    HashMap<Field, List<Integer>> avgMap;

    /**
     * Aggregate constructor
     * @param gbfield the 0-based index of the group-by field in the tuple, or NO_GROUPING if there is no grouping
     * @param gbfieldtype the type of the group by field (e.g., Type.INT_TYPE), or null if there is no grouping
     * @param afield the 0-based index of the aggregate field in the tuple
     * @param what aggregation operator to use -- only supports COUNT
     * @throws IllegalArgumentException if what != COUNT
     */

    public StringAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        // some code goes here
        this.gbField = gbfield;
        this.gbFieldType = gbfieldtype;
        this.aField = afield;
        this.aggOp = what;
        groupMap = new HashMap<>();
        avgMap = new HashMap<>();
        this.td = gbfield != NO_GROUPING ?
                new TupleDesc(new Type[]{gbfieldtype, Type.INT_TYPE}, new String[]{"gbVal", "aggVal"})
                : new TupleDesc(new Type[]{Type.INT_TYPE}, new String[]{"aggVal"});
}

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the constructor
     * @param tup the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        // some code goes here
        StringField aggField = (StringField) tup.getField(aField);
        String value = aggField.getValue();
        Field groupField = gbField == NO_GROUPING ? null : tup.getField(gbField);
        if(groupField != null && gbFieldType != groupField.getType()) {
            throw new IllegalArgumentException("error gbField type!");
        }
        if(aggOp == Op.COUNT) {
            groupMap.put(groupField, groupMap.getOrDefault(groupField, 0) + 1);
        } else {
            throw new IllegalArgumentException("Error Op Type");
        }
    }


    /**
     * Create a OpIterator over group aggregate results.
     *
     * @return a OpIterator whose tuples are the pair (groupVal,
     *   aggregateVal) if using group, or a single (aggregateVal) if no
     *   grouping. The aggregateVal is determined by the type of
     *   aggregate specified in the constructor.
     */
    public OpIterator iterator() {
        // some code goes here
        ArrayList<Tuple> tuples = new ArrayList<>();
        for(Field gField : groupMap.keySet()) {
            Tuple tuple = new Tuple(this.td);
            if (this.gbField == NO_GROUPING) {
                tuple.setField(0, new IntField(groupMap.get(gField)));
            } else {
                tuple.setField(0, gField);
                tuple.setField(1, new IntField(groupMap.get(gField)));
            }
            tuples.add(tuple);
        }
        return new TupleIterator(td, tuples);
    }


}
