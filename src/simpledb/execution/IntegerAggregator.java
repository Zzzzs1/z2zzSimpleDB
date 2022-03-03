package simpledb.execution;

import simpledb.common.Type;
import simpledb.storage.*;

import java.util.*;

/**
 * Knows how to compute some aggregate over a set of IntFields.
 *  * 我加了参数给构造器，目的是想让聚合器的使用者(即代表aggregate操作符的Aggregate类)来
 *  * 负责告诉aggregator聚合后的行描述。因为聚合器在生成结果的迭代器时需要使用到td，然而这部分逻辑在Aggregate类中也需要
 *  * 已经在Aggregate中实现了，没必要重复，于是由Aggregate类来负责给聚合器传入聚合后的行描述
 *  * 那么，为什么不在聚合器中实现“得到聚合后的行描述”，然后让使用者调用就好了呢？
 *  * 这是因为在原设计中，Aggregate的测试类有一个方法是在新建了Aggregate类，还没有进行聚合的前提下就调用了getTupleDesc
 *而如果它是调用聚合器的方法，由于聚合器还没有遇到任何一个tuple，所以无法确定聚合后的行描述，就会返回null或者出错
 */
public class IntegerAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;

    //该索引值指定了要使用tuple的哪一个列分组
    int groupField;
    //指定使用tuple哪一个列进行聚合
    int aggregateField;
    //聚合后tuple行描述
    TupleDesc td;
    //指定作为分组依据的那一列的值的类型
    Type groupFieldType;
    //指定使用哪种聚合操作
    Op aggregateOp;
    //Key:每个不同的分组字段Value:聚合的结果
    HashMap<Field,Integer> groupMap;
    //Key:每个不同的分组字段Value:该分组进行平均值聚合过程处理的所有值的个数以及他们的和
    //这个map仅用于辅助在计算平均值时得到以前聚合过的总数
    HashMap<Field,List<Integer>> avgMap;
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
        // some code goes here
        this.groupField = gbfield;
        this.groupFieldType = gbfieldtype;
        this.aggregateField = afield;
        this.aggregateOp = what;
        groupMap = new HashMap<>();
        avgMap = new HashMap<>();
        this.td = gbfield != NO_GROUPING ?
                new TupleDesc(new Type[]{gbfieldtype, Type.INT_TYPE}, new String[]{"gbVal", "aggVal"})
                : new TupleDesc(new Type[]{Type.INT_TYPE}, new String[]{"aggVal"});
    }


    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the
     * constructor
     * 
     * @param tup
     *            the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        // some code goes here
        //获取聚合字段
        IntField aField = (IntField) tup.getField(aggregateField);
        //获取聚合字段的值
        int value = aField.getValue();
        //获取分组字段，如果单纯只是聚合，则该字段为null
        Field gbField = groupField == NO_GROUPING ? null : tup.getField(groupField);
        if (gbField != null && gbField.getType() != this.groupFieldType && groupFieldType != null) {
            throw new IllegalArgumentException("Tuple has wrong type");
        }
        //根据聚合运算符处理数据
        switch (aggregateOp) {
            case MIN:
                groupMap.put(gbField, Math.min(groupMap.getOrDefault(gbField, value), value));
                break;
            case MAX:
                groupMap.put(gbField, Math.max(groupMap.getOrDefault(gbField, value), value));
                break;
            case COUNT:
                groupMap.put(gbField, groupMap.getOrDefault(gbField, 0) + 1);
                break;
            case SUM:
                groupMap.put(gbField, groupMap.getOrDefault(gbField, 0) + value);
                break;
            case AVG:
                if (!avgMap.containsKey(gbField)) {
                    List<Integer> list = new ArrayList<>();
                    list.add(value);
                    avgMap.put(gbField, list);
                } else {
                    List<Integer> list = avgMap.get(gbField);
                    list.add(value);
                    avgMap.put(gbField, list);
                }
                break;
            default:
                throw new IllegalArgumentException("Wrong Operator!");
        }
    }

    /**
     * Create a OpIterator over group aggregate results.
     * 
     * @return a OpIterator whose tuples are the pair (groupVal, aggregateVal)
     *         if using group, or a single (aggregateVal) if no grouping. The
     *         aggregateVal is determined by the type of aggregate specified in
     *         the constructor.
     */
    public OpIterator iterator() {
        // some code goes here
        ArrayList<Tuple> tuples = new ArrayList<>();
        if(aggregateOp == Op.AVG) {
            for(Field gField : avgMap.keySet()) {
                List<Integer> list = avgMap.get(gField);
                int sum = 0;
                for(Integer i : list) {
                    sum += i;
                }
                int avg = sum / list.size();
                Tuple tuple = new Tuple(td);
                if(groupField != NO_GROUPING) {
                    tuple.setField(0, gField);
                    tuple.setField(1, new IntField(avg));
                } else {
                    System.out.println(tuple + "<====>");
                    tuple.setField(0, new IntField(avg));
                }
                tuples.add(tuple);
            }
            return new TupleIterator(td, tuples);
        } else {
            for(Field gField : groupMap.keySet()) {
                Tuple tuple = new Tuple(td);
                if(groupField != NO_GROUPING) {
                    tuple.setField(0, gField);
                    tuple.setField(1, new IntField(groupMap.get(gField)));
                } else {
                    tuple.setField(0, new IntField(groupMap.get(gField)));
                }

                tuples.add(tuple);
            }
            return new TupleIterator(td, tuples);
        }
    }


}
