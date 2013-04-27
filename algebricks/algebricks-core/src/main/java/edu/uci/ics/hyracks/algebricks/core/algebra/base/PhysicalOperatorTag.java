package edu.uci.ics.hyracks.algebricks.core.algebra.base;

public enum PhysicalOperatorTag {
    AGGREGATE,
    ASSIGN,
    BROADCAST_EXCHANGE,
    BTREE_SEARCH,
    STATS,
    DATASOURCE_SCAN,
    DISTRIBUTE_RESULT,
    EMPTY_TUPLE_SOURCE,
    EXTERNAL_GROUP_BY,
    IN_MEMORY_HASH_JOIN,
    HASH_GROUP_BY,
    HASH_PARTITION_EXCHANGE,
    HASH_PARTITION_MERGE_EXCHANGE,
    HYBRID_HASH_JOIN,
    HDFS_READER,
    IN_MEMORY_STABLE_SORT,
    MICRO_PRE_CLUSTERED_GROUP_BY,
    NESTED_LOOP,
    NESTED_TUPLE_SOURCE,
    ONE_TO_ONE_EXCHANGE,
    PRE_SORTED_DISTINCT_BY,
    PRE_CLUSTERED_GROUP_BY,
    RANGE_PARTITION_EXCHANGE,
    RANDOM_MERGE_EXCHANGE,
    RTREE_SEARCH,
    RUNNING_AGGREGATE,
    SORT_MERGE_EXCHANGE,
    SINK,
    SINK_WRITE,
    SPLIT,
    STABLE_SORT,
    STREAM_LIMIT,
    STREAM_SELECT,
    STREAM_PROJECT,
    STRING_STREAM_SCRIPT,
    SUBPLAN,
    UNION_ALL,
    UNNEST,
    WRITE_RESULT,
    INSERT_DELETE,
    INDEX_INSERT_DELETE,
    UPDATE,
    INVERTED_INDEX_SEARCH,
    FUZZY_INVERTED_INDEX_SEARCH,
    PARTITIONINGSPLIT,
    EXTENSION_OPERATOR
}
