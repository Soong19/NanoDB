# Table Statistics and Plan Costing

> [Assignment 3](http://courses.cms.caltech.edu/cs122/assignments/lab3.html):
> Table Statistics and Plan Costing
> ([Telegraph](https://telegra.ph/Assignment-3-Table-Statistics-and-Plan-Costing-11-13))

* Complete the implementation of statistics-collection from table data.
* Complete the plan-costing computations for various plan-nodes.
* Compute the selectivity of various predicates that can appear within execution
  plans.
* Update the statistics of tuples output by plan-nodes based on predicates.
* Perform some simple experiments with your implementations.

## Prepare

Plan costing is **imprecise** operation, and it is involved to implement.
However, it is really important for a commercial database system to have a good
planner (maybe based on plan costing). It is critical for a DBMS to choose a
"better" physical plan from a lot of possible plans for single one logical plan.

## Task #1: Statistics Collection

> Complete the implementation of statistics-collection from table data.

* For each table, store table statistics in `TableStats`
    * T(R): number of tuples in R
    * A(R): average size of a tuple in R
    * B(R): number of data pages in R
* For each column of a table, store column statistics in `ColumnStats`
    * V(R,c): number of distinct non-null values of column c in R
    * N(R,c): number of null values of column c in R
    * MAX(R,c): maximum value of column c in R (do not collect for string)
    * MIN(R,c): minimum value of column c in R

What we need to do is to implement `analyze` in `HeapTupleFile`:
```
for each data block in the tuple file:
    update stats based on the data block
    (e.g. use tuple_data_end – tuple_data_start to update a "total bytes in all tuples" value)
   
    for each tuple in the current data block:
        update stats based on the tuple (e.g. increment the tuple-count)
        for each column in the current tuple:
            update column-level stats
```

For implementation details, use util/static class to help us. It is very
straightforward.

test
script: [stats-test.sh](../src/test/resources/edu/caltech/test/nanodb/stats/stats-test.sh)

## Task #2: Selectivity Estimates

### Selectivity Estimates

> Complete the plan-costing computations for various plan-nodes.

* Estimate Boolean expressions
    * AND: a * b *...
    * OR: 1- (1 - a) * (1 - b) *...
    * NOT: 1 - a
* Estimate Comparison expressions (Assume: Uniform distribution)
    * EQ/NE: 1 / V(c)
    * GE/<: (Max - Val) / (Max - Min)
    * LE/>: (Val - Val) / (Max - Min)

### Update Plan-Node Column Statistics

> Update the statistics of tuples output by plan-nodes based on predicates.

Assume that the distribution is uniform, we **only** perform update on the
specific column:
* COLUMN op VALUE, where op is one of the following
    * =, ≠: perform selectivity on unique values
    * \>, ≥: perform selectivity on unique values & update MinValue
    * \<, ≤: perform selectivity on unique values & update MaxValue
* P1 AND P2 AND ...
    * Just computes P2 based on P1, and so on

There is very little to do, hopefully I do not get wrong here.

### Task #3: Plan Node Costing

> Complete the plan-costing computations for various plan-nodes.

What we need to do is to estimate cost and column-level statistics for the three
types of plan nodes:
* `SimpleFilterNode`: a select applied to a subplan
* `FileScanNode`: a select applied to a table file stored on disk
* `NestedLoopJoinNode`: a theta-join applied to two subplans; the join may be an
  inner or an outer join

The cost is estimated by:
* number of tuples
* tuple size
* CPU cost
* number of disk-block IOs (for now, assume Mem is enough)
* number of large disk-seeks

TODO:
