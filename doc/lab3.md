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

## Task #3: Plan Node Costing

> Complete the plan-costing computations for various plan-nodes.

The cost is estimated by:
* number of tuples
* tuple size
* CPU cost
* number of disk-block IOs (for now, assume Mem is enough => only original
  access matters here)
* number of large disk-seeks (I do not know what is a large disk-seek...)

What we need to do is to estimate cost and column-level statistics for the three
types of plan nodes:
* `SimpleFilterNode`: a select applied to a subplan
    * Increase CPU cost with numTuples (need to walk through all the tuples)
    * Decrease # of tuples with selectivity
* `FileScanNode`: a select applied to a table file stored on disk
    * Inherit statistics from table
    * May need to apply predicate
* `NestedLoopJoinNode`: a theta-join applied to two subplans; the join may be an
  inner or an outer join
    * Inherit statistics from left & right
    * For cpuCost, because use left table as the base table => ltup + (ltup * rtup)
      (And also plus two inherited costs)
    * For # of tuples, multiply the two numbers and apply predicate => Inner Join<br/>
      If outer join, plus the specific number of tuples again.
    * May need to apply predicate

## Task #4: Testing Plan Costing

### Basic Plan Costs

```
# EXPLAIN SELECT * FROM cities;
Explain Plan:
    FileScan[table:  cities] cost=[tuples=254.0, tupSize=23.8, cpuCost=254.0, blockIOs=1, largeSeeks=1]

Estimated 254.000000 tuples with average size 23.787401
Estimated number of block IOs:  1

# EXPLAIN SELECT * FROM cities WHERE population > 5000; (exactly same to prev)
Explain Plan:
    FileScan[table:  cities, pred:  cities.population > 5000] cost=[tuples=254.0, tupSize=23.8, cpuCost=254.0, blockIOs=1, largeSeeks=1]

Estimated 254.000000 tuples with average size 23.787401
Estimated number of block IOs:  1

# EXPLAIN SELECT * FROM cities WHERE population > 1000000; (actually 9 tuples)
Explain Plan:
    FileScan[table:  cities, pred:  cities.population > 1000000] cost=[tuples=225.0, tupSize=23.8, cpuCost=225.0, blockIOs=1, largeSeeks=1]

Estimated 225.000000 tuples with average size 23.787401
Estimated number of block IOs:  1

# EXPLAIN SELECT * FROM cities WHERE population > 5000000; (actually 1 tuple)
Explain Plan:
    FileScan[table:  cities, pred:  cities.population > 5000000] cost=[tuples=99.0, tupSize=23.8, cpuCost=99.0, blockIOs=1, largeSeeks=1]

Estimated 99.000000 tuples with average size 23.787401
Estimated number of block IOs:  1
```

=> Clearly, recording a histogram for different columns would produce much more
accurate estimates.

### Costing Joins

```
# EXPLAIN SELECT store_id FROM stores, cities
# WHERE stores.city_id = cities.city_id AND cities.population > 1000000; (74 tuples actually)
Explain Plan:
    Project[values:  [stores.store_id]] cost=[tuples=1776.2, tupSize=5.3, cpuCost=1020030.3, blockIOs=5, largeSeeks=5]
        SimpleFilter[pred:  cities.city_id == stores.city_id AND cities.population > 1000000] cost=[tuples=1776.2, tupSize=36.8, cpuCost=1018254.0, blockIOs=5, largeSeeks=5]
            NestedLoop[no pred] cost=[tuples=508000.0, tupSize=36.8, cpuCost=510254.0, blockIOs=5, largeSeeks=5]
                FileScan[table:  stores] cost=[tuples=2000.0, tupSize=13.0, cpuCost=2000.0, blockIOs=4, largeSeeks=4]
                FileScan[table:  cities] cost=[tuples=254.0, tupSize=23.8, cpuCost=254.0, blockIOs=1, largeSeeks=1]

Estimated 1776.238159 tuples with average size 5.255343
Estimated number of block IOs:  5

# EXPLAIN SELECT store_id
# FROM stores JOIN
#      (SELECT city_id FROM cities
#       WHERE population > 1000000) AS big_cities
#      ON stores.city_id = big_cities.city_id; (74 actually)
Explain Plan:
    Project[values:  [stores.store_id]] cost=[tuples=1771.7, tupSize=4.7, cpuCost=454221.7, blockIOs=5, largeSeeks=5]
        NestedLoop[pred:  big_cities.city_id == stores.city_id] cost=[tuples=1771.7, tupSize=18.9, cpuCost=452450.0, blockIOs=5, largeSeeks=5]
            FileScan[table:  stores] cost=[tuples=2000.0, tupSize=13.0, cpuCost=2000.0, blockIOs=4, largeSeeks=4]
            Rename[resultTableName=big_cities] cost=[tuples=225.0, tupSize=5.9, cpuCost=450.0, blockIOs=1, largeSeeks=1]
                Project[values:  [cities.city_id]] cost=[tuples=225.0, tupSize=5.9, cpuCost=450.0, blockIOs=1, largeSeeks=1]
                    FileScan[table:  cities, pred:  cities.population > 1000000] cost=[tuples=225.0, tupSize=23.8, cpuCost=225.0, blockIOs=1, largeSeeks=1]

Estimated 1771.653564 tuples with average size 4.736712
Estimated number of block IOs:  5

# EXPLAIN SELECT store_id, property_costs
# FROM stores, cities, states
# WHERE stores.city_id = cities.city_id AND
#       cities.state_id = states.state_id AND
#       state_name = 'Oregon' AND property_costs > 500000; (7 tuples actually)
Explain Plan:
    Project[values:  [stores.store_id, stores.property_costs]] cost=[tuples=19.6, tupSize=11.7, cpuCost=52326324.0, blockIOs=6, largeSeeks=6]
        SimpleFilter[pred:  cities.city_id == stores.city_id AND cities.state_id == states.state_id AND states.state_name == 'Oregon' AND stores.property_costs > 500000] cost=[tuples=19.6, tupSize=52.5, cpuCost=52326304.0, blockIOs=6, largeSeeks=6]
            NestedLoop[no pred] cost=[tuples=25908000.0, tupSize=52.5, cpuCost=26418304.0, blockIOs=6, largeSeeks=6]
                NestedLoop[no pred] cost=[tuples=508000.0, tupSize=36.8, cpuCost=510254.0, blockIOs=5, largeSeeks=5]
                    FileScan[table:  stores] cost=[tuples=2000.0, tupSize=13.0, cpuCost=2000.0, blockIOs=4, largeSeeks=4]
                    FileScan[table:  cities] cost=[tuples=254.0, tupSize=23.8, cpuCost=254.0, blockIOs=1, largeSeeks=1]
                FileScan[table:  states] cost=[tuples=51.0, tupSize=15.7, cpuCost=51.0, blockIOs=1, largeSeeks=1]

Estimated 19.588217 tuples with average size 11.656460
Estimated number of block IOs:  6

# EXPLAIN SELECT store_id, property_costs
# FROM stores JOIN cities ON stores.city_id = cities.city_id
#             JOIN (SELECT * FROM states WHERE state_name = 'Oregon') AS states
#             ON cities.state_id = states.state_id
# WHERE property_costs > 500000;
Explain Plan:
    Project[values:  [stores.store_id, stores.property_costs]] cost=[tuples=19.6, tupSize=11.7, cpuCost=516313.8, blockIOs=6, largeSeeks=6]
        SimpleFilter[pred:  stores.property_costs > 500000] cost=[tuples=19.6, tupSize=52.5, cpuCost=516294.2, blockIOs=6, largeSeeks=6]
            NestedLoop[pred:  cities.state_id == states.state_id] cost=[tuples=39.2, tupSize=52.5, cpuCost=516255.0, blockIOs=6, largeSeeks=6]
                NestedLoop[pred:  cities.city_id == stores.city_id] cost=[tuples=2000.0, tupSize=36.8, cpuCost=512254.0, blockIOs=5, largeSeeks=5]
                    FileScan[table:  stores] cost=[tuples=2000.0, tupSize=13.0, cpuCost=2000.0, blockIOs=4, largeSeeks=4]
                    FileScan[table:  cities] cost=[tuples=254.0, tupSize=23.8, cpuCost=254.0, blockIOs=1, largeSeeks=1]
                Rename[resultTableName=states] cost=[tuples=1.0, tupSize=15.7, cpuCost=1.0, blockIOs=1, largeSeeks=1]
                    FileScan[table:  states, pred:  states.state_name == 'Oregon'] cost=[tuples=1.0, tupSize=15.7, cpuCost=1.0, blockIOs=1, largeSeeks=1]

Estimated 19.588217 tuples with average size 11.656460
Estimated number of block IOs:  6
```

It works well for trivial cases. But the estimation of numTuples is always bad
when the distribution is unbalanced. 

### Updating Column Statistics

```
# EXPLAIN SELECT * FROM cities WHERE city_id > 20 AND city_id < 200;
Explain Plan:
    FileScan[table:  cities, pred:  cities.city_id > 20 AND cities.city_id < 200] cost=[tuples=184.0, tupSize=23.8, cpuCost=184.0, blockIOs=1, largeSeeks=1]

Estimated 184.000000 tuples with average size 23.787401
Estimated number of block IOs:  1

# EXPLAIN SELECT * FROM (
#     SELECT * FROM cities WHERE city_id > 20) t
# WHERE city_id < 200;
Explain Plan:
    SimpleFilter[pred:  t.city_id < 200] cost=[tuples=180.0, tupSize=23.8, cpuCost=468.0, blockIOs=1, largeSeeks=1]
        Rename[resultTableName=t] cost=[tuples=234.0, tupSize=23.8, cpuCost=234.0, blockIOs=1, largeSeeks=1]
            FileScan[table:  cities, pred:  cities.city_id > 20] cost=[tuples=234.0, tupSize=23.8, cpuCost=234.0, blockIOs=1, largeSeeks=1]

Estimated 180.000000 tuples with average size 23.787401
Estimated number of block IOs:  1

# EXPLAIN SELECT * FROM (
#     SELECT * FROM cities WHERE city_id < 200) t
# WHERE city_id > 20;
Explain Plan:
    SimpleFilter[pred:  t.city_id > 20] cost=[tuples=180.0, tupSize=23.8, cpuCost=398.0, blockIOs=1, largeSeeks=1]
        Rename[resultTableName=t] cost=[tuples=199.0, tupSize=23.8, cpuCost=199.0, blockIOs=1, largeSeeks=1]
            FileScan[table:  cities, pred:  cities.city_id < 200] cost=[tuples=199.0, tupSize=23.8, cpuCost=199.0, blockIOs=1, largeSeeks=1]

Estimated 180.000000 tuples with average size 23.787401
Estimated number of block IOs:  1
```

From the cpuCost, the updater works (but don't know works well or not).
