# SQL Translation and Joins

> [Assignment 2](http://courses.cms.caltech.edu/cs122/assignments/lab2.html):
> SQL Translation and Joins
> ([Telegraph](https://telegra.ph/Assignment-2-SQL-Translation-and-Joins-11-02))

* Implement a simple query planner that translates a wide range of parsed SQL
  expressions into query plans that can be executed
* Complete the implementation of a nested-loop join plan-node that supports
  inner and left-outer joins
* Create some automated tests to ensure that your inner- and outer-join support
  works correctly
* Implement a processor on aggregation that scans and transforms expressions,
  identifies and replaces aggregate functions

Before starting the assignment,
read [数据库内核杂谈](https://www.infoq.cn/theme/46) first.

## Task #1: NanoDB Query Planner

> Implement a simple query planner that translates a wide range of parsed SQL
> expressions into query plans that can be executed

* *Parser*: Translate SQL query statements to **Abstract Syntax Tree**
    * Detect syntax error without table information
* *Binder*: Bind table metadata with AST to **bound AST**
    * Detect error with table information, such as is table "a.tbl" in database
* *Planner*/*Optimizer*: Translate AST to **physical plan**, which can be used
  directly by DBMS. In general, *planner* first generate **logical plan** and
  multiple **physical plan**s by it. Use a cost-model to choose the least-cost
  plan to execute.
    * Plan: a tree constructed by multiple plan nodes

<details>
<img alt="real database" src="https://static001.infoq.cn/resource/image/74/90/74207315eda9acd26bbb91c922b66c90.png">
<p></p>
<img alt="the life of a SQL query]s" src="https://user-images.githubusercontent.com/70138429/199474700-45b40411-90b4-44bb-9492-ff56742c7296.png">
</details>

For this task, we only implement: generate a correct **physical plan**.

In NanoDB, when get a root plan node (tree), it will follow the scheme below to
execute the tree.

High level architecture:
* Processing Model: demand-driven pipeline (Use `Next()` function)
* Plan Processing: Top-to-Bottom (Pull data from child)
* Access Method: File scan (Index Scan or Sequential Scan)

---

For now, the *planner* should support: Project (`SELECT`), Select (`WHERE`), 0
or 1 table (`FROM`). Follow the following scheme:

```
PlanNode plan = null;

if (FROM-clause is present)
    plan = generate_FROM_clause_plan();

if (WHERE-clause is present)
    plan = add_WHERE_clause_to(plan);

if (GROUP-BY-clause and/or HAVING-clause is present)
    plan = handle_grouping_and_aggregation(plan);

if (ORDER-BY-clause is present)
    plan = add_ORDER_BY_clause_to(plan);

// There's always a SELECT clause of some sort!
plan = add_SELECT_clause_to(plan);
```

The implementation is very straightforward: behave varies by clause.

## Task #2: Nested-Loop Join

> Complete the implementation of a nested-loop join plan-node that supports
> inner and left-outer joins

*Nested-Loop Join* is a naive (stupid) way to implement Join, but it is so easy
that it has been implemented by almost every DBMS. The pseudocode illustrates
how it works:

```
# For table r and s, walk through all tuples using a 2-for loop
for tr in r:
    for ts in s:
        if pred(tr, ts):
            add join(tr, ts) to result
```

---

NOTE: There is something wrong with OUTER JOIN, because I misunderstood what it
means.

For example, left outer join with "SELECT * FROM tbl_a LEFT JOIN tbl_b ON a=c"

```
 tbl_a
| a | b |
|---|---|
| 1 | 2 |
| 2 | 3 |
| 3 | 4 |

 tbl_b
| c | d |
|---|---|
| 1 | 2 |
| 2 | 3 |

=> result
| a | b | c | d |
|---|---|---|---|
| 1 | 2 | 1 | 2 |
| 2 | 3 | 2 | 3 |
| 3 | 4 | \ | \ |
```

---

In practice, we need to deal with different types of join in `getTuplesToJoin`:
* *inner join*: Basic join return all matching tuples
* *left-outer join*: Includes non-matching rows from the left table
* *right-outer join*: Includes non-matching rows from the right table

```java
while (getTuplesToJoin()) {
    if (canJoinTuples())
        return joinTuples(leftTuple,rightTuple);

    else if (joinType==JoinType.LEFT_OUTER ||
            (joinType==JoinType.FULL_OUTER&&leftTuple==null))
        return joinTuples(leftTuple,rightNullTuple);

    else if (joinType==JoinType.RIGHT_OUTER ||
            (joinType==JoinType.FULL_OUTER&&rightTuple==null))
        return joinTuples(leftNullTuple,rightTuple);
}
```

## Task #3: Automated Testing

> Create some automated tests to ensure that your inner- and outer-join support
> works correctly

My simple test cases:
* testNormalInnerJoin: Inner Join on 2 normal tables
* testNormalLeftOuterJoin: Left Outer Join on 2 normal tables
* testNormalRightOuterJoin: Right Outer Join on 2 normal tables
* testEmptyLeftOuterJoin: Left Outer Join on empty table (left) and normal
  table (right)
* testEmptyRightOuterJoin: Right Outer Join on empty table (right) and normal
  table (left)
* testOneRowLeft: Inner Join on table with one row (left) and table with
  multiple rows (right)
* testOneRowRight: Inner Join on table with one row (right) and table with
  multiple rows (left)
* testInnerJoinWhere: Cross Join with WHERE
* testThreeInnerJoin: Inner Join on 3 tables

NanoDB supports: INNER JOIN, FULL OUTER JOIN, LEFT OUTER JOIN, RIGHT OUTER JOIN,
CROSS JOIN. The SQL statements supports most of SQL queries without GROUP BY.

## Task #4: Grouping and Aggregation

> Implement a processor on aggregation that scans and transforms expressions,
> identifies and replaces aggregate functions

The two main tasks:
1. Scanning and/or Transforming Expressions
2. Identifying and Replacing Aggregate Functions

The three exceptions:
1. No nested function call
2. No `WHERE`/`ON` containing aggregation
3. No `GROUP BY` containing aggregation

---

Implement approach:
1. Validate whether expressions contain aggregation in 3 situations
   (`WHERE`/`ON`/`GROUP BY`)
2. Implement basic Aggregation expression: Use `AggregationProcessor` to
   validate whether there is a nested-aggregation, and replace the aggregation
   with a placeholder `ColumnValue`
3. Implement Aggregation expression with `GROUP BY`: Add group by attributes
   to the aggregate and group by node
4. Implement Aggregation expression with `GROUP BY` and `HAVING`: Add a filter
   node as the parent node

## Task #5: Extra Credit

> Implementing a LimitOffsetNode

Do not forget to perform test on it though it is easy to implement.
