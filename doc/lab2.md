# SQL Translation and Joins

> [Assignment 2](http://courses.cms.caltech.edu/cs122/assignments/lab2.html)
> SQL Translation and Joins
> ([Telegraph](https://telegra.ph/Assignment-2-SQL-Translation-and-Joins-11-02))

* Implement a simple query planner that translates a wide range of parsed SQL
  expressions into query plans that can be executed
* Complete the implementation of a nested-loop join plan-node that supports
  inner and left-outer joins
* Create some automated tests to ensure that your inner- and outer-join support
  works correctly

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

In practice, we need to deal with different types of join in `getTuplesToJoin`:
* *inner join*: Basic join return all matching tuples
* *left-outer join*: Includes non-matching rows from the left table
* *right-outer join*: Includes non-matching rows from the right table

```java
while (getTuplesToJoin()) {
    if (canJoinTuples())
        return joinTuples(leftTuple, rightTuple);
    else if (joinType == JoinType.LEFT_OUTER)
        return joinTuples(leftTuple, nullTuple);
    else if (joinType == JoinType.RIGHT_OUTER)
        return joinTuples(nullTuple, rightTuple);
}
```
