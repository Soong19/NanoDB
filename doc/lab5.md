# Advanced Subqueries

> [Assignment 5](http://courses.cms.caltech.edu/cs122/assignments/lab5.html):
> Advanced Subqueries
> ([Telegraph](https://telegra.ph/Assignment-5-Advanced-Subqueries-11-24))

* Complete "naïve" support for uncorrelated subqueries in the `SELECT`, `WHERE`
  and `HAVING` clauses
* Add support for correlated evaluation
* Update plan-costing to include costs of subqueries

## Step #1: Planning Subqueries

> Complete "naïve" support for uncorrelated subqueries in the `SELECT`, `WHERE`
> and `HAVING` clauses

What we need to do at this step is to generate subquery plan for `SELECT`,
`WHERE` & `HAVING`.

* `SELECT`: Subquery here evaluates to a single value => ScalarSubquery
  * non-correlated: `SELECT a, (SELECT 1) FROM tbl_1`
* `WHERE`: => ScalarSubquery, InSubqueryOperator, ExistsOperator
  * non-correlated: `SELECT a FROM tbl_1 WHERE b IN (SELECT 1, 2, 3)`
* `HAVING`: => ScalarSubquery, InSubqueryOperator, ExistsOperator
  * non-correlated: `SELECT a FROM tbl_1 GROUP BY a HAVING b IN (SELECT 1, 2, 3)`

## Step #2: Costing Expressions with Subqueries

> Update plan-costing to include costs of subqueries

What we need to do at this step is to update statistics for `SimpleFilterNode`,
`ProjectNode`, `FileScanNode`. => Implement an `ExpressionCostCalculator` to
summarize cost of the one expression.

* `ProjectNode`: Traverse contents to get per-row cost, then multiply it by
  estimated number of rows.
* `SimpleFilterNode`: Get the cost to evaluate a predicate, then multiply it by
  [non-selectivity] estimated number of rows.
* `FileScanNode`: Same with previous one.

## Step #3: Correlated Evaluation

> Add support for correlated evaluation

A correlated subquery is a subquery that refers to a column of a table that is
not in its `FROM` clause. The column can be in the Projection clause or in the
`WHERE` clause. An environment provides schemas for tuples to find out where it
goes. An expression might refer columns that are not in current table, but in
parent.

In order to support correlated evaluation, NanoDB uses a naive approach:
an environment might have one parent env or more parent envs so that an
expression can be passed along the env-chain until all info is ok. <s>A subquery
need to see the tuples produced by enclosing selects => using the way of parent
envs.</s>

What we need to do at this step is to implement correlated evaluation within a
query engine. (For now, NanoDB doesn't support de-correlate)

For each clause (Projection, Where or Having), when we generate the plan for
subquery, also add its parent environment to it. After planning process of each
clause (already generated plans), set current plan's environment as the env just
generated. => build an environment-chain

For example, when executing "SELECT a FROM test_1 t1 WHERE EXISTS(SELECT b FROM
test_2 t2 WHERE t1.a * 10 = b)", after retrieving one column value from `test_1`,
will always use the predicate to check if satisfy or not. Firstly, in `Exists`
operator, initialize subquery plan node. Secondly, in [subquery's] `Compare`
operator, get lhs & rhs, then compare them. The computation of rhs is easy, but
for lhs:
1. In subquery's environment, walk through current schema to find out whether
   there is the column => Not found
2. Enter the parent environment, get the **current tuple** of `t1` => Found

So, it is clear now. When retrieving a tuple from a node, it also stores tuple
in its environment. Therefore, when subquery acquires a tuple, it can easily
get from the parent environment. => acquire tuple from parent environment
