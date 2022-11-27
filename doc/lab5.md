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
