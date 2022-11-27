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

What we need to do in this step is to generate subquery plan for `SELECT`,
`WHERE` & `HAVING`.

* `SELECT`: Subquery here evaluates to a single value => ScalarSubquery
  * non-correlated: `SELECT a, (SELECT 1) FROM tbl_1`
* `WHERE`: => ScalarSubquery, InSubqueryOperator, ExistsOperator
  * non-correlated: `SELECT a FROM tbl_1 WHERE b IN (SELECT 1, 2, 3)`
* `HAVING`: => ScalarSubquery, InSubqueryOperator, ExistsOperator
  * non-correlated: `SELECT a FROM tbl_1 GROUP BY a HAVING b IN (SELECT 1, 2, 3)`
