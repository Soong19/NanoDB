# Join Optimization

> [Assignment 4](http://courses.cms.caltech.edu/cs122/assignments/lab4.html):
> Join Optimization
> ([Telegraph](https://telegra.ph/Assignment-4-Join-Optimization-11-21))

Write planner/optimizer to choose & generate an optimal join ordering using DP.

## Prepare

Optimizer based on **dynamic programming**:
* Identify all "leaves" in the `FROM`-expression of the query.
* Create an optimal plan for accessing **each leaf** identified above.<br/>
  Store each **optimal** leaf plan, along with its cost.
* Create an optimal join plan for every pair of leaves.<br/>
  Store each of optimal plan, along with their costs.
* Continue this process for three leaves.

## Step #1: Refactoring

To reuse our code, create `AbstractPlannerImpl` to hold the logic about grouping
and aggregation. Besides basic functionalities we implemented in Assignment 2,
we need to support generating an optimal join plan. So, we need to know how the
join-related components work.

TODO: Need a rough description about `MakePlan`

## Step #2: Collecting details from the `From`Clause

To provide the base information for Join Optimization, we need to retrieve some
details about leaf nodes and predicates. Because join ordering matters a lot and
pushing conjuncts down benefits.

* Predicates: collect conjuncts from the predicates of non-leaf node
* Leaf node:
  * base-table
  * subquery
  * outer join (handling outer join is grungy)
