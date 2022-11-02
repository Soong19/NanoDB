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

## Task #1: NanoDB Query Planner

> Implement a simple query planner that translates a wide range of parsed SQL
> expressions into query plans that can be executed

What we need to do is to implement *Binder*/*Planner* which generates query
plan. *Planner* is responsible for translate the AST to logical plan, which can
be used directly by DBMS.

![the life of a SQL query](https://user-images.githubusercontent.com/70138429/199474700-45b40411-90b4-44bb-9492-ff56742c7296.png)

High level architecture:
* Processing Model: demand-driven pipeline (Use `Next()` function)
* Plan Processing: Top-to-Bottom (Pull data from child)
* Access Method: File scan (Index Scan or Sequential Scan)
