# B+Tree Indexes

> [Assignment 6](http://courses.cms.caltech.edu/cs122/assignments/lab6.html):
> B+Tree Indexes
> ([Telegraph](https://telegra.ph/Assignment-6-B-Tree-Indexes-12-05))

* Complete the B+ tree tuple-file implementation.
* Implement row-event listeners to update indexes when table contents change.
* Do some basic analysis of the B+ tree data structure.

## Part 1: Complete Missing B+ Tree Operations

* Firstly, implement `navigateToLeafPage()` method to support: adding a tuple,
removing a tuple, or searching for tuples.
  * Use the search key to determine the next child node (sequential scan to find
    the first key > s-key, choose the previous pointer to return)
  * Loop until encountering a leaf node
