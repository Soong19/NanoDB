# B+Tree Indexes

> [Assignment 6](http://courses.cms.caltech.edu/cs122/assignments/lab6.html):
> B+Tree Indexes
> ([Telegraph](https://telegra.ph/Assignment-6-B-Tree-Indexes-12-05))

* Complete the B+ tree tuple-file implementation.
* Implement row-event listeners to update indexes when table contents change.
* Do some basic analysis of the B+ tree data structure.

## Part 1: Complete Missing B+ Tree Operations

* Firstly, implement `navigateToLeafPage()` method to support: adding a tuple,
removing a tuple, or searching for tuples. (Of course, basic operations)
  1. Use the search key to determine the next child node (sequential scan to find
    the first key > s-key, choose the previous pointer to return)
  2. Loop until encountering a leaf node
* Secondly, implement `splitLeafAndAddTuple()` method to support: inserting into
  a leaf and split a leaf into two leaves, and then update the parent of the
  leaf with the new leaf-pointer.
  1. Update siblings relation
  2. Move half of tuples to right including inserting tuple
  3. Update parent page (Create a parent node and insert, or just insert)
* Thirdly, implement `movePointersLeft()` and `movePointersRight()` to support:
  moving pointers to a left- or right-sibling page.<br/>
  Because of the requirement that every tuple in an inner page must be
  sandwiched between two pointers, the task is somehow tricky to figure out.
  When moving pointers, we need retrieve the child page to get partition key and
  also update parent partition key.<br/>
  Mostly copy from [CS122](https://github.com/AChelikani/CS122), it's too boring.
