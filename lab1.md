# NanoDB Set-Up and Storage Layer

> [Assignment 1](http://courses.cms.caltech.edu/cs122/assignments/lab1.html):
> NanoDB Set-Up and Storage Layer

* Add support for tuple updates and deletion in NanoDB
* Add code to pin/unpin pages and tuples for proper buffer management
* Improve the insert performance of the NanoDB heap file implementation,
  without substantially increasing the overall file size

### Task #1: Deleting and Updating Tuples

> Add support for tuple updates and deletion in NanoDB

High-level Architectures:

* HeaderPage (Page 0): meta-data
* DataPage: slotted-page (both index and table)
* HeapTupleFileManager: create, open, delete files
* HeapTupleFile: scanning a tuple file, inserting a record, etc.
* HeapFilePageTuple: individual tuple in heap file

```
# DataPage layout
----------------------------------------------------
| Header | ... FREE SPACE ... | ... Data Range ... |
----------------------------------------------------
# Header format
-------------------------------
| Slot1 | Slot2 | Slot3 | ... |
-------------------------------
# Data Range
----------------------------------
| ... | Tuple3 | Tuple2 | Tuple1 |
----------------------------------
```

---

`deleteTuple(DBPage dbPage, int slot)`

1. Move data before `tuple[slot]` to cover the tuple
2. Mark the tuple as deleted via making tuple to NULL
3. Check the end of slots to find out whether it is NULL.
   If is, decrement the number of slots. (Use `while`)
