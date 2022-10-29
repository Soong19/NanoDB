# NanoDB Set-Up and Storage Layer

> [Assignment 1](http://courses.cms.caltech.edu/cs122/assignments/lab1.html):
> NanoDB Set-Up and Storage Layer
> ([Telegraph](https://telegra.ph/Assignment-1-NanoDB-Set-Up-and-Storage-Layer-10-28))

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

# PageTuple layout: example
Low_Addr               ->                      High_Addr
--------------------------------------------------------
| Bitmap | INTEGER | SHORT | CHAR(5) | VARCHAR | FLOAT |
--------------------------------------------------------
    1        8        2        5       2+len       4
```

---

`deleteTuple(DBPage dbPage, int slot)`
1. Move data before `tuple[slot]` to cover the tuple
2. Mark the tuple as deleted via making tuple to NULL
3. Check the end of slots to find out whether it is NULL.
   If is, decrement the number of slots. (Use `while`)

To implement tuple updating, we need to implement two methods below:

`setNonNullColumnValue(int iCol, Object value)`
1. Clear specific bit in null-bitmap
2. Manipulate space when old value is empty: insertTupleData before the closest
   valid value, low_addr-=newSize
3. Manipulate space when old non-empty value is VARCHAR<br/>
   (1) NewSize > OldSize: insertTupleData, move all data before offset backward,
   low_addr-=delta<br/>
   (2) NewSize < OldSize: deleteTupleData, move all data before offset forward,
   low_addr+=delta
4. Write non-null value, aka. fill the manipulated space
5. Update ValueOffsets

`setNullColumnValue(int iCol)`
1. Set specific bit in null-bitmap
2. Manipulate space: move all data before offset forward, low_addr+=length
3. Update ValueOffsets

##### DEBUG

<b>*</b> StackOverflow when running UpdateCommand => bug01: Infinite Loop

```diff
    var colType = schema.getColumnInfo(iCol).getType();
    var offset = valueOffsets[iCol];
    var length = getColumnValueSize(colType, offset);
    if (colType.getBaseType() != SQLDataType.VARCHAR) {
        // fixed-size type
-       setColumnValue(iCol, value);
+       switch (colType.getBaseType()) {
+           case INTEGER:
+              dbPage.writeInt(offset, (Integer) value);
+              break;
```

### Task #2: Unpinning Tuples and Pages

> Add code to pin/unpin pages and tuples for proper buffer management

The pin scheme we need to implement is very trivial:
* pin/unpin data page when getting, adding tuples in `HeapTupleFile`
* pin/unpin header page in `HeapTupleFileManager`
* unpin current tuple before Iterator/Volcano calling `getNext()`
* unpin `currentTuple` when it is not what we want `SelectNode`

To test buffer pool manager, add execute "set property 'nanodb.pagecache.size' =
65536;" before a query involving a lot of tuples.

### Task #3: NanoDB Storage Performance

> Improve the insert performance of the NanoDB heap file implementation,
> without substantially increasing the overall file size

Header page in NanoDB is responsible for few things, it only stores schema and
table stats of the table file. For simplicity, I choose to not use the header
page as the directory page to track available space. (There is no way to know
how big the table stats is, so it is hard to use directory page scheme
properly.)

---

Prototype design:
Maintain a **bitmap** that records which pages have available space:<br/>
Allocate a bitmap (4 bytes for maximum page sizes 65536) at the start of the
header page. Each tuple page with `page_id` is marked whether free or not by the
specific bit in `bitmap[page_id]`. Because header page is somewhat always
buffered in Mem, it is fast to check out whether a page has <s>enough</s> free
space.

```
# [WRONG] HeaderPage layout (in byte)
-------------------------------------------------------------------------------------
| file type & page size | schema SIZE | stats SIZE | BITMAP | SCHEMA | STATS | FREE |
-------------------------------------------------------------------------------------
0                       2             4            6        14       ...    ...
```

Something goes wrong: the maximum number of pages a tuple file is 65536, which
acquires 8192 bytes for single bitmap. It is so huge an overhead that we cannot
carry on in single page, which introduce more complexity.

---

Prototype Re-design: Use a free page list to track which pages have free space
* Header Page: Maintain head of the free list, default value is 0
* Tuple Page: Maintain next pointer to the next free page

```
# HeaderPage layout (in byte)
----------------------------------------------------------------------------------------
| file type & page size | schema SIZE | stats SIZE | list HEAD | SCHEMA | STATS | FREE |
----------------------------------------------------------------------------------------
0                       2             4            6           10       ...    ...

# TuplePag layout (in byte)
------------------------------------------------------------------
| Header | ... FREE SPACE ... | ... Data Range ... | Next PageNo |
------------------------------------------------------------------
0                                             PageSize - 4      PageSize
```

* `addTuple`: Use the free page list to find free page or create a free page
* `deleteTuple`: When deleting a tuple from a non-free page, add the page to
  free page list

---

After optimization, measure the performance:

```diff
# 1. ins20k.sql
- storage.pagesRead = 3,306,267
+ storage.pagesRead =    40,331
- storage.fileDistanceTraveled = 105,144,752
+ storage.fileDistanceTraveled = 108,444,448
- storage.occupied = 2.7 MB
+ storage.occupied = 2.6 MB

# 2. ins50k.sql
- storage.pagesRead = 20,645,580
+ storage.pagesRead =    100,831
- storage.fileDistanceTraveled = 659,042,640
+ storage.fileDistanceTraveled = 677,734,592
- storage.occupied = 6.7 MB
+ storage.occupied = 6.5 MB

# 3. ins50k-del.sql
- storage.occupied = 5.0 MB
- storage.occupied = 6.3 MB   # before recyle delete-tuple page
+ storage.occupied = 4.8 MB   # after optimization on deleteTuple
```
