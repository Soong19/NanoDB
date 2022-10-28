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
