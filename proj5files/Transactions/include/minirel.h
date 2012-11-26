#ifndef _MINIREL_H
#define _MINIREL_H

#include <iostream>

#include "da_types.h"
#include "new_error.h"
#include "system_defs.h"

using namespace std;

enum AttrType {
	attrString,
	attrInteger,
	attrReal,
	attrSymbol,
	attrFoo,
	attrNull
};

enum AttrOperator {
	aopEQ,
	aopLT,
	aopGT,
	aopNE,
	aopLE,
	aopGE,
	aopNOT,
	aopNOP,
	opRANGE
};

enum LogicalOperator {
	lopAND,
	lopOR,
	lopNOT
};

enum TupleOrder {
	Ascending,
	Descending,
	Random
};

enum IndexType {
	None,
	//  B_Index,
	SH_Index,    // Static Hashing
	Hash
};

enum SelectType {
	selRange,
	selExact,
	selBoth,
	selUndefined
};

public enum TransactionStatus
{
	NEW, RUNNING, GROUPWRITE, COMMITTED, ABORTED
};

public enum LockType { SHARED, EXCLUSIVE};

public enum OpType {INSERT, DELETE, UPDATE};

typedef int PageID;

struct RecordID {
	PageID  pageNo;
	int     slotNo;

	int operator==(const RecordID rid) const
	{
		return (pageNo == rid.pageNo) && (slotNo == rid.slotNo);
	};

	int operator!=(const RecordID rid) const
	{
		return (pageNo != rid.pageNo) || (slotNo != rid.slotNo);
	};

	bool operator<(const RecordID &rid) const
	{	return (pageNo < rid.pageNo)
			   || (pageNo == rid.pageNo && slotNo < rid.slotNo);
	};

	bool operator>(const RecordID &rid) const
	{	return (pageNo > rid.pageNo)
			   || (pageNo == rid.pageNo && slotNo > rid.slotNo);
	};

	friend ostream &operator<< (ostream &out, const struct RecordID rid);
};

// typedef struct RecordID RecordID;

const int MINIBASE_PAGESIZE = 4096;           // in bytes

const int MINIBASE_BUFFER_POOL_SIZE = 1024;   // in Frames

const int MINIBASE_DB_SIZE = 1000000;  // in Pages => the DBMS Manager tells the DB how much disk
									   //space is available for the database.


const int MAX_NUM_RECORDS_PER_PAGE = 400;
const int BIT_VECTOR_SIZE = (MAX_NUM_RECORDS_PER_PAGE / 8 + 1);

const int MINIBASE_MAX_TRANSACTIONS = 100;
const int MINIBASE_DEFAULT_SHAREDMEM_SIZE = 1000;

const int MAXFILENAME  = 15;          // also the name of a relation
const int MAXINDEXNAME = 40;
const int MAXATTRNAME  = 15;

const int NUMBUF = 50; // default buffer pool size
const int INITNUMBUCKETS = 8; //default initial number of buckets in linear hashing

#endif
