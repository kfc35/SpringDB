#ifndef _HashIndex_H
#define _HashIndex_H

#include "HashDirectory.h"
#include "new_error.h"
#include "BufMgr.h"
#include "da_types.h"
#include "minirel.h"

class HashDirectory;

class HashIndex {
public:
	HashIndex(BufMgr *BM, int initNumberOfBuckets);
	~HashIndex();

	//Use hash function to convert key to its hash value and 
	//use the directory to Get pointer to page in memory
	//do a linear scan to check if the key already exists
	//if not then insert the value, make the page dirty and
	//then unpin the page Set error code accordingly
	Status HashIndex::InsertKeyValue(KeyType key, DataType value, LockType sharedOrExclusive);

	//Use hash function to convert key to its hash value and use
	//the directory to Get pointer to page in memory
	//do a linear scan to find the key and delete the data
	//declare the page to be dirty and unpin it
	Status HashIndex::DeleteKey(KeyType key, LockType sharedOrExclusive);

	//Use hash function to convert key to its hash value and use the 
	// directory search function to Get pointer to page in memory 
	//(bm will pin if necessary). Do a linear scan to find the key
	// and Set value or error code accordingly. Unpin the page
	Status HashIndex::GetValue(KeyType key, DataType &value);


	Status FindBucketToLatch(KeyType key, int &bucketID);

	Status SplitBucket();

	static HashValue hash(KeyType key, int Level) {


		KeyType a = key;
		//COMMENT THE NEXT FEW LINES WHEN DEBUGGING
		a = (a+0x7ed55d16) + (a<<12);
		a = (a^0xc761c23c) ^ (a>>19);
		a = (a+0x165667b1) + (a<<5);
		a = (a+0xd3a2646c) ^ (a<<9);
		a = (a+0xfd7046c5) + (a<<3);
		a = (a^0xb55a4f09) ^ (a>>16);

		int modulo = (1 << Level) * INITNUMBUCKETS;
		int hv =  a % modulo;
		return hv;
	}



private:

	PageID directoryPageID;
	HashDirectory *dir;
	BufMgr *BM;

	int initNumberOfBuckets;

};

#endif