#ifndef _TSHI_H
#define _TSHI_H

#include "hashindex.h"
#include "Latch.h"
#include "transaction.h"

class hashDirectory;

// An implementation of a wrapper which uses the lock and the
// latch manager to ensure safe concurrent modifications to the hash
// index.
class threadSafeHashIndex {
	// The shared hash index which is shared across transactions.
	hashIndex *HI;

	// The transaction which owns this copy of the thread safe hash index.
	TransactionID ownerID;

public:
	
	// Constructor to initialize the owner and set the underlying hash index.
	threadSafeHashIndex(TransactionID, hashIndex *);

	
	//--------------------------------------------------------------------
	// threadSafeHashIndex::insertKeyValue
	//
	// Input    : The key and value to be inserted.
	// Return   : OK on success, FAIL on failure. (Uncertain behavior on inserting
	//			  duplicate keys.
	//--------------------------------------------------------------------
	Status insertKeyValue(KeyType key, DataType value);

	
	//--------------------------------------------------------------------
	// threadSafeHashIndex::deleteKey
	//
	// Input    : The key to be deleted.
	// Return   : OK on success, FAIL on failure.
	//--------------------------------------------------------------------
	Status deleteKey(KeyType key);

	
	//--------------------------------------------------------------------
	// threadSafeHashIndex::updateValue
	//
	// Input    : The key to be updated. The new value.
	// Return   : OK on success, FAIL on failure. Missing key will cause FAIL.
	//--------------------------------------------------------------------
	Status updateValue(KeyType key, DataType newValue);

	//--------------------------------------------------------------------
	// threadSafeHashIndex::getValue
	//
	// Input    : The key whose value is to be retrieved.
	// Output   : The value for the key if it exists.
	// Return   : OK on success, FAIL on failure. Missing key will cause FAIL.
	//--------------------------------------------------------------------
	Status getValue(KeyType key, DataType &value);
};

#endif