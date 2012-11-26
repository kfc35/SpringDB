#ifndef _TSHI_H
#define _TSHI_H

#include "da_types.h"
#include "HashIndex.h"
#include "latch.h"
#include "transaction.h"

class HashDirectory;

// An implementation of a wrapper which uses the lock and the
// latch manager to ensure safe concurrent modifications to the hash
// index.
class ThreadSafeHashIndex {
	// The shared hash index which is shared across transactions.
	HashIndex *HI;

	// The transaction which owns this copy of the thread safe hash index.
	TransactionID ownerID;

public:
	
	// Constructor to initialize the owner and Set the underlying hash index.
	ThreadSafeHashIndex(TransactionID, HashIndex *);

	
	//--------------------------------------------------------------------
	// ThreadSafeHashIndex::InsertKeyValue
	//
	// Input    : The key and value to be inserted.
	// Return   : OK on success, FAIL on failure. (Uncertain behavior on inserting
	//			  duplicate keys.
	//--------------------------------------------------------------------
	Status InsertKeyValue(KeyType key, DataType value);

	
	//--------------------------------------------------------------------
	// ThreadSafeHashIndex::DeleteKey
	//
	// Input    : The key to be deleted.
	// Return   : OK on success, FAIL on failure.
	//--------------------------------------------------------------------
	Status DeleteKey(KeyType key);

	
	//--------------------------------------------------------------------
	// ThreadSafeHashIndex::UpdateValue
	//
	// Input    : The key to be updated. The new value.
	// Return   : OK on success, FAIL on failure. Missing key will cause FAIL.
	//--------------------------------------------------------------------
	Status UpdateValue(KeyType key, DataType newValue);

	//--------------------------------------------------------------------
	// ThreadSafeHashIndex::GetValue
	//
	// Input    : The key whose value is to be retrieved.
	// Output   : The value for the key if it exists.
	// Return   : OK on success, FAIL on failure. Missing key will cause FAIL.
	//--------------------------------------------------------------------
	Status GetValue(KeyType key, DataType &value);
};

#endif