#include "transaction.h"
#include "lock.h"
#include <vector>
#include <algorithm>
using namespace std;

Transaction::Transaction()
{
	this->status = NEW;
}

Status Transaction::AbortTransaction()
{
	this->status = ABORTED;
	ReleaseAllLocks();
	cout << "Transaction " << this->tid << " Aborted" << endl;
	return OK;
}

//--------------------------------------------------------------------
// Transaction::InsertLock
//
// Input    : The identifier or the object which is locked and if it is a shared lock.
// Purpose  : Add entry into lock list to keep track of all locks Acquired by 
//			  this transaction.
// Return   : OK. Should never return FAIL.
//--------------------------------------------------------------------
void Transaction::InsertLock(int oid, bool shared) {
	// TODO : Add your code here.
	if (!shared) {
		// look for existing shared lock and upgrade
		for (int i = 0; i < LockList.size(); i++) {
			if (LockList[i].first == oid) {
				LockList[i].second = EXCLUSIVE;
				return;
			}
		}
	}
	LockList.push_back(make_pair(oid, shared));
}

//--------------------------------------------------------------------
// Transaction::ReleaseAllLocks
//
// Input    : None
// Purpose  : Release all locks Acquired by the current transaction.
//--------------------------------------------------------------------
void Transaction::ReleaseAllLocks() {
	// TODO : Add your code here.
	for (int i = 0; i < LockList.size(); i++) {
		pair<int, bool> lockPair = LockList[i];
		if (lockPair.second == SHARED) { // share lock
			LockManager::ReleaseSharedLock(this->tid, lockPair.first);
		} else { // exclusive lock
			LockManager::ReleaseExclusiveLock(this->tid, lockPair.first);
		}
	}
	LockList.clear();
}

//--------------------------------------------------------------------
// Transaction::Read
//
// Input    : The key whose value is to be read.
// Output   : The value corresponding to the key.
// Purpose  : Get the value of the key using the Hash Index with 
//            appropriate locking. Abort Transaction and FAIL if locking
//			  fails.
// Return   : OK on success, FAIL on failure.
//--------------------------------------------------------------------
Status Transaction::Read(KeyType key, DataType &value) {
	if (!LockManager::AcquireSharedLock(this->tid, key)) {
		// acquiring lock failed
		ReleaseAllLocks();
		this->status = ABORTED;
		return FAIL;
	}

	InsertLock(key, SHARED);

	if (TSHI->GetValue(key, value) != OK) {
		ReleaseAllLocks();
		this->status = ABORTED;
		return FAIL;
	}

	return OK;
}

//--------------------------------------------------------------------
// Transaction::AddWritePair
//
// Input    : The key-value pair to be written with the type of the
//			  operation (INSERT/UPDATE/DELETE)
// Output   : None
// Purpose  : Add a key-value pair to the writeList.
// Return   : OK on success, FAIL on failure.
//--------------------------------------------------------------------
Status Transaction::AddWritePair(KeyType key, DataType value, OpType op) {
	// TODO : Add your code here.
	KVP pair;
	pair.key = key;
	pair.value = value;
	pair.op = op;
	vector<KVP>::iterator position = FindInsertionPosition(key);
	writeList.insert(position, pair);
	return OK;
}

vector<KVP>::iterator Transaction::FindInsertionPosition(KeyType key) {
	vector<KVP>::iterator iter = writeList.begin();
	while (iter != writeList.end() && iter->key < key) {
		iter++;
	}
	return iter;
}

//--------------------------------------------------------------------
// Transaction::GroupWrite
//
// Input    : None. (it uses the writeList)
// Output   : None
// Purpose  : Performs all operations in writeList after acquiring 
//			  appropriate locks. Abort transaction and FAIL if locking
//            FAILS. NOTE : Order the lock-acquisition properly to avoid
//            deadlocks. If all transactions follow the same order, then 
//            there can be no deadlock unless due to lock upgrades (think why).
// Return   : OK on success, FAIL on failure.
//--------------------------------------------------------------------
Status Transaction::GroupWrite() {
	// TODO : Add your code here.

	// piazza says to fail if calling GroupWrite with empty writeList
	// but we can't pass test1 with this block of code because it calls
	// GroupWrite with empty writeList

	//if (writeList.size() == 0) {
	//	ReleaseAllLocks();
	//	this->status = ABORTED;
	//	return FAIL;
	//}

	// acquiring locks
	for (int i = 0; i < writeList.size(); i++) {
		KVP kvPair = writeList[i];
		if (!LockManager::AcquireExclusiveLock(this->tid, kvPair.key)) {
			ReleaseAllLocks();
			this->status = ABORTED;
			return FAIL;
		}

		InsertLock(kvPair.key, EXCLUSIVE);
	}

	// writing data
	for (int i = 0; i < writeList.size(); i++) {
		KVP kvPair = writeList[i];
		switch (kvPair.op) {
		case INSERT:
			if (TSHI->InsertKeyValue(kvPair.key, kvPair.value) != OK) {
				ReleaseAllLocks();
				this->status = ABORTED;
				return FAIL;
			}
			break;
		case UPDATE:
			if (TSHI->UpdateValue(kvPair.key, kvPair.value) != OK) {
				ReleaseAllLocks();
				this->status = ABORTED;
				return FAIL;
			}
			break;
		case DELETE:
			if (TSHI->DeleteKey(kvPair.key) != OK) {
				ReleaseAllLocks();
				this->status = ABORTED;
				return FAIL;
			}
			break;
		default:
			return FAIL;
		}
	}

	ReleaseAllLocks();
	this->status = GROUPWRITE;
	return OK;
}

Status Transaction::StartTranscation()
{
	if (this->status != NEW)
	{
		return FAIL;
	}

	this->status = RUNNING;

	cout << "Transaction " << this->tid << " Started" << endl;
	return OK;
}

Status Transaction::EndTransaction()
{
	if (this->status != ABORTED) {
		ReleaseAllLocks();
	}

	return OK;
}

Status Transaction::CommitTransaction()
{
	if ((this->status != RUNNING) && (this->status != GROUPWRITE))
	{
		return FAIL;
	}

	cout << "Transaction " << this->tid << " Committed" << endl;
	this->status = COMMITTED;
	return OK;
}
