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
	LockList.insert(LockList.end(), make_pair(oid, shared));
}

//--------------------------------------------------------------------
// Transaction::ReleaseAllLocks
//
// Input    : None
// Purpose  : Release all locks Acquired by the current transaction.
//--------------------------------------------------------------------
void Transaction::ReleaseAllLocks() {
	// TODO : Add your code here.
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
		LockManager::ReleaseSharedLock(this->tid, key);
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
	writeList.insert(writeList.end(), pair);
	return OK;
}

//--------------------------------------------------------------------
// Transaction::GroupWrite
//
// Input    : None. (it uses the writeList)
// Output   : None
// Purpose  : Performs all operations in writeList after acquiring 
//			  appropriate locks. Abort transaction and FAIL is locking
//            FAILS. NOTE : Order the lock-acquisition properly to avoid
//            deadlocks. If all transactions follow the same order, then 
//            there can be no deadlock unless due to lock upgrades (think why).
// Return   : OK on success, FAIL on failure.
//--------------------------------------------------------------------
Status Transaction::GroupWrite() {
	// TODO : Add your code here.
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
