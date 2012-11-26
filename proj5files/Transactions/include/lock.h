#ifndef _LOCK_H
#define _LOCK_H

#include "minirel.h"
#using <System.dll>
using namespace System;
using namespace System::Threading;
using namespace System::Collections::Generic;

#define PRINTLOG 0 //1 FOR DEBUG

enum WakeUpListStatus {
	EMPTY,
	SLIST,
	XLIST
};

public ref class Request {
public:
	int pid;
	LockType type;

	Request(int pid, LockType type)
	{
		this->pid = pid;
		this->type = type;
	}
};

public ref class ReadWriteFIFOLock {
public:
	Object^ _m; //monitor ojbect
	int objectID;
	int sharedLockCount;
	int exclusiveLockCount;

	List<int>^ wakeUpList;
	List<int>^ abortList;
	List<int>^ lockingList;
	LinkedList<Request^>^ waitQ;

	ReadWriteFIFOLock(int oid);

	bool AcquireSharedLock(int pid);
	void ReleaseSharedLock(int pid);
	bool AcquireExclusiveLock(int pid);
	void ReleaseExclusiveLock(int pid);

private:
	WakeUpListStatus wakeUpListStatus;
};

public ref class LockManager {
public:
	static Dictionary<int, ReadWriteFIFOLock^>^ lockTable =
		gcnew Dictionary<int, ReadWriteFIFOLock^> ();

	static void testNew(int oid)
	{
		Monitor::Enter(lockTable);

		if (!lockTable->ContainsKey(oid))
		{
			lockTable->Add(oid,  gcnew ReadWriteFIFOLock(oid));
		}

		Monitor::Exit(lockTable);
	}

	//--------------------------------------------------------------------
	// LockManager::AcquireSharedLock
	//
	// Input    : identifier of the requesting process (pid)
	//			  identifier of the object to be locked (oid)
	// Purpose  : Acquire a shared lock. Adds lock entry in lock table.
	// Return   : True, if lock is Acquired. False, otherwise.
	//--------------------------------------------------------------------
	static bool AcquireSharedLock(int pid, int oid)
	{
		testNew(oid);
		return lockTable[oid]->AcquireSharedLock(pid);
	}

	//--------------------------------------------------------------------
	// LockManager::ReleaseSharedLock
	//
	// Input    : identifier of the requesting process (pid)
	//			  identifier of the object to be locked (oid)
	// Purpose  : Release a shared lock.
	// Return   : None.
	//--------------------------------------------------------------------
	static void ReleaseSharedLock(int pid, int oid)
	{
		testNew(oid);
		lockTable[oid]->ReleaseSharedLock(pid);
	}

	//--------------------------------------------------------------------
	// LockManager::AcquireExclusiveLock
	//
	// Input    : identifier of the requesting process (pid)
	//			  identifier of the object to be locked (oid)
	// Purpose  : Acquire an exclusive lock. Adds lock entry in lock table.
	// Return   : True, if lock is Acquired. False, otherwise.
	//--------------------------------------------------------------------
	static bool AcquireExclusiveLock(int pid, int oid)
	{
		testNew(oid);
		return lockTable[oid]->AcquireExclusiveLock(pid);
	}

	//--------------------------------------------------------------------
	// LockManager::ReleaseExclusiveLock
	//
	// Input    : identifier of the requesting process (pid)
	//			  identifier of the object to be locked (oid)
	// Purpose  : Release an exclusive lock.
	// Return   : None.
	//--------------------------------------------------------------------
	static void ReleaseExclusiveLock(int pid, int oid)
	{
		testNew(oid);
		lockTable[oid]->ReleaseExclusiveLock(pid);
	}

	//--------------------------------------------------------------------
	// LockManager::AcquireSharedIndexLock
	//
	// Input    : identifier of the requesting process (pid)
	// Purpose  : Acquire a shared lock on the hash index. Assumes a single index.
	// Return   : None.
	//-------------------------------------------------------------------
	static void AcquireSharedIndexLock(int pid)
	{
		AcquireSharedLock(pid, -1);
	}

	//--------------------------------------------------------------------
	// LockManager::ReleaseSharedIndexLock
	//
	// Input    : identifier of the requesting process (pid)
	// Purpose  : Release a shared lock on the hash index. Assumes a single index.
	// Return   : None.
	//--------------------------------------------------------------------
	static void ReleaseSharedIndexLock(int pid)
	{
		ReleaseSharedLock(pid, -1);
	}

	//--------------------------------------------------------------------
	// LockManager::AcquireExclusiveIndexLock
	//
	// Input    : identifier of the requesting process (pid)
	// Purpose  : Acquire an exclusive lock on the hash index. Assumes a single index.
	// Return   : None.
	//-------------------------------------------------------------------
	static void AcquireExclusiveIndexLock(int pid)
	{
		AcquireExclusiveLock(pid, -1);
	}

	//--------------------------------------------------------------------
	// LockManager::ReleaseExclusiveIndexLock
	//
	// Input    : identifier of the requesting process (pid)
	// Purpose  : Release an exclusive lock on the hash index. Assumes a single index.
	// Return   : None.
	//-------------------------------------------------------------------
	static void ReleaseExclusiveIndexLock(int pid)
	{
		ReleaseExclusiveLock(pid, -1);
	}
};

#endif // _LOCK_H