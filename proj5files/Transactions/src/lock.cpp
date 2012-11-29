#include "lock.h"

ReadWriteFIFOLock::ReadWriteFIFOLock(int oid)
{
	_m = gcnew Object();
	sharedLockCount = 0;
	exclusiveLockCount = 0;
	wakeUpListStatus = EMPTY;
	wakeUpList = gcnew List<int>();
	lockingList = gcnew List<int>();
	waitQ = gcnew LinkedList<Request^>();
	objectID = oid;
	abortList = gcnew List<int>();
}


bool ReadWriteFIFOLock::AcquireSharedLock(int pid)
{
	Monitor::Enter(_m);

	if ((waitQ->Count > 0) || (exclusiveLockCount != 0) || wakeUpListStatus == XLIST)
	{
		Console::WriteLine("waitQ count: " + waitQ->Count + " exclusiveLockCount: " + exclusiveLockCount);
		Console::WriteLine("waitQ1: " + waitQ->First->Value->pid + " waitQ2: " + waitQ->First->Next->Value->pid);
		Request^ r = gcnew Request(pid, SHARED);
		waitQ->AddLast(r);

		while (true)
		{
			Monitor::Wait(_m);

			if (wakeUpList->Contains(pid))
			{
				wakeUpList->Remove(pid);
				if (wakeUpList->Count == 0) {
					wakeUpListStatus = EMPTY;
				}
				break;
			}
		}
	}


	if (abortList->Contains(pid)) {

		abortList->Remove(pid);

		for each(Request^ r in waitQ)
		{
			if (r->pid == pid)
			{
				waitQ->Remove(r);
				break;
			}
		}

		while (wakeUpList->Contains(pid)) {
			wakeUpList->Remove(pid);
		}
		
		if (wakeUpList->Count == 0) {
			wakeUpListStatus = EMPTY;
		}

		if (PRINTLOG) {
			Console::WriteLine("p" + pid + " FAIL to get shared lock for " + objectID);
		}

		Monitor::Exit(_m);
		return false;
	}
	else {
		lockingList->Add(pid);
		sharedLockCount++;

		if (PRINTLOG)
			if (objectID == -1) {
				Console::WriteLine("p" + pid + " get shared index lock");
			} else {
				Console::WriteLine("p" + pid + " get shared lock for " + objectID);
			}

		Monitor::Exit(_m);
		return true;
	}
}

void ReadWriteFIFOLock::ReleaseSharedLock(int pid)
{
	Monitor::Enter(_m);

	lockingList->Remove(pid);
	sharedLockCount--;

	if ((sharedLockCount == 0) && (waitQ->Count > 0) && (wakeUpListStatus == EMPTY))
	{
		//release an exclusive lock
		Request^ t = waitQ->First->Value;
		waitQ->RemoveFirst();

		if (t->type == SHARED)
		{
			Console::WriteLine("Unexpected shared lock request by " + t->pid + " for " + objectID + " (" + pid + ")");
		}

		if (wakeUpList->Count != 0)
		{
			Console::WriteLine("Wake up list is not empty when releasing the last shared lock");
		}

		wakeUpListStatus = XLIST;
		wakeUpList->Add(t->pid);
		Monitor::PulseAll(_m);
	}
	else if ((sharedLockCount == 1) && (waitQ->Count > 0) && (wakeUpListStatus == EMPTY))
	{
		Request^ r = waitQ->First->Value;

		if ((lockingList[0] == r->pid) && (r->type == EXCLUSIVE))
		{
			//ready to upgrade, return to AcquireExclusiveLock function
			waitQ->RemoveFirst();
			wakeUpListStatus = XLIST;
			wakeUpList->Add(r->pid);
			Monitor::PulseAll(_m);
		}
	}

	if (PRINTLOG)
		if (objectID == -1) {
			Console::WriteLine("p" + pid +" release shared index lock");
		} else {
			Console::WriteLine("p" + pid +" release shared lock for " + objectID);
		}

	Monitor::Exit(_m);
}

bool ReadWriteFIFOLock::AcquireExclusiveLock(int pid)
{
	Monitor::Enter(_m);

	//directly upgradeable?
	if ((sharedLockCount == 1) && (wakeUpListStatus == EMPTY))
	{
		if (lockingList[0] == pid)
		{
			sharedLockCount--;
			exclusiveLockCount++;

			if (PRINTLOG) {
				Console::WriteLine("p" + pid +" get exclusive lock for " + objectID);
			}

			Monitor::Exit(_m);
			return true;
		}
	}

	if ((waitQ->Count > 0) || (exclusiveLockCount != 0) || (sharedLockCount !=0) || (wakeUpListStatus != EMPTY))
	{
		Request^ r = gcnew Request(pid, EXCLUSIVE);

		if (lockingList->Contains(pid))
		{
			//upgrade
			waitQ->AddFirst(r);
		}
		else
		{
			waitQ->AddLast(r);
		}

		while (true)
		{
			Monitor::Wait(_m);

			if (wakeUpList->Contains(pid))
			{
				wakeUpListStatus = EMPTY;
				wakeUpList->Remove(pid);
				break;
			}
		}
	}

	if (abortList->Contains(pid)) {

		abortList->Remove(pid);

		for each(Request^ r in waitQ)
		{
			if (r->pid == pid)
			{
				waitQ->Remove(r);
				break;
			}
		}

		while (wakeUpList->Contains(pid)) {
			wakeUpList->Remove(pid);
		}

		if (wakeUpList->Count == 0) {
			wakeUpListStatus = EMPTY;	
		}

		if (PRINTLOG) {
			Console::WriteLine("p" + pid + " FAIL to get exclusive lock for " + objectID);
		}

		Monitor::Exit(_m);
		return false;
	}
	else {
		if (lockingList->Contains(pid))
		{
			//upgrade
			sharedLockCount--;
		}
		else
		{
			lockingList->Add(pid);
		}

		exclusiveLockCount++;

		if (PRINTLOG)
			if (objectID == -1) {
				Console::WriteLine("p" + pid + " get exclusive index lock");
			} else {
				Console::WriteLine("p" + pid + " get exclusive lock for " + objectID);
			}

		Monitor::Exit(_m);
		return true;
	}
}

void ReadWriteFIFOLock::ReleaseExclusiveLock(int pid)
{
	Monitor::Enter(_m);

	lockingList->Remove(pid);
	exclusiveLockCount--;

	if ((waitQ->Count > 0) && (wakeUpListStatus == EMPTY))
	{
		Request^ t = waitQ->First->Value;

		if (t->type == EXCLUSIVE)
		{
			wakeUpListStatus = XLIST;
			wakeUpList->Add(t->pid);
			waitQ->RemoveFirst();
		}
		else
		{
			while (t->type == SHARED)
			{
				wakeUpListStatus = SLIST;
				wakeUpList->Add(t->pid);
				waitQ->RemoveFirst();

				if (waitQ->Count == 0) {
					break;
				}

				t = waitQ->First->Value;
			}
		}

		Monitor::PulseAll(_m);
	}

	if (PRINTLOG)
		if (objectID == -1) {
			Console::WriteLine("p" + pid +" release exclusive index lock");
		} else {
			Console::WriteLine("p" + pid +" release exclusive lock for " + objectID);
		}

	Monitor::Exit(_m);
}
