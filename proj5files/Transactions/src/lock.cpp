#include "lock.h"

ReadWriteFIFOLock::ReadWriteFIFOLock(int oid)
{
	_m = gcnew Object();
	sharedLockCount = 0;
	exclusiveLockCount = 0;
	wakeUpList = gcnew List<int>();
	lockingList = gcnew List<int>();
	waitQ = gcnew LinkedList<Request^>();
	objectID = oid;
	abortList = gcnew List<int>();
}


bool ReadWriteFIFOLock::AcquireSharedLock(int pid)
{
	Monitor::Enter(_m);

	if ((waitQ->Count > 0) || (exclusiveLockCount != 0))
	{
		Request^ r = gcnew Request(pid, SHARED);
		waitQ->AddLast(r);

		while (true)
		{
			Monitor::Wait(_m);

			if (wakeUpList->Contains(pid))
			{
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
				Console::WriteLine("p" + pid + " Acquire shared lock for " + objectID);
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

	if ((sharedLockCount == 0) && (waitQ->Count > 0) && (wakeUpList->Count == 0))
	{
		//Release an exclusive lock
		Request^ t = waitQ->First->Value;
		waitQ->RemoveFirst();

		if (t->type == SHARED)
		{
			Console::WriteLine("Unexpected shared lock request by" + t->pid);
		}

		if (wakeUpList->Count != 0)
		{
			Console::WriteLine("Wake up list is not empty when releasing the last shared lock");
		}

		wakeUpList->Add(t->pid);
		Monitor::PulseAll(_m);
	}
	else if ((sharedLockCount == 1) && (waitQ->Count > 0))
	{
		Request^ r = waitQ->First->Value;

		if ((lockingList[0] == r->pid) && (r->type == EXCLUSIVE))
		{
			//ready to upgrade, return to AcquireExclusiveLock function
			waitQ->RemoveFirst();
			wakeUpList->Add(r->pid);
			Monitor::PulseAll(_m);
		}
	}

	if (PRINTLOG)
		if (objectID == -1) {
			Console::WriteLine("p" + pid +" Release shared index lock");
		} else {
			Console::WriteLine("p" + pid +" Release shared lock for " + objectID);
		}

	Monitor::Exit(_m);
}

bool ReadWriteFIFOLock::AcquireExclusiveLock(int pid)
{
	Monitor::Enter(_m);

	//directly upgradeable?
	if ((sharedLockCount == 1))
	{
		if (lockingList[0] == pid)
		{
			sharedLockCount--;
			exclusiveLockCount++;

			if (PRINTLOG) {
				Console::WriteLine("p" + pid +" Acquire exclusive lock for " + objectID);
			}

			Monitor::Exit(_m);
			return true;
		}
	}

	if ((waitQ->Count > 0) || (exclusiveLockCount != 0) || (sharedLockCount !=0))
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

		if (PRINTLOG) {
			Console::WriteLine("p" + pid + " FAIL to Acquire exclusive lock for " + objectID);
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
				Console::WriteLine("p" + pid + " Acquire exclusive index lock");
			} else {
				Console::WriteLine("p" + pid + " Acquire exclusive lock for " + objectID);
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

	if ((waitQ->Count > 0) && (wakeUpList->Count == 0))
	{
		if (wakeUpList->Count != 0)
		{
			Console::WriteLine("Wake up list is not empty when releasing the last exclusive lock");
		}

		Request^ t = waitQ->First->Value;

		if (t->type == EXCLUSIVE)
		{
			wakeUpList->Add(t->pid);
			waitQ->RemoveFirst();
		}
		else
		{
			while (t->type == SHARED)
			{
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
			Console::WriteLine("p" + pid +" Release exclusive index lock");
		} else {
			Console::WriteLine("p" + pid +" Release exclusive lock for " + objectID);
		}

	Monitor::Exit(_m);
}
