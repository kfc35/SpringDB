#include "latch.h"

FIFOLatch::FIFOLatch(int oid)
{
	_m = gcnew Object();
	LatchCount = 0;
	wakeUpList = gcnew List<int>();
	latchingList = gcnew List<int>();
	waitQ = gcnew Queue<LatchRequest^>();
	objectID = oid;
}


void FIFOLatch::AcquireLatch(int pid)
{
	Monitor::Enter(_m);

	if ((waitQ->Count > 0) || (LatchCount != 0))
	{
		LatchRequest^ r = gcnew LatchRequest(pid);
		waitQ->Enqueue(r);

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

	latchingList->Add(pid);
	LatchCount++;

	if (PRINTLOG) {
		Console::WriteLine("p" + pid +" get latch for bucket " + objectID);
	}

	Monitor::Exit(_m);
}

void FIFOLatch::ReleaseLatch(int pid)
{
	Monitor::Enter(_m);

	latchingList->Remove(pid);
	LatchCount--;

	if ((waitQ->Count > 0) && (wakeUpList->Count == 0))
	{
		if (wakeUpList->Count != 0)
		{
			Console::WriteLine("Wake up list is not empty when releasing the last exclusive lock");
		}

		LatchRequest^ t = waitQ->Peek();
		t = waitQ->Dequeue();
		wakeUpList->Add(t->pid);

		Monitor::PulseAll(_m);
	}

	if (PRINTLOG) {
		Console::WriteLine("p" + pid +" Release latch for bucket " + objectID);
	}

	Monitor::Exit(_m);
}
