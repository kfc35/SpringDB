#ifndef _LATCH_H
#define _LATCH_H

#using <System.dll>
using namespace System;
using namespace System::Threading;
using namespace System::Collections::Generic;

#define PRINTLOG 0

public ref class LatchRequest {
public:
	int pid;

	LatchRequest(int pid)
	{
		this->pid = pid;
	}
};

public ref class FIFOLatch {

	Object^ _m; //monitor ojbect
	int objectID;
	int LatchCount;
	List<int>^ wakeUpList;
	List<int>^ latchingList;
	Queue<LatchRequest^>^ waitQ;

public:
	FIFOLatch(int oid);

	void AcquireLatch(int pid);
	void ReleaseLatch(int pid);
};

public ref class LatchManager
{
	static Dictionary<int, FIFOLatch^>^ latchTable =
		gcnew Dictionary<int, FIFOLatch^> ();

	static void testNew(int oid)
	{
		Monitor::Enter(latchTable);

		if (!latchTable->ContainsKey(oid))
		{
			latchTable->Add(oid,  gcnew FIFOLatch(oid));
		}

		Monitor::Exit(latchTable);
	}


public:

	static void AcquireLatch(int pid, int oid)
	{
		testNew(oid);
		latchTable[oid]->AcquireLatch(pid);
	}

	static void ReleaseLatch(int pid, int oid)
	{
		testNew(oid);
		latchTable[oid]->ReleaseLatch(pid);
	}
};

#endif // _LATCH_H