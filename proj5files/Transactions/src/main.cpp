// Compile using /clr option.
#include "lock.h"
#include "transaction.h"
#include "new_error.h"
#include "system_defs.h"
#include "minirel.h"
#include "page.h"
#include "frame.h"
#include "db.h"
#include "da_types.h"
#include "clock.h"
#include "bufmgr.h"
#include "HashIndex.h"
#include "threadsafe_HashIndex.h"
#include "deadlock_detector.h"
#include "test.h"

using namespace System;

int MINIBASE_RESTART_FLAG = 0;

int main()
{
	Status status;
	int bufSize = NUMBUF;
	minibase_globals = new SystemDefs(status, "MINIBASE.DB", 2000, bufSize, "Clock");
	HashIndex *HI = new HashIndex(MINIBASE_BM, INITNUMBUCKETS);

	DeadlockDetector^ d = gcnew DeadlockDetector();
	Thread^ oThreadd = gcnew Thread(gcnew ThreadStart(d,  &DeadlockDetector::run));
	oThreadd->Start();

	// IMP : TEST CASES MUST BE RUN SEPARATELY ONE AT A TIME.
	test1(HI);
	//test2(HI);
	//test3(HI);
	//test4(HI);

	Console::ReadLine();
	return 0;
}

