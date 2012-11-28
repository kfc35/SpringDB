#include "deadlock_detector.h"
#include <vector>

#define maxT 50 //maxNumOfCurrentTransaction

bool abortT[maxT];

//POSSIBLE DATA STRUCTURE FOR WAIT-FOR GRAPH
bool waitFor[maxT][maxT];

void DeadlockDetector::BuildWaitForGraph()
{
	for each(KeyValuePair<int, ReadWriteFIFOLock^> kvp in LockManager::lockTable)
	{
		int oid = kvp.Key;
		ReadWriteFIFOLock^ lock = kvp.Value;

		// Make sure Monitor::Exit(lock->_m) is called for every Monitor::Enter
		// (so watch out if you use "continue" or "break")
		Monitor::Enter(lock->_m);

		//BEGIN OF TODO

		//Data structure to store all the requests that wants an exclusive lock
		LinkedList<int>^ exclusiveList = gcnew LinkedList<int>();

		//TRAVERSAL waitQ
		for each (Request^ req in lock->waitQ)
		{
			//if the type is for exclusive, add it do the data structure
			if (req->type == EXCLUSIVE) {
				exclusiveList->AddFirst(req->pid);
			}
			else {
				break; //you can break out since exclusives are purely at the front
			}
		}

		//TRAVERSAL lockingList
		for each (int pid in lock->lockingList)
		{
			//for each request that wants an exclusive lock
				//set waitsFor[request][pid] = true
			for each (int exid in exclusiveList) {
				waitFor[exid][pid] = true;
			}
		}

		/*
		if (lock->waitQ->Count > 0)
		{
			//FIRST ELEMENT OF waitQ
			Request^ req = lock->waitQ->First->Value;
							
			if (lock->waitQ->Count > 1)
			{
				//SECOND ELEMENT OF waitQ
				Request^ r = lock->waitQ->First->Next->Value;
			}
		}
		*/

		//END OF TODO

		Monitor::Exit(lock->_m);
	}
}

void DeadlockDetector::AnalyzeWaitForGraph()
{
	// TODO : Add your code here.

	bool seenT[maxT]; //keep track of the transactions we have traversed total
	for (int i = 0; i < maxT; i++) {
		seenT[i] = false;
	}

	//stack for breadth/depth first search
	LinkedList<int>^ traverseList = gcnew LinkedList<int>();

	for (int i = 0; i < maxT; i++) {
		if (abortT[i] || seenT[i]) {
			continue; //don't need to analyze if the transactions already aborted
			//or seen in a prior traversal of a transaction
		}
		// add this transaction to the /stack
		traverseList->AddFirst(i);

		// while the queue/stack is not empty
		while (!traverseList.empty()) {
			// pop something off the queue/stack, set seenT of it to true
			int currentT = traverseList->First();
			traverseList->RemoveFirst(i);
			seenT[currentT] = true;
			
			// try to find a cycle -- cycle detected if you bump into transaction that's already seen that HASNT been aborted yet
			// remove the appropriate transaction with the least pid or whatever you think should happen... set abortT for it to true
			// for now, we will just abort the "already seen" transaction
		}
	}
}


void DeadlockDetector::AbortTransactions()
{
	//DO NOT CHANGE ANY CODE HERE
	bool deadlock = false;

	for (int i = 0; i < maxT; ++i)
		if (abortT[i])
		{
			deadlock = true;
			break;
		}

	if (!deadlock) {
		//Console::WriteLine("no deadlock found");
	}
	else {
		for (int i = 0; i < maxT; ++i)
			if (abortT[i])
			{
				Console::WriteLine("DD: Abort Transaction: " + i);
			}

		for each(KeyValuePair<int, ReadWriteFIFOLock^> kvp in LockManager::lockTable)
		{
			int oid = kvp.Key;
			ReadWriteFIFOLock^ lock = kvp.Value;

			Monitor::Enter(lock->_m);

			bool pall = false;

			for each(Request^ req in lock->waitQ)
				if (abortT[req->pid])
				{
					lock->abortList->Add(req->pid);
					lock->wakeUpList->Add(req->pid);
					Console::WriteLine("DD: Transaction " + req->pid + " cancel object " + oid);
					pall = true;
				}

			if (pall)
			{
				Monitor::PulseAll(lock->_m);
			}

			Monitor::Exit(lock->_m);
		}
	}
}

void DeadlockDetector::run()
{
	while (true)
	{
		Thread::Sleep(timeInterval);

		//BEGIN OF TODO
		memset(waitFor, 0 ,sizeof(waitFor));
		//INITIALIZE ANY OTHER DATA STRUCTURES YOU DECLARE.

		//Clear all entries in the abort and wait for arrays
		for (int i = 0; i < maxT; i++) {
			abortT[i] = false;
			for (int j = 0; j < maxT; j++) {
				waitFor[i][j] = false;
			}
		}

		//END OF TODO

		memset(abortT, 0 ,sizeof(abortT));

		Monitor::Enter(LockManager::lockTable);

		BuildWaitForGraph();

		AnalyzeWaitForGraph();

		AbortTransactions();

		Monitor::Exit(LockManager::lockTable);
	}
}
