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

		//TRAVERSAL waitQ
		for each (Request^ req in lock->waitQ)
		{
			//if (lock->objectID == 1) {
				//Console::WriteLine("req " + req->pid + " is waiting for lock " + lock->objectID);
			//}
			//TRAVERSAL lockingList
			for each (int pid in lock->lockingList)
			{
				//if (lock->objectID == 1) {
					//Console::WriteLine("req " + pid + " is has a lock for " + lock->objectID);
				//}
				//the request on the waitQ is waiting for every lock on the lockingList
				//this includes exclusive reqs at the front of the queue, and the shared request(s) after
				if ((req->pid) != pid) {
					//Console::WriteLine("In BuildWaitFor: " + req->pid + " is waiting for " + pid);
					waitFor[req->pid][pid] = true;
				}
				//else {
					//Console::WriteLine("In BuildWaitFor Equalpid: " + req->pid + " is waiting for " + pid);
				//}
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

	bool seenLocalT[maxT]; //keep track of the transactions we have traversed from this currentT
	bool seenGlobalT[maxT]; //keep track of the transactions we have traversed total
	for (int i = 0; i < maxT; i++) {
		seenGlobalT[i] = false;
	}

	//stack for breadth/depth first search
	LinkedList<int>^ traverseList = gcnew LinkedList<int>();

	for (int i = 0; i < maxT; i++) {
		if (abortT[i] || seenGlobalT[i]) {
			continue; //don't need to analyze if the transactions already aborted
			//or seen in a prior traversal of a component
		}
		for (int j = 0; j < maxT; j++) {
			seenLocalT[j] = false; //reset the currentT's seen transactions -> it's a new iteration
		}
		// add this transaction to the /stack
		traverseList->AddFirst(i);

		// while the queue/stack is not empty
		while (traverseList->Count != 0) {
			// pop something off the queue/stack, set seenT of it to true

			int currentT = traverseList->First->Value;
			traverseList->RemoveFirst();
			// try to find a cycle -- cycle detected if you bump into transaction that's already seen
			if (seenLocalT[currentT] && !abortT[currentT]) {
				//simple abort protocol -- just delete the immediate one we found in the cycle
				//Console::WriteLine("Aborting " + currentT);
				abortT[currentT] = true;
				while (traverseList->Remove(currentT)) {
					//remove until it's all gone
				}
				continue;
			}
			else if (seenLocalT[currentT]) { //transaction was already aborted
				//continue - don't need to add adjacent edges for this aborted transaction
				continue;
			}
			seenLocalT[currentT] = true;

			// Push all neighbors to the front of the stack
			for (int j = 0; j < maxT; j++) {
				//don't need to connect wait if it's been aborted already or
				//already cleared of deadlocks globally
				if (waitFor[currentT][j] && !abortT[j] && !seenGlobalT[j]) {
					//Console::WriteLine("In AnalyzeWaitFor: " + currentT + " is waiting for " + j);
					traverseList->AddFirst(j);
				}
			}
		}
		//Update globally seen nodes
		for (int j = 0; j < maxT; j++) {
			if (seenLocalT[j]) { 
				seenGlobalT[j] = true;
			}
		}
	}
	//Console::WriteLine("-----");
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
		Console::WriteLine("no deadlock found");
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
