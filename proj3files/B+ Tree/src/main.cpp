#include <iostream>
#include <ctime>

#include "SortedKVPage.h"
#include "InteractiveBTreeTest.h"

int MINIBASE_RESTART_FLAG = 0;

void printHelp()
{
	cout << "Commands should be of the form:"<<endl;
	cout << "insert <low> <high>"<<endl;
	cout << "scan <low> <high>"<<endl;
	cout <<"\tIf <low> == -1 then the scan will start at the first record in the tree" << endl;
	cout <<"\tIf <high> == -1 then the scan will end at the last record in the tree" << endl;
	cout << "test <testnum>"<<endl;
	cout << "\tTest 1: Test a tree with single leaf." << endl;
	cout << "\tTest 2: Test inserts with leaf splits." << endl;
	cout << "\tTest 3: Test inserts with index splits." << endl;
	cout << "\tTest 4: Test a large workload." << endl;
	cout << "\tTest 5: Test that everything gets unpinned from the buffer pool." << endl;
	cout << "\tTest 6: Test that delete current works." << endl;
	cout << "print"<<endl;
	cout << "quit (not required)"<<endl;
}


int btreeTestManual()
{
	cout << "btree tester with manual input" <<endl;
	printHelp();

	InteractiveBTreeTest btt;
	Status dbstatus = btt.RunTests(cin);

	if (dbstatus != OK) {
		cout << "Error encountered during btree tests: " << endl;
		minibase_errors.show_errors();
		return 1;
	}

	return 0;
}


int main(int argc, char *argv[])
{
	int ret = btreeTestManual();

	std::cout << "Hit [enter] to continue..." << endl;
	std::cin.get();

	return ret;
}

