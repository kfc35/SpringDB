#include <cmath>
#include <iostream>
#include <cstdio>
#include <cstdlib>
#include <cassert>
using namespace std;

#include "bufmgr.h"
#include "db.h"

#include "InteractiveBTreeTest.h"

#define MAX_COMMAND_SIZE	1024

//-------------------------------------------------------------------
// ParseInt
//
// Input   : stringValue, A string of characters that we wish to parse
//                        as a signed decimal integer
// Output  : intValue, The value of the parsed integer
// Return  : True if the string was successfully parsed, or false
//           if we encountered an error.
// Purpose : Helper function to InteractiveBTreeTest::RunTests
//-------------------------------------------------------------------
bool ParseInt(const char *stringValue, int &intValue)
{
	char *endPtr;
	long longValue;

	if (strlen(stringValue) >= MAX_KEY_LENGTH) {
		// Input string is too long to properly represent with the default
		// settings for the BTreeDriver::toString function
		cerr << "Value is too large and could cause a buffer overflow: " << stringValue << endl;
		return false;
	}

	if (stringValue[0] == '\0') {
		// Input string is empty
		return false;
	}

	longValue = strtol(stringValue, &endPtr, 10);

	if (*endPtr != '\0') {
		// Couldn't parse the whole string
		return false;
	}

	if (longValue <= INT_MIN || longValue >= INT_MAX) {
		// The parsed value might be too big to be represented
		// as an int.
		cerr << "Value out of range: " << stringValue << endl;
		return false;
	}

	intValue = (int) longValue;
	return true;
}


// Helper function to InteractiveBTreeTest::RunTests
bool GetArgumentAsInt(char **context, int &intValue)
{
	char *argument = strtok_s(NULL, " ", context);

	if (argument == NULL) {
		return false;
	} else {
		return ParseInt(argument, intValue);
	}
}


Status InteractiveBTreeTest::RunTests(istream &in)
{
	// Initialize Minibase and the B+ tree.
	char *dbname="btdb";
	char *logname="btlog";
	char *btfname="BTreeIndex";

	remove(dbname);
	remove(logname);

	Status status;
	minibase_globals = new SystemDefs(status, dbname, logname, 1000, 500, 200);

	if (status != OK) {
		cerr << "ERROR: Couldn'initialize the Minibase globals" << std::endl;
		minibase_errors.show_errors();

		cerr << "Hit [enter] to continue..." << endl;
		cin.get();
		exit(1);
	}

	BTreeFile *btf = createIndex(btfname);

	if (btf == NULL) {
		cerr << "Error: Unable to create index file" << endl;
		minibase_errors.show_errors();

		cerr << "Hit [enter] to continue..." << endl;
		cin.get();
		exit(1);
	}

	// Get and process commands from the user.
	char command[MAX_COMMAND_SIZE];
	cout << "command> ";
	in.getline(command, MAX_COMMAND_SIZE);

	while (in) {
		char *mode = NULL, *arg1 = NULL, *arg2 = NULL, *context = NULL;

		mode = strtok_s(command, " ", &context);

		if (mode != NULL) {
			if (!strcmp(mode, "insert")) {
				int low, high;
				bool inputOK = false;

				if (GetArgumentAsInt(&context, low) && low >= 0) {
					if (GetArgumentAsInt(&context, high) && high >= 0) {
						inputOK = true;
						insertHighLow(btf,low,high);
					}
				}

				if (!inputOK) {
					cout << "Usage: insert <low> <high>" << endl;
				}
			} else if (!strcmp(mode, "scan")) {
				int low, high;
				bool inputOK = false;

				if (GetArgumentAsInt(&context, low) && low >= -1) {
					if (GetArgumentAsInt(&context, high) && high >= -1) {
						inputOK = true;
						scanHighLow(btf,low,high);
					}
				}

				if (!inputOK) {
					cout << "Usage: scan <low> <high>" << endl;
				}
			} else if (!strcmp(mode, "print")) {
				btf->PrintWhole(true);
			} else if (!strcmp(mode, "test")) {
				int testNum;

				if (GetArgumentAsInt(&context, testNum)) {
					bool testSuccess = false;

					switch (testNum) {
					case 1:
						testSuccess = BTreeDriver::TestSinglePage();
						break;
					case 2:
						testSuccess = BTreeDriver::TestInsertsWithLeafSplits();
						break;
					case 3:
						testSuccess = BTreeDriver::TestInsertsWithIndexSplits();
						break;
					case 4:
						testSuccess = BTreeDriver::TestLargeWorkload();
						break;
					default:
						cout << "Unrecognized test: " << testNum << endl;
					}

					if (testSuccess) {
						cout << "PASSED Test " << testNum << endl;
					} else {
						cout << "FAILED Test " << testNum << endl;
					}
				} else {
					cout << "Usage: test <testnum>" << endl;
				}
			} else if (!strcmp(mode, "quit")) {
				// Exit the loop
				break;
			} else {
				cout << "Error: Unrecognized command: "<< command << endl;
			}
		}

		cout << "command> ";
		in.getline(command, MAX_COMMAND_SIZE);
	}

	// Clean up
	destroyIndex(btf, btfname);

	delete minibase_globals;
	remove(dbname);
	remove(logname);

	return OK;
}


BTreeFile *InteractiveBTreeTest::createIndex(char *name)
{
	cout << "Create B+tree." << endl;
	cout << "  Page size=" << MINIBASE_PAGESIZE << " Max space=" << MAX_SPACE << endl;

	Status status;
	BTreeFile *btf = new BTreeFile(status, name);

	if (status != OK) {
		minibase_errors.show_errors();
		cout << "  Error: can not open index file."<<endl;

		if (btf!=NULL) {
			delete btf;
		}

		return NULL;
	}

	cout << "  Success." << endl;
	return btf;
}


void InteractiveBTreeTest::destroyIndex(BTreeFile *btf, char *name)
{
	cout << "Destroy B+tree."<<endl;
	Status status = btf->DestroyFile();

	if (status != OK) {
		minibase_errors.show_errors();
	}

	delete btf;
}


void InteractiveBTreeTest::insertHighLow(BTreeFile *btf, int low, int high)
{

	int numkey=high-low+1;
	cout << "Inserting: (" << low << " to " << high << ")" << endl;

	bool res = BTreeDriver::InsertRange(btf, low, high, lastOffset++);

	if (res) {
		cout << "  Success."<<endl;
	} else {
		cout << "Insert failed." << endl;
	}

}


void InteractiveBTreeTest::scanHighLow(BTreeFile *btf, int low, int high)
{

	cout << "Scanning (" << low << " to " << high << "):" << endl;

	char strLow[MAX_KEY_LENGTH];
	char strHigh[MAX_KEY_LENGTH];
	BTreeDriver::toString(low, strLow);
	BTreeDriver::toString(high, strHigh);

	char *lowPtr = (low == -1) ? NULL : strLow;
	char *highPtr = (high == -1) ? NULL : strHigh;

	BTreeFileScan *scan = btf->OpenScan(lowPtr, highPtr);

	if (scan == NULL) {
		cout << "  Error: cannot open a scan." << endl;
		minibase_errors.show_errors();
		return;
	}

	RecordID rid;
	int count = 0;
	char *skey;

	Status status = scan->GetNext(rid, skey);

	while (status == OK) {
		count++;
		cout << "  Scanned @[pg,slot]=[" << rid.pageNo << "," << rid.slotNo << "]";
		cout << " key=" << skey << endl;
		status = scan->GetNext(rid, skey);
	}

	delete scan;
	cout << "  " << count << " records found." << endl;

	if (status != DONE) {
		minibase_errors.show_errors();
		return;
	}

	cout << "  Success."<<endl;
}
