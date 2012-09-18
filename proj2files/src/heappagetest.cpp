//*****************************************
//  Driver program for heappages
//****************************************

#include <stdio.h>
#include <stdlib.h>
#include <iostream>
#include <string.h>
#include <stdlib.h>
#include <assert.h>

#include "db.h"
#include "heapfile.h"
#include "scan.h"
#include "heappagetest.h"
#include "bufmgr.h"

using namespace std;

static const int namelen = 24;

#define ASSERT(b) if(!(b)) {std::cout << "Failed at line " << __LINE__ << std::endl; return false;} 

struct Rec
{
	int ival;
	double fval;
	char name[namelen];
};

static const int reclen = sizeof(Rec);

HeapPageDriver::HeapPageDriver() : TestDriver( "hftest" )
{}

HeapPageDriver::~HeapPageDriver()
{}

Status HeapPageDriver::RunTests()
{
	return TestDriver::RunTests();
}

Status HeapPageDriver::RunAllTests()
{
	return TestDriver::RunAllTests();
}

const char* HeapPageDriver::TestName()
{
	return "Heap Page";
}

// A sample test case provided to you. 
bool HeapPageDriver::Test1()
{
	int SlotSize = 4;
	HeapPage hp;
	hp.Init(0);

	ASSERT(hp.AvailableSpace() == HEAPPAGE_DATA_SIZE);

	char rec[5] = "abcd";
	const int RecordLen = 5;
	RecordID* recordIds = new RecordID[100];
	// Insert 100 records.
	for (int i = 0; i < 100; i++) {
		ASSERT(hp.InsertRecord(rec, RecordLen, recordIds[i]) == OK);
	}

	ASSERT(hp.AvailableSpace() == HEAPPAGE_DATA_SIZE - SlotSize * 100 - RecordLen * 100);

	// Delete all even records
	for (int i = 0; i < 100; i++) {
		if (i % 2 == 0) {
			ASSERT(hp.DeleteRecord(recordIds[i]) == OK);
		}
	}

	RecordID recId;
	RecordID prevRecId;
	int deleteEveryTwo = 0;
	while(hp.InsertRecord(rec, RecordLen, recId) == OK) {
		deleteEveryTwo++;
		if (deleteEveryTwo % 3 == 0) {
			hp.DeleteRecord(prevRecId);
		}
		prevRecId = recId;
	}

	ASSERT(hp.AvailableSpace() == 8);
	ASSERT(hp.GetNumOfRecords() == 452);
	return true;
}

bool HeapPageDriver::Test2()
{
	// Add your tests here.
	return true;
}

bool HeapPageDriver::Test3()
{
	// Add your tests here.
	return true;
}

bool HeapPageDriver::Test4()
{
	// Add your tests here.
	return true;
}

bool HeapPageDriver::Test5()
{
	// Add your tests here.
	return true;
}