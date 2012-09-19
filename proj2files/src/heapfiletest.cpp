//*****************************************
//  Driver program for heapfiles
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
#include "heapfiletest.h"
#include "bufmgr.h"

using namespace std;

static const int namelen = 24;

struct Rec
{
	int ival;
	double fval;
	char name[namelen];
};

static const int reclen = sizeof(Rec);

HeapFileDriver::HeapFileDriver() : TestDriver( "hftest" )
{
	choice = 100; // big enough for file to occupy >1 page
}

HeapFileDriver::~HeapFileDriver()
{}


Status HeapFileDriver::RunTests()
{
	return TestDriver::RunTests();
}

Status HeapFileDriver::RunAllTests()
{
	Status answer;
	minibase_globals = new SystemDefs(answer,"MINIBASE.DB", "MINIBASE.LOG",
		100,500,100,"Clock");
	if ( answer == OK )
		answer = TestDriver::RunAllTests();


	delete minibase_globals;
	return answer;
}

const char* HeapFileDriver::TestName()
{
	return "Heap File";
}


//*****************************************************
//***	Test 1: Insert and scan fixed-size records	***
bool HeapFileDriver::Test1()
{
	cout << "\n  Test 1: Insert and scan fixed-size records\n";
	Status status = OK;
	RecordID rid;

	//	Initalize the heap file
	cout << "  - Create a heap file\n";
	HeapFile f("file_1", status);

	if (status != OK)
		cerr << "*** Could not create heap file\n";
	else if ( MINIBASE_BM->GetNumOfUnpinnedBuffers() != MINIBASE_BM->GetNumOfBuffers() )
	{
		cerr << "*** The heap file has left pages pinned\n";
		status = FAIL;
	}

	if ( status == OK )
	{
		//	Insert records into the HeapFile
		cout << "  - Add " << choice << " records to the file\n";
		for (int i =0; i<choice && status == OK; i++)
		{
			Rec rec = { i, i*2.5 };
			sprintf(rec.name, "record %i",i);

			status = f.InsertRecord((char *)&rec, reclen, rid);

			if (status != OK)
				cerr << "*** Error inserting record " << i << endl;
			else if ( MINIBASE_BM->GetNumOfUnpinnedBuffers() != MINIBASE_BM->GetNumOfBuffers() )
			{
				cerr << "*** Insertion left a page pinned\n";
				status = FAIL;
			}
		}

		//	Check the total number of records
		if ( f.GetNumOfRecords() != choice )
		{
			status = FAIL;
			cerr << "*** File reports " << f.GetNumOfRecords() << " records, not "
				<< choice << endl;
		}
	}

	//	Prepare to scan the records in the HeapFile
	Scan* scan = 0;
	if ( status == OK )
	{
		cout << "  - Scan the records just inserted\n";
		scan = f.OpenScan(status);

		if (status != OK)
			cerr << "*** Error opening scan\n";
		else if ( MINIBASE_BM->GetNumOfUnpinnedBuffers() == MINIBASE_BM->GetNumOfBuffers() )
		{
			cerr << "*** The heap-file scan has not pinned the first page\n";
			status = FAIL;
		}
	}

	//	Scan the records in heap file and check them.
	//	The scan must return the insertion order.
	if ( status == OK )
	{
		int len, i = 0;
		Rec rec;

		//	State the length of allocated memory
		len = sizeof(rec);

		//	Scan the next record
		while ( (status = scan->GetNext(rid, (char *)&rec, len)) == OK )
		{
			//	Check the record
			if ( len != reclen )
			{
				cerr << "*** Record " << i << " had unexpected length " << len
					<< endl;
				status = FAIL;
				break;
			}
			else if ( MINIBASE_BM->GetNumOfUnpinnedBuffers() == MINIBASE_BM->GetNumOfBuffers() && i != choice - 1)
			{
				cerr << "*** The heap-file scan has not left its page pinned\n";
				status = FAIL;
				break;
			}
			char name[ sizeof rec.name ];
			sprintf( name, "record %i", i );
			if( rec.ival != i  ||
				rec.fval != i*2.5  ||
				0 != strcmp( rec.name, name ) )
			{
				cerr << "*** Record " << i << " differs from what we inserted\n";
				status = FAIL;
				break;
			}
			++i; len = sizeof(rec);
		}

		//	Finish scan
		if ( status == DONE )
		{
			if ( MINIBASE_BM->GetNumOfUnpinnedBuffers() != MINIBASE_BM->GetNumOfBuffers() )
				cerr << "*** The heap-file scan has not unpinned its page after finishing\n";
			else if ( i == choice )
				status = OK;
			else
				cerr << "*** Scanned " << i << " records instead of "
				<< choice << endl;
		}
	}

	delete scan;

	if ( status == OK )
		cout << "  Test 1 completed successfully.\n";
	return (status == OK);
}


//*****************************************************
//***	Test 2: Delete fixed-size records			***
bool HeapFileDriver::Test2()
{
	cout << "\n  Test 2: Delete fixed-size records\n";
	Status status = OK;
	Scan* scan = 0;
	RecordID rid;

	//	Open the heap file created by test case 1
	cout << "  - Open the same heap file as Test 1\n";
	HeapFile f("file_1", status);

	if (status != OK)
		cerr << "*** Error opening heap file\n";

	if ( status == OK )
	{
		cout << "  - Delete half the records\n";
		scan = f.OpenScan(status);
		if (status != OK)
			cerr << "*** Error opening scan\n";
	}

	//	Delete half of the records
	if ( status == OK )
	{
		int len, i = 0;
		Rec rec;

		//	State the length of allocated memory
		len = sizeof(rec);
		while ( (status = scan->GetNext(rid, (char *)&rec, len)) == OK )
		{
			if ( i & 1 )        // Delete the odd-numbered ones.
			{
				status = f.DeleteRecord( rid );
				if ( status != OK )
				{
					cerr << "*** Error deleting record " << i << endl;
					break;
				}
			}
			++i; len = sizeof(rec);
		}

		if ( status == DONE )
			status = OK;
	}

	delete scan;
	scan = 0;

	if ( status == OK
		&& MINIBASE_BM->GetNumOfUnpinnedBuffers() != MINIBASE_BM->GetNumOfBuffers() )
	{
		cerr << "*** Deletion left a page pinned\n";
		status = FAIL;
	}

	if ( status == OK )
	{
		cout << "  - Scan the remaining records\n";
		scan = f.OpenScan(status);
		if (status != OK)
			cerr << "*** Error opening scan\n";
	}
	//	Check the remaining records
	if ( status == OK )
	{
		int len, i = 0;
		Rec rec;

		//	State the length of allocated memory
		len = sizeof(rec);
		cout << "Before get next";
		while ( (status = scan->GetNext(rid, (char *)&rec, len)) == OK )
		{
			//	Check retrieved record
			if( rec.ival != i  ||
				rec.fval != i*2.5 )
			{
				cout << "*** Record " << i << " differs from what we "
					"inserted\n";
				cerr << "*** Record " << i << " differs from what we "
					"inserted\n";
				status = FAIL;
				break;
			}
			i += 2; // Because we deleted the odd ones
			len = sizeof(rec);
		}
		cout << "After get next";

		if ( status == DONE )
			status = OK;
	}

	delete scan;

	if ( status == OK )
		cout << "  Test 2 completed successfully.\n";
	return (status == OK);
}


//*****************************************************
//***	Test 3: Update fixed-size records			***
bool HeapFileDriver::Test3()
{
	cout << "\n  Test 3: Update fixed-size records\n";
	Status status = OK;
	Scan* scan = 0;
	RecordID rid;

	// Open heap file
	cout << "  - Open the same heap file as tests 1 and 2\n";
	HeapFile f("file_1", status);

	if (status != OK)
		cerr << "*** Error opening heap file\n";


	if ( status == OK )
	{
		cout << "  - Change the records\n";
		scan = f.OpenScan(status);
		if (status != OK)
			cerr << "*** Error opening scan\n";
	}
	if ( status == OK )
	{
		int len, i = 0;
		Rec rec;

		// State the length of allocated memory
		len = sizeof(rec);
		while ( (status = scan->GetNext(rid, (char *)&rec, len)) == OK )
		{
			rec.fval = (float)7*i; // We'll check that i==rec.ival below.
			status = f.UpdateRecord( rid, (char*)&rec, len );	
			if ( status != OK )
			{
				cerr << "*** Error updating record " << i << endl;
				break;
			}
			i += 2; // Recall, we deleted every other record above.
			len = sizeof(rec);
		}

		if ( status == DONE )
			status = OK;
	}

	delete scan;
	scan = 0;

	if ( status == OK
		&& MINIBASE_BM->GetNumOfUnpinnedBuffers() != MINIBASE_BM->GetNumOfBuffers() )
	{
		cerr << "*** Updating left pages pinned\n";
		status = FAIL;
	}

	if ( status == OK )
	{
		cout << "  - Check that the updates are really there\n";
		scan = f.OpenScan(status);
		if (status != OK)
			cerr << "*** Error opening scan\n";
	}

	// Check whether the records are really updated
	if ( status == OK )
	{
		int len, i = 0;
		Rec rec, rec2;

		len = sizeof(rec);
		while ( (status = scan->GetNext(rid, (char *)&rec, len)) == OK )
		{
			// While we're at it, Test the getRecord method too.
			status = f.GetRecord( rid, (char*)&rec2, len );
			if ( status != OK )
			{
				cerr << "*** Error getting record " << i << endl;
				break;
			}
			if( rec.ival != i || rec.fval != i*7
				|| rec2.ival != i || rec2.fval != i*7 )
			{
				cerr << "*** Record " << i << " differs from our "
					"update\n";
				status = FAIL;
				break;
			}
			i += 2; // Because we deleted the odd ones
			len = sizeof(rec);
		}

		if ( status == DONE )
			status = OK;
	}

	delete scan;

	if ( status == OK )
		cout << "  Test 3 completed successfully.\n";
	return (status == OK);
}


//*********************************************************************
//***	Test 4: Temporary heap files and variable-length records	***
bool HeapFileDriver::Test4()
{
	cout << "\n  Test 4: Temporary heap files and variable-length records\n";
	Status status = OK;
	Scan* scan = 0;
	RecordID rid;

	cout << "  - Create a temporary heap file\n";

	HeapFile f( 0, status );

	if (status != OK)
		cerr << "*** Error creating temp file\n";


	if ( status == OK )
		cout << "  - Add variable-sized records\n";

	unsigned recSize = MINIBASE_PAGESIZE / 2;
	/* We use half a page as the starting size so that a single page won't
	hold more than one record.  We add a series of records at this size,
	then halve the size and add some more, and so on.  We store the index
	number of each record on the record itself. */
	unsigned numRecs = 0;
	char record[MINIBASE_PAGESIZE] = "";
	for ( ; recSize >= (sizeof numRecs + sizeof recSize) && status == OK;
		recSize /= 2 )
		for ( unsigned i=0; i < 10 && status == OK; ++i, ++numRecs )
		{
			memcpy( record, &numRecs, sizeof numRecs );
			memcpy( record+(sizeof numRecs), &recSize, sizeof recSize );
			status = f.InsertRecord( record, recSize, rid );
			if ( status != OK )
				cerr << "*** Failed inserting record " << numRecs
				<< ", of size " << recSize << endl;
		}


		int *seen = new int[numRecs];
		for (int i = 0; i < (int)numRecs; i++)
		{
			seen[i] = false;
		}

		memset( seen, 0, sizeof seen );
		if ( status == OK )
		{
			cout << "  - Check that all the records were inserted\n";
			scan = f.OpenScan(status);
			if (status != OK)
				cerr << "*** Error opening scan\n";
		}
		if ( status == OK )
		{
			int len;

			while (true) 
			{
				//	State the length of allocated memory
				len = sizeof(char) * 4096;
				if ((status = scan->GetNext(rid, record, len)) != OK )
					break;

				unsigned recNum;
				memcpy( &recNum, record, sizeof recNum );

				//	Check duplicate records
				if ( seen[recNum])
				{
					cerr << "*** Found record " << recNum << " twice!\n";
					status = FAIL;
					break;
				}
				seen[recNum] = true;

				//	Check size
				memcpy( &recSize, record + sizeof recNum, sizeof recSize );
				if ( recSize != (unsigned)len )
				{
					cerr << "*** Record size mismatch: stored " << recSize
						<< ", but retrieved " << len << endl;
					status = FAIL;
					break;
				}

				--numRecs;
			}

			if ( status == DONE )
				if ( numRecs )
					cerr << "*** Scan missed " << numRecs << " records\n";
				else
					status = OK;
		}

		delete scan;

		if ( status == OK )
			cout << "  Test 4 completed successfully.\n";
		return (status == OK);
}


//*********************************************
//***	Test 5: Test some error conditions	***
bool HeapFileDriver::Test5()
{
	cout << "\n  Test 5: Test some error conditions\n";
	Status status = OK;
	Scan* scan = 0;
	RecordID rid;

	//	Open the heap file
	HeapFile f("file_1", status);
	if (status != OK)
		cerr << "*** Error opening heap file\n";

	if ( status == OK )
	{
		cout << "  - Try to change the size of a record\n";
		scan = f.OpenScan(status);
		if (status != OK)
			cerr << "*** Error opening scan\n";
	}

	//	Try to change the size of a record -- should fail
	if ( status == OK )
	{
		int len;
		Rec rec;
		len = sizeof(rec);
		status = scan->GetNext(rid, (char *)&rec, len);
		if ( status != OK )
			cerr << "*** Error reading first record\n";
		else
		{
			status = f.UpdateRecord( rid, (char*)&rec, len-1 );
			TestFailure( status, HEAPFILE, "Shortening a record" );
			if ( status == OK )
			{
				status = f.UpdateRecord( rid, (char*)&rec, len+1 );
				TestFailure( status, HEAPFILE, "Lengthening a record" );
			}
		}
	}

	delete scan;

	//	Try to insert a too long record -- should fail
	if ( status == OK )
	{
		cout << "  - Try to insert a record that's too long\n";
		char record[MINIBASE_PAGESIZE] = "";
		status = f.InsertRecord( record, MINIBASE_PAGESIZE, rid );
		TestFailure( status, HEAPFILE, "Inserting a too-long record" );
	}

	if ( status == OK )
		cout << "  Test 5 completed successfully.\n";
	return (status == OK);
}
