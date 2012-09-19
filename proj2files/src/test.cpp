#include <stdlib.h>
#include <stdio.h>
#include <sys/types.h>
#include <string.h>
#include <errno.h>
#include <iostream>
#include <assert.h>
#include <process.h>

using namespace std;

#include "new_error.h"
#include "test.h"

TestDriver::TestDriver( const char* nameRoot )
{
	unsigned len = (unsigned int)strlen(nameRoot);
	char basename[200];
	char dbfname[200];
	char logfname[200];

	sprintf( basename, "%s%ld", nameRoot, _getpid() );
	sprintf( dbfname, "%s.minibase-db", basename );
	sprintf( logfname, "%s.minibase-log", basename );

	dbpath = strdup(dbfname);
	logpath = strdup(logfname);
}

TestDriver::~TestDriver()
{
	::free(dbpath);
	::free(logpath);
}

bool TestDriver::Test1()
{
	return true;
}

bool TestDriver::Test2()
{
	return true;
}

bool TestDriver::Test3()
{
	return true;
}

bool TestDriver::Test4()
{
	return true;
}

bool TestDriver::Test5()
{
	return true;
}

const char* TestDriver::TestName()
{
	return "*** unknown ***";
}

void TestDriver::TestFailure( Status& status, Status expectedStatus,
	const char* activity, int postedErrExpected )
{
	if ( status == OK )
	{
		status = FAIL;
		cerr << "*** " << activity << " did not return a failure status.\n";
	}
	else
	{
		status = OK;
		cout << "    --> Failed as expected\n";
	}

	if ( status != OK )
		minibase_errors.show_errors();
	minibase_errors.clear_errors();
}

Status TestDriver::RunTests()
{
	cout << "\nRunning " << TestName() << " tests...\n" << endl;

	char* newdbpath;
	char* newlogpath;

	newdbpath = new char[ strlen(dbpath) + 20];
	newlogpath = new char[ strlen(logpath) + 20];
	strcpy(newdbpath,dbpath); 
	strcpy(newlogpath, logpath);

	sprintf(newdbpath, "%s", dbpath);
	sprintf(newlogpath, "%s", logpath);

	unlink( newdbpath );
	unlink( newlogpath );

	minibase_errors.clear_errors();

	// Run the tests.
	Status answer = RunAllTests();
	minibase_errors.clear_errors();

	cout << "\n..." << TestName() << " tests "
		<< (answer == OK ? "completed successfully" : "failed")
		<< ".\n\n";

	delete newdbpath; delete newlogpath;

	return answer;
}

Status TestDriver::RunAllTests()
{
	Status status = OK;
	int result;
	int i;
	const int inTxtLen = 32;
	char *inputTxt = new char[inTxtLen];

	cout << "Input a space separated test sequence (ie. a list of numbers " << endl <<
		" in the range 1-5: 1 5 2 3) or hit ENTER to run all tests: ";

	cin.getline ( inputTxt, inTxtLen );
	if ( strlen(inputTxt) == 0 )
	{
		inputTxt = "12345";
	}	
	for ( i = 0; i < (int)strlen(inputTxt); i++)
	{
		switch ( inputTxt[i] )
		{
		case '1' : 
			minibase_errors.clear_errors();
			result = Test1();
			if ( !result || minibase_errors.error() )
			{
				status = FAIL;
				if ( minibase_errors.error() )
					cerr << (result? "*** Unexpected error(s) logged, test failed:\n"
					: "Errors logged:\n");
				minibase_errors.show_errors(cerr);
			}
			break;
		case '2' :
			minibase_errors.clear_errors();
			result = Test2();
			if ( !result || minibase_errors.error() )
			{
				status = FAIL;
				if ( minibase_errors.error() )
					cerr << (result? "*** Unexpected error(s) logged, test failed:\n"
					: "Errors logged:\n");
				minibase_errors.show_errors(cerr);
			}
			break;
		case '3' :
			minibase_errors.clear_errors();
			result = Test3();
			if ( !result || minibase_errors.error() )
			{
				status = FAIL;
				if ( minibase_errors.error() )
					cerr << (result? "*** Unexpected error(s) logged, test failed:\n"
					: "Errors logged:\n");
				minibase_errors.show_errors(cerr);
			}    
			break;
		case '4' :
			minibase_errors.clear_errors();
			result = Test4();
			if ( !result || minibase_errors.error() )
			{
				status = FAIL;
				if ( minibase_errors.error() )
					cerr << (result? "*** Unexpected error(s) logged, test failed:\n"
					: "Errors logged:\n");
				minibase_errors.show_errors(cerr);
			}
			break;
		case '5' :
			minibase_errors.clear_errors();
			result = Test5();
			if ( !result || minibase_errors.error() )
			{
				status = FAIL;
				if ( minibase_errors.error() )
					cerr << (result? "*** Unexpected error(s) logged, test failed:\n"
					: "Errors logged:\n");
				minibase_errors.show_errors(cerr);
			}
			break;
		}
	}
	return status;
}
