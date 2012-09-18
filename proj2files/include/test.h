#ifndef _TEST_DRIVER_H_
#define _TEST_DRIVER_H_

#include "minirel.h"

// This class is driver class for tests from which other test drivers can be derived.
class TestDriver
{
public:
	// This runs a series of tests.  If it returns OK, then everything worked
	// as advertised.  Otherwise, there were errors reported.
	virtual Status RunTests();

protected:
	// This constructs the tester.  You supply a name like "dbtest" as the
	// root of the database and log file names.
	TestDriver( const char* nameRoot );
	virtual ~TestDriver();

	char* dbpath;
	char* logpath;

	// Subclasses override these tests
	virtual bool Test1();
	virtual bool Test2();
	virtual bool Test3();
	virtual bool Test4();
	virtual bool Test5();

	// ...and this method, which is printed as the kind of test being done,
	// for example "Disk Space Management".
	virtual const char* TestName();

	void TestFailure( Status& status, Status expectedStatus,
		const char* activity, int postedErrExpected = true );

	virtual Status RunAllTests();
};

#endif
