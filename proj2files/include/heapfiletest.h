#ifndef _HEAP_FILE_DRIVER_H_
#define _HEAP_FILE_DRIVER_H_

#include "test.h"

// This is a driver class for running tests on heap file.
class HeapFileDriver : public TestDriver
{
public:

	HeapFileDriver();
	~HeapFileDriver();

	Status RunTests();

private:
	int choice;

	bool Test1();
	bool Test2();
	bool Test3();
	bool Test4();
	bool Test5();

	Status RunAllTests();
	const char* TestName();
};

#endif
