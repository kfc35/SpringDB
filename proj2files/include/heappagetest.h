#ifndef _HEAP_PAGE_DRIVER_H_
#define _HEAP_PAGE_DRIVER_H_

#include "test.h"

// This is a driver class for running tests on heap file.
class HeapPageDriver : public TestDriver
{
public:

	HeapPageDriver();
	~HeapPageDriver();

	Status RunTests();

private:

	bool Test1();
	bool Test2();
	bool Test3();
	bool Test4();
	bool Test5();

	Status RunAllTests();
	const char* TestName();
};

#endif
