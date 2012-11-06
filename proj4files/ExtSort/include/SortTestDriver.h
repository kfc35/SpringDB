#ifndef _SORT_TEST_DRIVER_H_
#define _SORT_TEST_DRIVER_H_

#include <iostream>
using namespace std;

#include "minirel.h"

#define MAX_REC_LENGTH 4096

class SortTestDriver
{
public:
	// Test cases
	bool TestSortOnly();

	bool TestOneMerge();

	bool TestMulMerge();

	bool TestRandInt();

	bool TestAll();
};





#endif