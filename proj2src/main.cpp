#include <stdlib.h>
#include <iostream>

#include "heappagetest.h"
#include "heapfiletest.h"
#include "heappage.h"

int MINIBASE_RESTART_FLAG = 0;

int main(int argc, char **argv)
{
	HeapFileDriver hd;
	Status dbstatus;

	// Runs tests at heap file level.
	dbstatus = hd.RunTests();

	if (dbstatus != OK) {       
		cout << "Error encountered during hfpage tests: " << endl;
		minibase_errors.show_errors();
	}

	cout << "Please press any key to run page level tests" << endl;
	cin.get();

	// Run tests at heap page level.
	HeapPageDriver hpd;
	hpd.RunTests();

	cin.get();
	return(0);
}
