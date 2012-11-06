
#include <string.h>
#include <stdio.h>
#include <stdlib.h>
#include <memory.h>

#include "heapfile.h"
#include "scan.h"

#include "Sort.h"

//-------------------------------------------------------------------
// Sort::CreateTempFilename
//
// Input   : file_name,	The output file name of the sorting task.
//			 pass,		The number of passes (assuming the sort phase is pass 0).
//			 run,		The run number within the pass.
// Output  : None.
// Return  : The temporary file name
// Example : File 7 in pass 3 for output file FOO will be named FOO.sort.temp.3.7.
// Note    : It is your responsibility to destroy the return filename string.
//-------------------------------------------------------------------
char *Sort::CreateTempFilename(char *filename, int pass, int run)
{
	char *name = new char[strlen(filename) + 20];
	sprintf(name,"%s.sort.temp.%d.%d", filename, pass, run);
	return name;
}

Status Sort::PassZero(int &numTempFiles) 
{
	// Open the unsorted heapfile
	Status result = OK;
	HeapFile *file = new HeapFile(_inFile, result);
	if (result != OK) {
		std::cerr << "Heap File cannot be opened\n";
		delete file; //TODO do we have to call a destructor?
		return FAIL;
	}
	// Open a scan to get all the records
	Scan *filescan = file->OpenScan(result);
	if (result != OK) {
		std::cerr << "Scan cannot be opened\n";
		delete file;
		delete filescan;
		return FAIL;
	}

	int pass = 0;
	int run = 0;
	
	/* The runs will be of length _numBufPages */
	// filescan->GetNext
	for (int i = 1; i <= _numBufPages; i++) {
		//fill all the pages with unsorted records.
		
		//after all the pages are filled or theres no more records, qsort the run.

		//create a temp file with the records
	}
}

Sort::Sort(
	char		*inFile,		// Name of unsorted heapfile.
	char		*outFile,		// Name of sorted heapfile.
	int      	numFields,		// Number of fields in input records.
	AttrType 	fieldTypes[],	// Array containing field types of input records.
	// i.e. index of in[] ranges from 0 to (len_in - 1)
	short    	fieldSizes[],	// Array containing field sizes of input records.
	int       	sortKeyIndex,	// The number of the field to sort on.
	// fld_no ranges from 0 to (len_in - 1).
	TupleOrder 	sortOrder,		// ASCENDING, DESCENDING
	int       	numBufPages,	// Number of buffer pages available for sorting.
	Status 	&s)
{
	/*Initialize private variables so that private functions can access them*/
	_inFile = inFile;
	_outFile = outFile;
	_fieldSizes = fieldSizes;
	_sortKeyIndex = sortKeyIndex;
	_numBufPages = numBufPages;
	//TODO calculate _recLength? it's sum of fieldSizes + size of an id, or just sum of fieldSizes?
	_recLength = 0;
	for (int i = 0; i < numFields; i++) {
		_recLength += fieldSizes[i];
	}

	//Do Pass Zero - includes opening the file, reading in records, sorting them into runs.
	int numTempFiles;
	Status passZeroStatus = PassZero(numTempFiles);
	if (passZeroStatus == FAIL) {
		s = FAIL;
		return ;
	}
}


