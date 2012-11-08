
#include <string.h>
#include <stdio.h>
#include <stdlib.h>
#include <memory.h>

#include "heapfile.h"
#include "scan.h"

#include "Sort.h"

// GLOBAL VARIABLE - what kind of comparison you should make in compare()
AttrType attributeTypeToCompare;
// GLOBAL VARIABLE - offset into the record of where the attribute to compare is
int attributeOffset;
// GLOBAL VARIABLE - the length of the value in the field to compare
int attributeLength;


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



//-------------------------------------------------------------------
// Compare function for qsort
// returns 0 if a and b are equal
// <0 if a comes before b
// >0 if a comes after b
//-------------------------------------------------------------------
int compare(const void *a, const void *b) 
{
	switch(attributeTypeToCompare) {
		/*Locate the specified attribute within the char arrays and compare. */

		case attrString:
			char *aString = new char[attributeLength];
			char *bString = new char[attributeLength];
			//copy from where the to-compare attribute starts and finishes
			memcpy(aString, &(((char *)a)[attributeOffset]), attributeLength);
			memcpy(bString, &(((char *)b)[attributeOffset]), attributeLength);
			int result = strcmp(aString, bString);
			delete [] aString;
			delete [] bString;
			return result;

		case attrInteger:
			int *aInt = (int *) new char[attributeLength]; //or just new int[1]?
			int *bInt = (int *) new char[attributeLength]; //attributeLength and Offset are in bytes
			memcpy((char *)aInt, &(((char *)a)[attributeOffset]), attributeLength);
			memcpy((char *)bInt, &(((char *)b)[attributeOffset]), attributeLength);
			int result = *aInt - *bInt;
			delete [] aInt;
			delete [] bInt;
			return result;

		default:
			std::cerr << "Attribute type in compare not supported.\n";
			exit(EXIT_FAILURE);
	}
}

//-------------------------------------------------------------------
// Private function to transfer the given sorted memory into a heap file
//-------------------------------------------------------------------
Status Sort::TransferToHeapFile(char *unsortedMemory, int run, int numElements) {
	qsort(unsortedMemory, numElements, _recLength, compare);
	//Insert the contiguous memory into a new heap file.
	char *tempFile = CreateTempFilename(_inFile, 0, run);
	Status result;
	HeapFile *temp = new HeapFile(tempFile, result);
	if (result != OK) {
		std::cerr << "Temp Heap File cannot be created in PassZero\n";
		delete [] tempFile;
		delete temp;
		return FAIL;
	}
	RecordID rid;
	for (int i = 0; i < numElements; i++) {
		if (temp->InsertRecord(&unsortedMemory[i * _recLength], _recLength, rid) == FAIL) {
			std::cerr << "Could not insert into Temp Heap File in PassZero\n";
			delete [] tempFile;
			delete temp;
			return FAIL;
		}
	}
	delete [] tempFile; //our burden to delete it
	delete temp; //temp file is done being written to.
	return OK;
}

Status Sort::PassZero(int &numTempFiles) 
{
	// Open the unsorted heapfile
	Status result = OK;
	HeapFile *file = new HeapFile(_inFile, result);
	if (result != OK) {
		std::cerr << "Heap File cannot be opened\n";
		delete file;
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
	int run = 1;
	
	// allocate contiguous space in memory for inserting records into.
	int numMemory = PAGESIZE * _numBufPages; //in bytes
	char *runMemory = new char[numMemory]; //char is 1 byte
	int startIndex = 0;
	int numElements = 0;

	// continually insert all records into runMemory until full.
	RecordID rid; //just a placeholder.
	char *recPtr;
	int recLen;

	while (filescan->GetNext(rid, recPtr, recLen) != DONE){
		//check whether runMemory can fit the next record
		//when its ==, the memory just fits!
		if ((startIndex + recLen) > numMemory) {
			if (TransferToHeapFile(runMemory, run, numElements) != OK) {
				delete file;
				delete filescan;
				delete [] runMemory;
				return FAIL;
			}

			//reset all the variables, increase run.
			startIndex = 0;
			numElements = 0;
			run++;
		}

		//copy record into memory
		memcpy(&runMemory[startIndex], recPtr, recLen);
		startIndex += recLen;
		numElements++;
	}

	// The last run is not empty (aka the heap file to be sorted was not empty)
	if (startIndex != 0) {
		if (TransferToHeapFile(runMemory, run, numElements) != OK) {
			delete file;
			delete filescan;
			delete [] runMemory;
			return FAIL;
		}
		//don't increase run number here. No more runs after this
	}

	delete file;
	delete filescan;
	delete [] runMemory;

	numTempFiles = run; //run kept track of how many temp files we created.

	return OK;
}

Status PassOneAndBeyond(int numFiles) {
	return OK;
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
	_recLength = 0;
	for (int i = 0; i < numFields; i++) {
		_recLength += fieldSizes[i];
	}

	/*Initialize global variables for the compare function*/
	attributeTypeToCompare = fieldTypes[_sortKeyIndex];
	attributeOffset = 0;
	for (int i = 0; i < _sortKeyIndex; i++) {
		//add all attribute sizes preceding the toCompare attribute
		attributeOffset += fieldSizes[i]; 
	}
	attributeLength = fieldSizes[_sortKeyIndex];

	//Do Pass Zero - includes opening the file, reading in records, sorting them into runs.
	int numTempFiles;
	Status passZeroStatus = PassZero(numTempFiles);
	if (passZeroStatus == FAIL) {
		s = FAIL;
		return ;
	}
	if (numTempFiles == 0) {
		s = OK; //the heap file was empty...
		//TODO create an empty heap file.
		return ;
	}
	while (numTempFiles != 1) {
		//call PassOneAndBeyond repeatedly?
	}
}


