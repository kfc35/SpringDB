#ifndef _DataPage_H
#define _DataPage_H

#include "page.h"
#include "new_error.h"
#include "da_types.h"
#include "HashIndex.h"
#include "minirel.h"

struct DataRecord {
	KeyType key;
	DataType value;
};

// The data page interprets the page as a key-value store 
// and maintains the data and the metadata. 
class DataPage : public Page {

public:
	Status InsertKeyValue(KeyType key, DataType value);
	Status DeleteKey(KeyType key);
	Status GetValue(KeyType key, DataType &value); //Returns fail is key does not exist
	Status initializeDataPage();
	bool IsEmpty();
	bool IsFull();
	Status GetFirstKey(KeyType &);
	Status GetOverFlowPageID(PageID &);
	Status SetOverFlowPageID(PageID);

	// Distributes the data in the page between the current page
	// and its mirror page at a higher-level.
	Status SplitPage(DataPage *dp, int level);

	Status GetNumberOfRecord(int &numRecords);
	Status SetNumberOfRecord(int numberOfRecords);

	void PrintPage();

	//EXTRA CREDIT
	Status MergePage(DataPage *dp);
};


#endif