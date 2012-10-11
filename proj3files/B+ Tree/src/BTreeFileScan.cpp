#include "minirel.h"
#include "bufmgr.h"
#include "db.h"
#include "new_error.h"
#include "BTreeFile.h"
#include "BTreeFileScan.h"

//-------------------------------------------------------------------
// BTreeFileScan::BTreeFileScan
//
// Input   : None
// Output  : None
// Purpose : Constructs a new BTreeFileScan object. Note that this constructor
//           is private and can only be called from
//-------------------------------------------------------------------
BTreeFileScan::BTreeFileScan()
{
	low = NULL;
	high = NULL;
	current_leaf = NULL;
	current_key = NULL;
}


//-------------------------------------------------------------------
// BTreeFileScan::~BTreeFileScan
//
// Input   : None
// Output  : None
// Purpose : Clean Up the B+ tree scan.
//-------------------------------------------------------------------
BTreeFileScan::~BTreeFileScan()
{
	if (low != NULL) {
		delete [] low;
	}
	if (high != NULL) {
		delete [] high;
	}
	if (current_key != NULL) {
		delete [] current_key;
	}
}


//-------------------------------------------------------------------
// BTreeFileScan::GetNext
//
// Input   : None
// Output  : rid  - record id of the scanned record.
//           keyPtr - and a pointer to it's key value.
// Purpose : Return the next record from the B+-tree index.
// Return  : OK if successful, DONE if no more records to read
//           or if high key has been passed.
//-------------------------------------------------------------------
Status BTreeFileScan::GetNext(RecordID &rid, char *&keyPtr)
{
	// current_page is still pinned!
	/*CASE: First "GetNext" Call, with low == NULL*/
	if (low == NULL && current_key == NULL) {
		if (current_leaf->GetMinKeyValue(keyPtr, rid) == FAIL) {
			return DONE;
		}
		else {
			//Update the currently done key and record
			current_key = new char[MAX_KEY_LENGTH];
			memcpy(current_key, keyPtr, sizeof(keyPtr));
			current_record = rid;
			return OK;
		}
	}
	//TODO lots of other cases...
	//if (strcmp(current_key, current_leaf->GetMaxKey
	//current_leaf->
	// TODO: Add your code here.
	return FAIL;
}


//-------------------------------------------------------------------
// BTreeFileScan::DeleteCurrent
//
// Input   : None
// Output  : None
// Purpose : Delete the entry currently being scanned (i.e. returned
//           by previous call of GetNext()). Note that this method should
//           call delete on the page containing the previous key, but it
//           does (and should) NOT need to redistribute or merge keys.
// Return  : OK
//-------------------------------------------------------------------
Status BTreeFileScan::DeleteCurrent()
{
	return current_leaf->Delete(current_key, current_record);
}