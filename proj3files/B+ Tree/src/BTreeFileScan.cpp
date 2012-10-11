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
	currentIsDirty = false;
	current_key = NULL;
	current_record_index = -1;
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
	/*CASE: High key has been passed*/
	if (strcmp(current_key, high) > 0) {
		return DONE;
	}
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
			current_record_index = 0;
			current_record = rid;
			return OK;
		}
	}
	/*CASE: First "GetNext" Call, with low != null, current_key == null*/
	else if (current_key == NULL) {
		//Initialize the current_key to the low key
		current_key = new char[MAX_KEY_LENGTH];
		memcpy(current_key, low, sizeof(low));

		//Search for the key in this page
		PageKVScan<RecordID> kvscan;
		//Find the next valid key if low = current_key DNE in leaf page
		while (current_leaf->Search(current_key, kvscan) == FAIL) {
			/*Unpin the current page, Pin the next page and set it to current*/
			PageID next_leaf_pg_id = current_leaf->GetNextPage();
			MINIBASE_BM->UnpinPage(current_leaf->PageNo(), currentIsDirty);
			if (next_leaf_pg_id == INVALID_PAGE) {
				current_leaf = NULL;
				break;
			}
			Page *next_leaf = (Page *)current_leaf;
			MINIBASE_BM->PinPage(next_leaf_pg_id, next_leaf);
			current_leaf = (LeafPage *)next_leaf;
		}
		if (current_leaf == NULL) { //no more records
			return DONE;
		}
		//Iterate through kvscan to get the next valid key, set current_key
		//If the next valid key > high key, print done
	}
	/*CASE: Arbitrary "GetNext" Call when low == null and current_key != null*/
	else if (low == NULL) {
		//current key is definitely valid.
	}
	/*CASE: Arbitrary "GetNext" call when low and current_key != null*/
	else {
		//current key is definitely valid.
	}
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