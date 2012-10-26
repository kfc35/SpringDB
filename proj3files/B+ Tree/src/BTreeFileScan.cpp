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
	if (current_leaf != NULL) {
		MINIBASE_BM->UnpinPage(((ResizableRecordPage *) current_leaf)->PageNo(), currentIsDirty);
	}
}

/** Advances the current_leaf to the next leaf page if possible**/
void BTreeFileScan::AdvanceCurrentLeaf() {
	/*Unpin the current page, Pin the next page and set it to current*/
	PageID next_leaf_pg_id = current_leaf->GetNextPage();
	MINIBASE_BM->UnpinPage(current_leaf->PageNo(), currentIsDirty);
	if (next_leaf_pg_id == INVALID_PAGE) {
		current_leaf = NULL;
		return;
	}
	Page *next_leaf = (Page *)current_leaf;
	MINIBASE_BM->PinPage(next_leaf_pg_id, next_leaf);
	current_leaf = (LeafPage *)next_leaf;
	currentIsDirty = false;
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
	/*CASE: No more records to read*/
	if (current_leaf == NULL) {
		return DONE;
	}
	/*CASE: high key has been passed*/
	if (high != NULL && current_key != NULL) {
		if (strcmp(current_key, high) > 0) {
			return DONE;
		}
	}
	// current_page is still pinned!
	/*CASE: First "GetNext" Call, with low == NULL*/
	if (low == NULL && current_key == NULL) {
		if (current_leaf->GetMinKey(keyPtr) == FAIL) {
			return DONE;
		}
		else {
			//Update the currentkey and record
			current_key = new char[MAX_KEY_LENGTH];
			strcpy(current_key, keyPtr);

			//Initialize the base scan.
			current_leaf->Search(current_key, current_scan); //this will not fail; key exists.
			current_scan.GetNext(keyPtr, current_record); //position the scan past the first entry
			rid = current_record;

			return OK;
		}
	}
	/*CASE: First "GetNext" Call, with low != null, current_key == null*/
	else if (current_key == NULL) {
		//Initialize the current_key to the low key
		current_key = new char[MAX_KEY_LENGTH];
		strcpy(current_key, low);

		//Find the next valid key if low = current_key DNE in this leaf page
		if (current_leaf->Search(current_key, current_scan) == FAIL) {
			//low is TOO low. just get the min key because it is higher than the low
			if (current_leaf->GetMinKey(keyPtr) == FAIL) {
				return DONE;
			}
			strcpy(current_key, keyPtr);
			current_leaf->Search(current_key, current_scan); //WILL be valid.
			/*AdvanceCurrentLeaf();
			if (current_leaf == NULL) { //no more records
				return DONE;
			}*/
		}
		
		//Iterate through the current_scan to get the next valid key
		current_scan.GetNext(keyPtr, current_record); //must be at least one to initialize
		//keyPtr will always be above low?
		/*while (strcmp(low, keyPtr) > 0) {
			//This can unfortunately happen (kv pairs run out on the page)
			if (current_scan.GetNext(keyPtr, current_record) == DONE) {
				AdvanceCurrentLeaf();
				if (current_leaf == NULL) {
					return DONE;
				}

				//update the scan to go through this leaf
				current_leaf->Search(current_key, current_scan);
			}
		}*/
		//Advancing has gone past the high key
		if (high != NULL) {
			if (strcmp(keyPtr, high) > 0) {
				return DONE;
			}
		}
		strcpy(current_key, keyPtr);
		rid = current_record;
		return OK;
	}
	/*CASE: Arbitrary "GetNext" Call when current_key != null*/
	else {
		//current_key and current_scan are definitely valid
		if (current_scan.GetNext(keyPtr, current_record) == DONE) {
			AdvanceCurrentLeaf();
			if (current_leaf == NULL) {
				return DONE;
			}

			//Get the minimum key of the new page
			if (current_leaf->GetMinKey(keyPtr) == FAIL) {
				return DONE;
			}
			strcpy(current_key, keyPtr);

			//Initialize the base scan.
			current_leaf->Search(current_key, current_scan); //this will not fail; key exists.
			current_scan.GetNext(keyPtr, current_record); //position the scan past the first entry
		}
		strcpy(current_key, keyPtr);
		//Advancing has gone past the high key
		if (high != NULL) {
			if (strcmp(keyPtr, high) > 0) {
				return DONE;
			}
		}
		rid = current_record;
		return OK;
	}
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
	currentIsDirty = true;
	return current_leaf->Delete(current_key, current_record);
}