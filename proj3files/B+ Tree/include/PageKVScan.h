#ifndef _PAGE_KV_SCAN_
#define _PAGE_KV_SCAN_

template<typename ValType> class SortedKVPage;


template<typename ValType>
class PageKVScan
{
public:
	friend class SortedKVPage<ValType>;

	//-------------------------------------------------------------------
	// PageKVScan::GetNext
	//
	// Input   : None.
	// Output  : key, Pointer to current key.
	//           val, Pointer to current value.
	// Return  : OK   if successful.
	//           DONE if there are no more key-value pairs on this page
	// Purpose : Retrieves the next key-value pair on this page.
	//-------------------------------------------------------------------
	Status GetNext(char *&key, ValType &val) {
		if (state == INVALID_SCAN || state == END) {
			return DONE;
		}

		// If at the beginning, get the first record
		if (state == BEGIN) {
			// Set deleted to false since the pointer is advancing
			curDeleted = false;
			// There are no records, so return done
			if (page->IsEmpty()) {
				return DONE;
			}
			// Set the state and current record to the first one
			state = MID;
			curRid.slotNo = 0;
			setKey(curRid);
		} else {
			/* Advance to the next value only if the last value was not deleted.
			 * Advancement is automatic on deletion because the curValNum
			 * now points to the following value
			 */
			if (!curDeleted) {
				curValNum++;
			}

			// Set deleted to false since the pointer is advancing
			curDeleted = false;

			// All values for this record are exhausted
			if (curValNum == numValsWithKey) {
				// If all vals for a key were deleted, the key has already
				// moved forward one spot. Otherwise increment the slot number.
				if (numValsWithKey > 0) {
					curRid.slotNo += 1;
				}

				// This is the last record on the page, so return DONE
				if (curRid.slotNo == page->GetNumOfRecords()) {
					state = END;
					return DONE;
				}

				setKey(curRid);
			}
		}

		key = curKey;
		val = GetVal(curKey, curValNum);
		return OK;
	}

	//-------------------------------------------------------------------
	// PageKVScan::GetPrev
	//
	// Input   : None.
	// Output  : key, Pointer to previous key.
	//           val, Pointer to previous value.
	// Return  : OK   if successful.
	//           DONE if there are no previous key-value pairs on this page.
	//                Note that this will not return the GetPrevPage pointer
	//                on index pages!
	// Purpose : Retrieves the previous key-value pair on this page.
	//-------------------------------------------------------------------
	Status GetPrev(char *&key, ValType &val) {
		if (state == INVALID_SCAN || state == BEGIN) {
			return DONE;
		}

		// Set deleted to false since the pointer is moving
		curDeleted = false;

		if (state == END) {
			// There are no records, so return done
			if (page->IsEmpty()) {
				return DONE;
			}
			// Set the state and current record to the first one
			state = MID;
			curRid.slotNo = page->GetNumOfRecords() - 1;
			setKey(curRid, true);
		} else {
			// Move back one value
			curValNum--;
			//All values in the record are exhausted
			if (curValNum < 0) {
				// Move to the new key
				curRid.slotNo--;

				// This was the first record on the page, so return done
				if (curRid.slotNo < 0) {
					state = BEGIN;
					return DONE;
				}
				setKey(curRid, true);
			}
		}

		key = curKey;
		val = GetVal(curKey, curValNum);
		return OK;
	}
	
	//-------------------------------------------------------------------
	// PageKVScan::DeleteCurrent
	//
	// Input   : None.
	// Output  : None.
	// Return  : OK   if successful.
	//           DONE if there have been no calls to GetNext or GetPrev,
	//				  or the element returned by the last call to GetNext
	//				  or GetPrev has already been deleted,
	//                or the last call returned DONE
	//           FAIL if the underlying call to Delete failed.
	// Purpose : Deletes the "current" key-value pair, i.e. the one returned
	// 	         by the last call to GetNext or GetPrev. After this has been called.
	//           The "cursor" will be reset to immediately before the deleted
	//           keyValue pair.
	//-------------------------------------------------------------------
	Status DeleteCurrent() {
		if (curDeleted || state != MID) {
			return DONE;
		}

		char *keyToDelete = curKey;
		ValType valToDelete = GetVal(curKey, curValNum);
		
		if (page->Delete(keyToDelete, valToDelete) == FAIL) {
			return FAIL;
		}

		numValsWithKey--;
		curDeleted = true;
		return OK;
	}

private:
	enum IteratorState {
		BEGIN,
		MID,
		END,
		INVALID_SCAN
	};
	SortedKVPage<ValType> *page;
	enum IteratorState state;
	RecordID curRid;
	int curValNum, numValsWithKey;
	char *curKey;
	bool curDeleted;

	/* Private method that sets the iterator variables using rid.
	 * Sets the iterator to the first value for the key if prev is
	 * false or the last value for the key if prev is true.
	 */
	void setKey(RecordID rid, bool prev = false) {
		int recLen;
		page->ReturnRecord(rid, curKey, recLen);

		int keyLength = strlen(curKey) + 1;

		assert(((recLen - keyLength) % sizeof(ValType)) == 0);
		numValsWithKey = (recLen - keyLength) / sizeof(ValType);

		if (prev) {
			curValNum = numValsWithKey - 1;
		} else {
			curValNum = 0;
		}
	}

	// Initializes the iterator. Should only be called by methods in SortedKVPage.
	void reset(SortedKVPage<ValType> *page, RecordID rid) {
		assert(rid.pageNo == page->PageNo());
		this->page = page;
		curRid = rid;

		if (page->IsEmpty() || rid.slotNo < 0 || rid.slotNo >= page->GetNumOfRecords()) {
			state = INVALID_SCAN;
		} else {
			curDeleted = true;
			state = MID;
			setKey(rid);
		}
	}
	
	ValType GetVal(char *key, int valNum) {
		return *((ValType *)(curKey + strlen(key) + 1 + valNum * sizeof(ValType)));
	}
};

#endif