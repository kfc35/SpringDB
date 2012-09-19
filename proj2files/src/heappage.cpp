#include <iostream>
#include <stdlib.h>
#include <memory.h>

#include "heappage.h"
#include "heapfile.h"
#include "bufmgr.h"
#include "db.h"

using namespace std;

//------------------------------------------------------------------
// Constructor of HeapPage
//
// Input     : Page ID
// Output    : None
//------------------------------------------------------------------
void HeapPage::Init(PageID pageNo)
{
	nextPage = INVALID_PAGE;
	prevPage = INVALID_PAGE;
	numOfSlots = 0; //page is initialized with no slots
	pid = pageNo; //set the pageID of this page to the arg.
	freePtr = HEAPPAGE_DATA_SIZE; //pointer is at the END of the page
	freeSpace = HEAPPAGE_DATA_SIZE; //at beginning, page is empty
}

//------------------------------------------------------------------
// HeapPage::SetNextPage
//
// Input    : The PageID for next page.
// Output   : None.
// Purpose  : Set the PageID for next page.
// Return   : None.
//------------------------------------------------------------------
void HeapPage::SetNextPage(PageID pageNo)
{
	nextPage = pageNo;
}

//------------------------------------------------------------------
// HeapPage::SetPrevPage
//
// Input    : The PageID for previous page.
// Output   : None.
// Purpose  : Set the PageID for previous page.
// Return   : None.
//------------------------------------------------------------------
void HeapPage::SetPrevPage(PageID pageNo)
{
	prevPage = pageNo;
}

//------------------------------------------------------------------
// HeapPage::GetNextPage
//
// Input    : None.
// Output   : None.
// Purpose  : Get the PageID of next page.
// Return   : The PageID of next page.
//------------------------------------------------------------------
PageID HeapPage::GetNextPage()
{
	return nextPage;
}

//------------------------------------------------------------------
// HeapPage::SetPrevPage
//
// Input    : The PageID for previous page.
// Output   : None.
// Purpose  : Get the PageID of previous page.
// Return   : The PageID of previous page.
//------------------------------------------------------------------
PageID HeapPage::GetPrevPage()
{
	return prevPage;
}

//------------------------------------------------------------------
// HeapPage::PageNo
//
// Input    : None.
// Output   : None.
// Purpose  : Get the PageID of this page.
// Return   : The PageID of this page.
//------------------------------------------------------------------
PageID HeapPage::PageNo() 
{
	return pid;
}

//------------------------------------------------------------------
// HeapPage::GetSlotAtIndex
//
// Input    : the slot number.
// Output   : None.
// Purpose  : Get the pointer to the slot at the given slot number in the slot directory.
// Return   : The pointer to the requested slot.
//------------------------------------------------------------------
HeapPage::Slot* HeapPage::GetSlotAtIndex(int slotNumber) { //slot numbers are 0...numOfSlots-1
	if (slotNumber >= numOfSlots) {
		return NULL;
	}
	return (Slot *)(&data[(slotNumber) * sizeof(Slot)]); //array slots are addressed 0 ... n-1
}

//------------------------------------------------------------------
// HeapPage::GetContiguousFreeSpaceSize
//
// Input    : None.
// Output   : None.
// Return   : The size of the contiguous free space region.
//------------------------------------------------------------------
int HeapPage::GetContiguousFreeSpaceSize() {
	//freePtr signals the end of free space in the data array.
	//numOfSlots * sizeof(Slot) returns the beginning of the free space.
	return freePtr - (numOfSlots * sizeof(Slot)); 
}

//------------------------------------------------------------------
// HeapPage::AppendNewSlot
//
// Input    : None.
// Output   : None.
// Purpose  : Increase the size of the slot directory by appending a new slot at the end.
// Return   : A pointer to the slot appended by the function.
//------------------------------------------------------------------
HeapPage::Slot* HeapPage::AppendNewSlot(){
	/*First, check that there's space for it.*/
	if (GetContiguousFreeSpaceSize() < sizeof(Slot)) {
		return NULL;
	}
	else {
		numOfSlots++;
		Slot *newSlot = GetSlotAtIndex(numOfSlots - 1); //the new slot is located at n-1.
		//freeSpace -= sizeof(Slot);
		return newSlot;
	}
}

//------------------------------------------------------------------
// HeapPage::Slot** HeapPage::SortSlotDirectory(Slot** slotDir, int begin, int end)
//
// Input     : The slot directory to be sorted, where to begin sorting, and where to end sorting.
// Output    : None
// Purpose   : Helper Function for CompressPage so that shifting slots is easier.
// Return    : A dynamically created Slot * array, ordered by Slot->offset from Highest to Lowest
//------------------------------------------------------------------
HeapPage::Slot** HeapPage::SortSlotDirectory(Slot** slotDir, int begin, int end) {
	if (begin == end) { //Only one entry.
		Slot **oneSlot;
		oneSlot = new Slot *[sizeof(Slot *)];
		*oneSlot = slotDir[begin];
		return oneSlot;
	}
	else {
		int leftEnd = ((end - begin)/2) + begin;
		int rightBegin = leftEnd + 1;
		//Sort the left side
		Slot** leftSide = SortSlotDirectory(slotDir, begin, leftEnd);

		//Sort the right side
		Slot** rightSide = SortSlotDirectory(slotDir, rightBegin, end);

		//Merge the sides
		Slot** sortedSides;
		sortedSides = new Slot *[(end - begin + 1) * sizeof(Slot *)];

		int pointerLeft = 0;
		int pointerRight = 0;
		int leftSideLength = (leftEnd - begin) + 1;
		int rightSideLength = (end - rightBegin) + 1;
		int sortedIndex = 0;
		//choose the largest between the two and place it first.
		while (pointerLeft < leftSideLength && pointerRight < rightSideLength) {
			if (leftSide[pointerLeft] -> offset >= rightSide[pointerRight] -> offset) {
				sortedSides[sortedIndex] = leftSide[pointerLeft];
				sortedIndex++;
				pointerLeft++;
			}
			else {
				sortedSides[sortedIndex] = rightSide[pointerRight];
				sortedIndex++;
				pointerRight++;
			}
		}
		//If one array is all in the sorted away, put the rest of the entries in the other array
		//into the sorted array.
		if (pointerLeft < leftSideLength) {
			for (int i = pointerLeft; i < leftSideLength; i++) {
				sortedSides[sortedIndex] = leftSide[pointerLeft];
				sortedIndex++;
			}
		}
		if (pointerRight < rightSideLength) {
			for (int i = pointerRight; i < rightSideLength; i++) {
				sortedSides[sortedIndex] = rightSide[pointerRight];
				sortedIndex++;
			}
		}

		/*These sides are no longer in use. can free them. No recursive delete though.*/
		delete [] leftSide;
		delete [] rightSide;

		return sortedSides;
	}
}

//------------------------------------------------------------------
// HeapPage::CompressPage
//
// Input     : None 
// Output    : The page after compression.
// Purpose   : To reclaim the free space in the page left as holes after deletion.
// Return    : OK if everything went OK, FAIL otherwise.
//------------------------------------------------------------------
Status HeapPage::CompressPage() {
	
	/**Firstly, sort the slots by inner offset so that it is easier to shift**/
	/*Dynamically allocated slot directory, so that the slot directory in data is unaffected during sorting*/
	Slot **slotDir;
	slotDir = new Slot *[numOfSlots];
	/*Initialize slotDir. O(n)*/
	for (int i = 0; i <= numOfSlots - 1; i++) {
		slotDir[i] = GetSlotAtIndex(i);
	}
	Slot **sortedDir = SortSlotDirectory(slotDir, 0, numOfSlots - 1); //Worst Case: O(n log n)
	delete [] slotDir;

	int compressedOffset = HEAPPAGE_DATA_SIZE; //the beginning of the compressed data portion.
	Slot *currentSlot; //current slot to shift right and compress
	for (int i = 0; i <= numOfSlots - 1; i++) { //O(n)
		currentSlot = sortedDir[i];
		if (currentSlot->offset == INVALID_SLOT) {
			//we are done dealing with valid slots
			break;
		}
		if (currentSlot->offset + currentSlot->length != compressedOffset) {
			memmove(&data[compressedOffset - currentSlot->length], &data[currentSlot->offset], currentSlot->length);
			compressedOffset -= currentSlot->length;
		}
	}
	freePtr = compressedOffset;
	delete [] sortedDir;
	return OK;
	//TODO when can this fail? supposedly never.
}

//------------------------------------------------------------------
// HeapPage::InsertRecord
//
// Input     : Pointer to the record and the record's length 
// Output    : Record ID of the record inserted.
// Purpose   : Insert a record into the page
// Return    : OK if everything went OK, DONE if sufficient space is not available.
//------------------------------------------------------------------
Status HeapPage::InsertRecord(const char *recPtr, int length, RecordID& rid) {
	// check if there are slots left
	bool slotAvail = false;
	Slot* slot;
	int slotNum;
	for (int i = 0; i < numOfSlots; i++) {
		slot = GetSlotAtIndex(i);
		if (slot->length == INVALID_SLOT) { //TODO SHOULD BE OFFSET
			slotAvail = true;
			slotNum = i;
			break;
		}
	}

	// check for available space
	int spaceNeeded = length;
	if (!slotAvail) {
		spaceNeeded += sizeof(Slot);
	}
	if (freeSpace < spaceNeeded) {
		return DONE;
	}
	if (GetContiguousFreeSpaceSize() < spaceNeeded) {
		if (CompressPage() != OK) {
			return FAIL;
		}
	}

	// copying record into data array
	memcpy(&data[freePtr-length], recPtr, length);

	// insert slot
	if (!slotAvail) {
		slot = AppendNewSlot();
		slotNum = numOfSlots-1;
	}
	slot->offset = freePtr-length;
	slot->length = length;

	// set free stats
	freePtr -= length;
	freeSpace -= spaceNeeded;

	// set output values
	rid.pageNo = pid;
	rid.slotNo = slotNum;
	return OK;
}

bool HeapPage::CheckRecordValidity(RecordID rid) {
	// check if rid is valid
	if (rid.pageNo != pid) {
		return false;
	}
	if (rid.slotNo >= numOfSlots || rid.slotNo < 0) {
		return false;
	}
	Slot* slot = GetSlotAtIndex(rid.slotNo);
	if (slot->length == INVALID_SLOT) {
		return false;
	}
	return true;
}

//------------------------------------------------------------------
// HeapPage::DeleteRecord 
//
// Input    : Record ID
// Output   : None
// Purpose  : Delete a record from the page
// Return   : OK if successful, FAIL otherwise  
//------------------------------------------------------------------ 
Status HeapPage::DeleteRecord(RecordID rid) {
	// check if rid is valid
	if (!CheckRecordValidity(rid)) {
		return FAIL;
	}

	Slot* slot = GetSlotAtIndex(rid.slotNo);
	freeSpace += slot->length;
	slot->length = INVALID_SLOT; //TODO this should be offset

	// if slot is the last one, delete it
	if (rid.slotNo == numOfSlots-1) {
		numOfSlots--;
	}
	return OK;
}

//------------------------------------------------------------------
// HeapPage::FirstRecord
//
// Input    : None
// Output   : record id of the first record on a page
// Purpose  : To find the first record on a page
// Return   : OK if successful, DONE otherwise
//------------------------------------------------------------------
Status HeapPage::FirstRecord(RecordID& rid) {
	if (IsEmpty()) return DONE;

	short max_offset = 0;
	int first_slot_no;
	for (int i = 0; i < numOfSlots; i++) {
		Slot* slot = GetSlotAtIndex(i);
		if (slot->offset > max_offset) {
			max_offset = slot->offset;
			first_slot_no = i;
		}
	}
	rid.pageNo = pid;
	rid.slotNo = first_slot_no;
	return OK;
}

//------------------------------------------------------------------
// HeapPage::NextRecord
//
// Input    : ID of the current record
// Output   : ID of the next record
// Return   : Return DONE if no more records exist on the page; 
//            otherwise OK
//------------------------------------------------------------------
Status HeapPage::NextRecord (RecordID curRid, RecordID& nextRid) {
	if (!CheckRecordValidity(curRid)) {
		return FAIL;
	}

	Slot* curSlot = GetSlotAtIndex(curRid.slotNo);
	if (curSlot->offset == freePtr) {
		return DONE;
	}

	int next_slot_no;
	short max_offset = 0;
	for (int i = 0; i< numOfSlots; i++) {
		Slot* slot = GetSlotAtIndex(i);
		if (slot->offset < curSlot->offset && slot->offset > max_offset) {
			// largest offset that's smaller than curSlot's record offset
			max_offset = slot->offset;
			next_slot_no = i;
		}
	}
	nextRid.pageNo = pid;
	nextRid.slotNo = next_slot_no;
	return OK;
}

//------------------------------------------------------------------
// HeapPage::GetRecord
//
// Input    : Record ID
// Output   : Records length and a copy of the record itself
// Purpose  : To retrieve a _copy_ of a record with ID rid from a page
// Return   : OK if successful, FAIL otherwise
//------------------------------------------------------------------
Status HeapPage::GetRecord(RecordID rid, char *recPtr, int& length) {
	if (!CheckRecordValidity(rid)) {
		return FAIL;
	}

	Slot* slot = GetSlotAtIndex(rid.slotNo);
	if (slot->length > length) { // TODO: length might be a pointer?
		return FAIL;
	}

	memcpy(recPtr, &data[slot->offset], slot->length);
	length = slot->length;
	return OK;
}

//------------------------------------------------------------------
// HeapPage::ReturnRecord
//
// Input    : Record ID
// Output   : pointer to the record, record's length
// Purpose  : To output a _pointer_ to the record
// Return   : OK if successful, FAIL otherwise
//------------------------------------------------------------------
Status HeapPage::ReturnRecord(RecordID rid, char*& recPtr, int& length) {	
	if (!CheckRecordValidity(rid)) {
		return FAIL;
	}

	Slot* slot = GetSlotAtIndex(rid.slotNo);
	recPtr = &data[slot->offset];
	length = slot->length;
	return OK;
}

//------------------------------------------------------------------
// HeapPage::AvailableSpace
//
// Input    : None
// Output   : None
// Purpose  : To return the amount of available space
// Return   : The amount of available space on the heap file page.
//------------------------------------------------------------------
int HeapPage::AvailableSpace(void)
{
	//If there are no empty slots, return freeSpace - sizeof(slot), since the slot will be
	//used for an upcoming record.
	for (int i = 0; i < numOfSlots; i++) {
		if (GetSlotAtIndex(i) -> offset == INVALID_SLOT) {
			return freeSpace;
		}
	}
	return (freeSpace - sizeof(Slot));
}

//------------------------------------------------------------------
// HeapPage::IsEmpty
//
// Input    : None
// Output   : None
// Purpose  : Check if there is any record in the page.
// Return   : true if the HeapPage is empty, and false otherwise.
//------------------------------------------------------------------
bool HeapPage::IsEmpty(void)
{
	/*Scan the slots. If there is a valid record, return false. Otherwise, true*/
	for (int i = 0; i < numOfSlots; i++) {
		if (GetSlotAtIndex(i) -> offset != INVALID_SLOT) {
			return false;
		}
	}
	return true;
}

//------------------------------------------------------------------
// HeapPage::GetNumOfRecords
//
// Input    : None
// Output   : None.
// Purpose  : To determine the number of records on the page (not necessarily equal to the number of slots).
// Return   : The number of records in the page.
//------------------------------------------------------------------
int HeapPage::GetNumOfRecords()
{
	int numOfRecords = 0;
	/*Scan the slots to see if the record is valid. If valid, inc numRecords.*/
	for (int i = 0; i < numOfSlots; i++) {
		if (GetSlotAtIndex(i) -> offset != INVALID_SLOT) {
			numOfRecords++;
		}
	}
	return numOfRecords;
}
