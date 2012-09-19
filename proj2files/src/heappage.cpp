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
HeapPage::Slot* HeapPage::GetSlotAtIndex(int slotNumber) { //slot numbers are 1...numOfSlots
	if (slotNumber > (numOfSlots)) {
		return NULL;
	}
	return (Slot *)(&data[(slotNumber - 1) * sizeof(Slot)]); //array slots are addressed 0 ... n-1
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
		Slot *newSlot = GetSlotAtIndex(numOfSlots);
		freeSpace -= sizeof(Slot);
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

		int pointerLeft = begin;
		int pointerRight = rightBegin;
		int sortedIndex = 0;
		//choose the largest between the two and place it first.
		while (pointerLeft <= leftEnd && pointerRight <= end) {
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
		if (pointerLeft <= leftEnd) {
			for (int i = pointerLeft; i <= leftEnd; i++) {
				sortedSides[sortedIndex] = leftSide[pointerLeft];
				sortedIndex++;
			}
		}
		if (pointerRight <= end) {
			for (int i = pointerRight; i <= end; i++) {
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
	for (int i = 1; i <= numOfSlots; i++) {
		slotDir[i-1] = GetSlotAtIndex(i);
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

	//TODO go through sorted directory and shift. O(n)

	/*Must verify all slots are compressed*/
	/**
	for (int i = 0; i <= numOfSlots - 1; i++) {

		/*Find the next slot to work on compressing towards the right.
		 *The next slot to work on is the slot which has the largest offset in the uncompresed data region 
		  (we moved all records past 'compressedOffset' already)
		bool noCompressionNeeded = false;
		currentSlot = NULL;
		largestOffsetUncompressed = 0;
		for (int j = 0; j <= numOfSlots - 1; j++) {
			Slot *slot = (Slot *) &(data[j * sizeof(slot)]);
			if ((slot -> offset) >= largestOffsetUncompressed && (slot -> offset) < compressedOffset) {
				//Check if the slot actually needs moving.
				if ((slot -> offset) + (slot -> length) == compressedOffset) {
					noCompressionNeeded = true;
					//since the slot is already compressed, update the compressedOffset
					compressedOffset = slot -> offset;
					break;
				}
				else {
					largestOffsetUncompressed = slot -> offset;
					currentSlot = slot;
				}
			}
		}
		// if there's no shift needed, continue to find the next slot that may need compression.
		if (noCompressionNeeded == true) {
			continue;
		}
		// If there are only slots with offset = INVALID_SLOT, the currentSlot will be null.
		// (the inner loop never sets currentSlot, since largestOffsetUncompressed is initialized to 0 > INVALID_SLOT)
		// break because there are no more valid records to compress
		if (currentSlot == NULL) {
			break;
		}
		// else, we must move the memory and update the slot offset.
		memmove(&data[compressedOffset - (currentSlot->length)], &data[currentSlot->offset], currentSlot->length);
		currentSlot->offset = compressedOffset - (currentSlot->length);
	}
	**/
	//TODO when can this fail? supposedly never.
	return OK;
}

//------------------------------------------------------------------
// HeapPage::InsertRecord
//
// Input     : Pointer to the record and the record's length 
// Output    : Record ID of the record inserted.
// Purpose   : Insert a record into the page
// Return    : OK if everything went OK, DONE if sufficient space is not available.
//------------------------------------------------------------------
Status HeapPage::InsertRecord(const char *recPtr, int length, RecordID& rid)
{
	//TODO: add your code here
	return FAIL;
}

//------------------------------------------------------------------
// HeapPage::DeleteRecord 
//
// Input    : Record ID
// Output   : None
// Purpose  : Delete a record from the page
// Return   : OK if successful, FAIL otherwise  
//------------------------------------------------------------------ 
Status HeapPage::DeleteRecord(RecordID rid)
{
	//TODO: add your code here
	return FAIL;
}

//------------------------------------------------------------------
// HeapPage::FirstRecord
//
// Input    : None
// Output   : record id of the first record on a page
// Purpose  : To find the first record on a page
// Return   : OK if successful, DONE otherwise
//------------------------------------------------------------------
Status HeapPage::FirstRecord(RecordID& rid)
{
	//TODO: add your code here
	return FAIL;
}

//------------------------------------------------------------------
// HeapPage::NextRecord
//
// Input    : ID of the current record
// Output   : ID of the next record
// Return   : Return DONE if no more records exist on the page; 
//            otherwise OK
//------------------------------------------------------------------
Status HeapPage::NextRecord (RecordID curRid, RecordID& nextRid)
{
	//TODO: add your code here
	return FAIL;
}

//------------------------------------------------------------------
// HeapPage::GetRecord
//
// Input    : Record ID
// Output   : Records length and a copy of the record itself
// Purpose  : To retrieve a _copy_ of a record with ID rid from a page
// Return   : OK if successful, FAIL otherwise
//------------------------------------------------------------------
Status HeapPage::GetRecord(RecordID rid, char *recPtr, int& length)
{
	//TODO: add your code here
	return FAIL;
}

//------------------------------------------------------------------
// HeapPage::ReturnRecord
//
// Input    : Record ID
// Output   : pointer to the record, record's length
// Purpose  : To output a _pointer_ to the record
// Return   : OK if successful, FAIL otherwise
//------------------------------------------------------------------
Status HeapPage::ReturnRecord(RecordID rid, char*& recPtr, int& length)
{	
	//TODO: add your code here
	return FAIL;
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
	for (int i = 1; i <= numOfSlots; i++) {
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
	for (int i = 1; i <= numOfSlots; i++) {
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
	for (int i = 1; i <= numOfSlots; i++) {
		if (GetSlotAtIndex(i) -> offset != INVALID_SLOT) {
			numOfRecords++;
		}
	}
	return numOfRecords;
}
