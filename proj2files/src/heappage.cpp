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
HeapPage::Slot* HeapPage::GetSlotAtIndex(int slotNumber) { //slot numbers are 1...n
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
	int contigFreeSpaceBeginning = numOfSlots * sizeof(Slot);
	/*Since slot offsets can point anywhere within the data, we must scan the whole directory
	  for the earliest offset, as this will be the end of the contiguous free region.*/
	int contigFreeSpaceEnding = HEAPPAGE_DATA_SIZE;
	for (int i = 0; i <= numOfSlots - 1; i++) {
		//if the offset of the current slot is before the currently marked end of the contig free space
		if ((((Slot *)(&data[i * sizeof(Slot)])) -> offset) < contigFreeSpaceEnding) {
			contigFreeSpaceEnding = (((Slot *)(&data[i * sizeof(Slot)])) -> offset);
		}
	}
	return contigFreeSpaceEnding - contigFreeSpaceBeginning; 
	//could also do (freeSpace - (HEAPPAGE_DATA_SIZE - contigFreeSpaceEnding))
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
		//the next slot is at numOfSlots (slots are indexed 0... n-1 in the array)
		Slot *newSlot = (Slot *)(&data[numOfSlots * sizeof(Slot)]);
		numOfSlots++;
		freeSpace -= sizeof(Slot);
		return newSlot;
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
	//Record offsets are -1 if there was a deleted
	//memmov
	// Order all the slots by offset. Then move slots down as necessary.

	Slot *currentSlot; //current slot to shift right for compression.
	int compressedOffset = HEAPPAGE_DATA_SIZE; //everything after this offset has been compressed
	int largestOffsetUncompressed; //largest offset of the slot in the uncompressed region.

	/*Must verify all slots are compressed*/
	for (int i = 0; i <= numOfSlots - 1; i++) {

		/*Find the next slot to work on compressing towards the right.
		 *The next slot to work on is the slot which has the largest offset in the uncompresed data region 
		  (we moved all records past 'compressedOffset' already)*/
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
	//TODO when can this fail?
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
Status HeapPage::InsertRecord(const char *recPtr, int length, RecordID& rid) {
	
	// check if there are slots left
	bool slot_avail = false;
	Slot* slot;
	for (int i = 0; i < numOfSlots; i++) {
		slot = (Slot *) &(data[i * sizeof(slot)]);
		if (slot->length == INVALID_SLOT) { // TODO: is this check sufficient?
			slot_avail = true;
			break;
		}
	}

	// check for available space
	int spaceNeeded;
	if (slot_avail) {
		spaceNeeded = length;
	} else {
		spaceNeeded = length + sizeof(Slot);
	}
	if (GetContiguousFreeSpaceSize() < spaceNeeded) {
		if (CompressPage() != OK) {
			return DONE;
		}
		if (GetContiguousFreeSpaceSize() < spaceNeeded) {
			return DONE;
		}
	}

	// copying record into data array
	memcpy(&data[freePtr-length], recPtr, length);

	// insert slot
	if (!slot_avail) {
		slot = AppendNewSlot();
	}
	slot->offset = freePtr-length;
	slot->length = length;

	freePtr -= length;
	freeSpace -= spaceNeeded;

	rid.pageNo=pid;
	//rid.slotNo = 
	return OK;
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
	for (int i = 0; i <= numOfSlots - 1; i++) {
		if (((Slot *)(&data[i * sizeof(Slot)])) -> offset == INVALID_SLOT) {
			return freeSpace;
		}
	}
	return freeSpace - sizeof(Slot);
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
	for (int i = 0; i <= numOfSlots - 1; i++) {
		if (((Slot *)(&data[i * sizeof(Slot)])) -> offset != INVALID_SLOT) {
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
	for (int i = 0; i <= numOfSlots - 1; i++) {
		if (((Slot *)(&data[i * sizeof(Slot)])) -> offset != INVALID_SLOT) {
			numOfRecords++;
		}
	}
	return numOfRecords;
}
