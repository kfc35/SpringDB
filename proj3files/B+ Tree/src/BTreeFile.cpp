#include "BTreeFile.h"
//#include "BTreeLeafPage.h"
#include "db.h"
#include "bufmgr.h"
#include "system_defs.h"

//-------------------------------------------------------------------
// BTreeFile::BTreeFile
//
// Input   : filename - filename of an index.
// Output  : returnStatus - status of execution of constructor.
//           OK if successful, FAIL otherwise.
// Purpose : Open the index file, if it exists.
//			 Otherwise, create a new index, with the specified
//           filename. You can use
//                MINIBASE_DB->GetFileEntry(filename, headerID);
//           to retrieve an existing index file, and
//                MINIBASE_DB->AddFileEntry(filename, headerID);
//           to create a new one. You should pin the header page
//           once you have read or created it. You will use the header
//           page to find the root node.
//-------------------------------------------------------------------
BTreeFile::BTreeFile(Status &returnStatus, const char *filename) {
	PageID start_pg = INVALID_PAGE;
	Page *headerPage;
	if (MINIBASE_DB->GetFileEntry(filename, start_pg) == OK) {
		//index does exist in the database.
		//start_pg is PageID of header page. Open and Pin it.
		if (MINIBASE_BM->PinPage(start_pg, headerPage) == OK) { 
			//I assume headerPage points to a valid page after this
			header = (BTreeHeaderPage *)headerPage;
			returnStatus = OK;
		}
		else {
			returnStatus = FAIL;
		}
	}
	else { //index does not exist in the database
		if (MINIBASE_DB->AllocatePage(start_pg) == OK) { //allocate page for header
			//Open and pin the new header page for the B+ Tree (it is an empty page)
			if (MINIBASE_BM->PinPage(start_pg, headerPage, true) == OK) {
				//I assume headerPage points to a valid page after this
				header = (BTreeHeaderPage *)headerPage;
				header->Init(start_pg);

				returnStatus = MINIBASE_DB->AddFileEntry(filename, start_pg);
			}
			else {
				returnStatus = FAIL;
			}
		}
		else { //The DB failed to allocate new page for the header
			//TODO ensure that AllocatePage returns OK on success... it could return DONE instead.
			returnStatus = FAIL;
		}
	}
}


//-------------------------------------------------------------------
// BTreeFile::~BTreeFile
//
// Input   : None
// Return  : None
// Output  : None
// Purpose : Free memory and clean Up. You should be sure to
//           unpin the header page if it has not been unpinned
//           in DestroyFile.
//-------------------------------------------------------------------
BTreeFile::~BTreeFile() {
	HeapPage* heap_header = (HeapPage *) header;
	// TODO: Not sure if OK is the only thing returned when done flushing,
	// (could also return DONE if nothing's flushed)
	if (MINIBASE_BM->FlushPage(heap_header->PageNo()) != OK) {
		std::cerr << "Unable to flush page " << heap_header << std::endl;
		return;
	}
	if (MINIBASE_BM->UnpinPage(heap_header->PageNo(), false) != OK) {
		std::cerr << "Unable to unpin page " << heap_header << std::endl;
	}
}

//-------------------------------------------------------------------
// BTreeFile::DestroyFile
//
// Input   : None
// Output  : None
// Return  : OK if successful, FAIL otherwise.
// Purpose : Free all pages and delete the entire index file. Once you have
//           freed all the pages, you can use MINIBASE_DB->DeleteFileEntry (dbname)
//           to delete the database file.
//-------------------------------------------------------------------
Status BTreeFile::DestroyFile()
{
	// TODO: Add your code here.
	return FAIL;
}


//-------------------------------------------------------------------
// BTreeFile::Insert
//
// Input   : key - pointer to the value of the key to be inserted.
//           rid - RecordID of the record to be inserted.
// Output  : None
// Return  : OK if successful, FAIL otherwise.
// Purpose : Insert an index entry with this rid and key.
// Note    : If the root didn't exist, create it.
//-------------------------------------------------------------------
Status BTreeFile::Insert(const char *key, const RecordID rid) {
	PageID root_pid = header->GetRootPageID();
	Page* root_pg;
	PageID leaf_pid; Page* leaf_pg;
	if (root_pid == NULL) {
		NEWPAGE(root_pid, root_pg);
		// insert key into index
		IndexPage* root_index = (IndexPage*) root_pg;
		// making the new leaf page
		NEWPAGE(leaf_pid, leaf_pg);
		root_index->Insert(key, leaf_pid);
	}
	PIN(root_pid, root_pg);
	PIN(leaf_pid, leaf_pg);
	IndexPage* root_index = (IndexPage*) root_pg;
	
	return FAIL;
}

//-------------------------------------------------------------------
// BTreeFile::OpenScan
//
// Input   : lowKey, highKey - pointer to keys, indicate the range
//                             to scan.
// Output  : None
// Return  : A pointer to BTreeFileScan class.
// Purpose : Initialize a scan.
// Note    : Usage of lowKey and highKey :
//
//           lowKey   highKey   range
//			 value	  value
//           --------------------------------------------------
//           NULL     NULL      whole index
//           NULL     !NULL     minimum to highKey
//           !NULL    NULL      lowKey to maximum
//           !NULL    =lowKey   exact match (may not be unique)
//           !NULL    >lowKey   lowKey to highKey
//-------------------------------------------------------------------
BTreeFileScan *BTreeFile::OpenScan(const char *lowKey, const char *highKey)
{
	// TODO: Add your code here.
	return NULL;
}


//-------------------------------------------------------------------
// BTreeFile::GetLeftLeaf
//
// Input   : None
// Output  : None
// Return  : The PageID of the leftmost leaf page in this index.
// Purpose : Returns the pid of the leftmost leaf.
//-------------------------------------------------------------------
PageID BTreeFile::GetLeftLeaf()
{
	PageID leafPid = header->GetRootPageID();

	while (leafPid != INVALID_PAGE) {
		ResizableRecordPage *rrp;

		if (MINIBASE_BM->PinPage(leafPid, (Page *&)rrp) == FAIL) {
			std::cerr << "Error pinning page in GetLeftLeaf." << std::endl;
			return INVALID_PAGE;
		}

		//If we have reached a leaf page, then we are done.
		if (rrp->GetType() == LEAF_PAGE) {
			break;
		}
		//Otherwise, traverse down the leftmost branch.
		else {
			PageID tempPid = rrp->GetPrevPage();

			if (MINIBASE_BM->UnpinPage(leafPid, CLEAN) == FAIL) {
				std::cerr << "Error unpinning page in OpenScan." << std::endl;
				return INVALID_PAGE;
			}

			leafPid = tempPid;
		}
	}

	if (leafPid != INVALID_PAGE) {
		if (MINIBASE_BM->UnpinPage(leafPid, CLEAN) == FAIL) {
			std::cerr << "Error unpinning page in OpenScan." << std::endl;
			return INVALID_PAGE;
		}
	}

	return leafPid;
}


//-------------------------------------------------------------------
// BTreeFile::PrintTree
//
// Input   : pageID,  the page to start printing at.
//           printContents,  whether to print the contents
//                           of the page or just metadata.
// Output  : None
// Return  : None
// Purpose : Prints the subtree rooted at the given page.
//-------------------------------------------------------------------
Status BTreeFile::PrintTree(PageID pageID, bool printContents)
{
	ResizableRecordPage *page;
	PIN(pageID, page);

	if (page->GetType() == INDEX_PAGE) {
		IndexPage *ipage = (IndexPage *) page;
		ipage->PrintPage(printContents);

		PageID pid = ipage->GetPrevPage();
		assert(pid != INVALID_PAGE);

		PrintTree(pid, printContents);

		PageKVScan<PageID> scan;

		if (ipage->OpenScan(&scan) != OK) {
			return FAIL;
		}

		while (true) {
			char *key;
			PageID val;
			Status stat = scan.GetNext(key, val);
			assert(val != INVALID_PAGE);

			if (stat == DONE) {
				break;
			}

			PrintTree(val, printContents);
		}
	} else {
		LeafPage *lpage = (LeafPage *) page;
		lpage->PrintPage(printContents);
	}

	UNPIN(pageID, CLEAN);
	return OK;
}


//-------------------------------------------------------------------
// BTreeFile::PrintWhole
//
// Input   : printContents,  whether to print the contents
//                           of each page or just metadata.
// Output  : None
// Return  : None
// Purpose : Prints the B Tree.
//-------------------------------------------------------------------
Status BTreeFile::PrintWhole(bool printContents)
{
	if (header == NULL || header->GetRootPageID() == INVALID_PAGE) {
		return FAIL;
	}

	return PrintTree(header->GetRootPageID(), printContents);
}
