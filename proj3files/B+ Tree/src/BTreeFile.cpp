#include "BTreeFile.h"
//#include "BTreeLeafPage.h"
#include "db.h"
#include "bufmgr.h"
#include "system_defs.h"

#include <list> 
using namespace std;

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
			header = (BTreeHeaderPage *)headerPage;
			fname = new char[strlen(filename) + 1]; //+1 for \0
			strcpy(fname, filename);
			returnStatus = OK;
		}
		else {
			std::cerr << "Error pinning header page in BTreeFile Constructor." << std::endl;
			returnStatus = FAIL;
		}
	}
	else { //index does not exist in the database
		if (MINIBASE_BM->NewPage(start_pg, headerPage) == OK) { //allocate and pin page for header
			//try to add the file entry
			if (MINIBASE_DB->AddFileEntry(filename, start_pg) == OK) {
				header = (BTreeHeaderPage *)headerPage;
				header->Init(start_pg);
				fname = new char[strlen(filename) + 1]; // +1 for \0
				strcpy(fname, filename);
				returnStatus = OK;
			}
			else {
				std::cerr << "Error adding file entry in BTreeFile Constructor." << std::endl;
				MINIBASE_BM->FreePage(start_pg); //Attempt to free the page on failure
				returnStatus = FAIL;
			}
		}
		else { //The DB failed to allocate new page for the header
			std::cerr << "Error getting new header page in BTreeFile Constructor." << std::endl;
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
	if (fname != NULL) {
		delete [] fname;
	}
	// TODO: Not sure if OK is the only thing returned when done flushing,
	// (could also return DONE if nothing's flushed)
	/*if (MINIBASE_BM->FlushPage(heap_header->PageNo()) != OK) {
		std::cerr << "Unable to flush page " << heap_header << std::endl;
		return;
	}*/
	/* Setting the page to be dirty just in case*/
	if (header != NULL) {
		HeapPage* heap_header = (HeapPage *) header;
		if (MINIBASE_BM->UnpinPage(heap_header->PageNo(), DIRTY) != OK) {
			std::cerr << "Unable to unpin page " << heap_header << std::endl;
		}
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
	PageID root_pid = header->GetRootPageID();
	Page *root_pg;
	if (MINIBASE_BM->PinPage(root_pid, root_pg) != OK) {
		std::cerr << "Unable to pin root page in DestroyFile" << std::endl;
		return FAIL;
	}
	if (((ResizableRecordPage *)root_pg)->GetType() == INDEX_PAGE) {
		//TODO: recursive free of child pages, and then free this page.
	}
	else {
		//Index page is a leaf page, free it.
		if (MINIBASE_BM->FreePage(root_pid) != OK) {
			std::cerr << "Unable to free root page in DestroyFile" << std::endl;
			return FAIL;	
		}
	}
	//Free the header page
	if (MINIBASE_BM->FreePage(((HeapPage *)header)->PageNo()) != OK) {
		std::cerr << "Unable to free header page in DestroyFile" << std::endl;
		return FAIL;
	}
	header = NULL;

	//Remove DB file
	return MINIBASE_DB->DeleteFileEntry(fname);
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

	/**CASE: B+ Tree is COMPLETELY Empty**/
	if (root_pid == INVALID_PAGE) {
		/*Must create a root page of type LEAF_PAGE for the first page*/
		// TODO replace with macro?
		if (MINIBASE_BM->NewPage(root_pid, root_pg) != OK) {
			std::cerr << "Error getting new page in Insert." << std::endl;
			return FAIL; //cannot allocate new page	
		}
		header->SetRootPageID(root_pid);
		/*Flush the setting of the new root page to disk*/
		//TODO this flushing comes up with errors <- BufMgr spec says can't flush unless page is unpinned
		/*if (MINIBASE_BM->FlushPage(((HeapPage *)header)->PageNo()) != OK) {
			std::cerr << "Error flushing header page in Insert." << std::endl;
			MINIBASE_BM->FreePage(root_pid); //Attempt to free the page on failure
			return FAIL;
		}*/

		LeafPage *leaf_pg = (LeafPage *)root_pg;
		leaf_pg->Init(root_pid, LEAF_PAGE);
		//leaf_pg->SetNextPage(INVALID_PAGE);
		//leaf_pg->SetPrevPage(INVALID_PAGE);
		/*Try to insert the record into this leaf (root) page*/
		if (leaf_pg->Insert(key, rid) != OK) { //Should not happen, there is space on the page.
			std::cerr << "Error in inserting record in root leaf page in Insert." << std::endl;
			MINIBASE_BM->FreePage(root_pid); //Attempt to free the page on failure
			return FAIL;
		}

		//Write the new record onto disk and unpin the page
		return MINIBASE_BM->UnpinPage(root_pid, DIRTY);
	}
	/**CASE: B+ Tree has a Root Node**/
	else {
		// TODO: replace with macro?
		//PIN(root_pid, root_pg);
		if (MINIBASE_BM->PinPage(root_pid, root_pg) != OK) {
			std::cerr << "Error pinning root page in Insert." << std::endl;
			return FAIL; //cannot pin root page
		}

		/**Traverse from the root page until you get the leaf page associated
		  *with this key*/
		LeafPage *leaf_pg;
		IndexPage *index_pg;
		PageKVScan<PageID> possiblePages;
		Page *current_pg = root_pg;
		list<PageID> traversed_pages;
		
		//TODO: we probably should keep track of the path taken all the way down to the leaf page
		//For future reference when we need to split nodes, since it can propogate up.
		while (((ResizableRecordPage *) current_pg)->GetType() == INDEX_PAGE) {
			index_pg = (IndexPage *)current_pg;
			traversed_pages.push_front(index_pg->PageNo());
			Status searchResult = index_pg->Search(key, possiblePages);

			PageID next_search_pg;
			char *key_ptr;
			if (searchResult == DONE || searchResult == OK) {
				possiblePages.GetNext(key_ptr, next_search_pg);
			} else {
				// there is no key smaller than the search key, go to pointer0
				next_search_pg = index_pg->GetPrevPage();
			}
			
			UNPIN(index_pg->PageNo(), CLEAN);
			PIN(next_search_pg, current_pg);
		}

		// at leaf level
		leaf_pg = (LeafPage *)current_pg;
		if (leaf_pg->HasSpaceForValue(key)) {
			if (leaf_pg->Insert(key, rid) != OK) { //Should not happen, there is space on the page.
				std::cerr << "Error in inserting record in root leaf page in Insert." << std::endl;
				return FAIL;	
			}
			//TODO later probably have to unpin the whole IndexPage traversal.
			// ^ currently unpinned after every step of the traversal
			return MINIBASE_BM->UnpinPage(leaf_pg->PageNo(), DIRTY);
		}
		else { // splitting
			// create a new page
			PageID split_pid;
			LeafPage* new_page;
			NEWPAGE(split_pid, new_page);

			// splits leaf into 2 pages
			new_page->Init(split_pid, LEAF_PAGE);
			SplitPage(new_page, leaf_pg, key, rid);

			// set next/prev pointers
			new_page->SetNextPage(leaf_pg->GetNextPage());
			leaf_pg->SetNextPage(new_page->PageNo());
			new_page->SetPrevPage(leaf_pg->PageNo());

			// updating index
			char* new_index_key;
			PageID new_index_value;
			PageID index_pid;
			bool update_index = true;
			
			new_page->GetMinKey(new_index_key);
			new_index_value = new_page->PageNo();

			// unpin the leaf pages
			UNPIN(leaf_pg->PageNo(), DIRTY);
			UNPIN(new_page->PageNo(), DIRTY);

			// TODO: might need to deference front()
			while (index_pid = traversed_pages.front() && update_index) {
				PIN(index_pid, index_pg);
				if (index_pg->HasSpaceForValue(new_index_key)) {
					if (index_pg->Insert(new_index_key, new_index_value) != OK) {
						std::cerr << "Error inserting index key." << std::endl;
						return FAIL;
					}
					update_index = false;
					UNPIN(index_pid, DIRTY);
				} else {
					// split index node
					PageID new_index_pid;
					IndexPage* new_index;
					NEWPAGE(new_index_pid, new_index);
					new_index->Init(new_index_pid, INDEX_PAGE);
					SplitIndex(new_index, index_pg, new_index_key, new_index_value);

					// get the propagated index and correct pointers
					index_pg->GetMaxKey(new_index_key);
					index_pg->GetMaxKeyValue(new_index_key, new_index_value);
					index_pg->DeleteKey(new_index_key);

					new_index->SetPrevPage(new_index_value);
					new_index_value = new_index->PageNo();

					// special case: index node == root node
					if (index_pg->PageNo() == header->GetRootPageID()) {
						PageID new_root_pid;
						IndexPage* new_root;
						NEWPAGE(new_root_pid, new_root);
						new_root->Init(new_root_pid, INDEX_PAGE);
						new_root->SetPrevPage(index_pg->PageNo());
						new_root->Insert(new_index_key, new_index_value);
						header->SetRootPageID(new_root_pid);
						UNPIN(new_root_pid, DIRTY);
					}

					UNPIN(index_pg->PageNo(), DIRTY);
					UNPIN(new_index->PageNo(), DIRTY);
				}
			}

			return OK;
		}
	}
}

void SplitIndex(IndexPage* newPage, IndexPage* oldPage, const char* newKey, PageID newValue) {
	PageKVScan<PageID> indexScanner;
	char* maxKey;
	PageID currentValue;
	bool inserted = false;

	// move half the pages over to the new page
	while (oldPage->AvailableSpace() < newPage->AvailableSpace()) {
		oldPage->GetMaxKey(maxKey);
		if (!inserted && strcmp(maxKey, newKey) < 0) {
			newPage->Insert(newKey, newValue);
			inserted = true;
		} else {
			oldPage->GetMaxKeyValue(maxKey, currentValue);
			newPage->Insert(maxKey, currentValue);
			oldPage->DeleteKey(maxKey);
		}
	}
}

void SplitPage(LeafPage* newPage, LeafPage* oldPage, const char* newKey, RecordID newValue) {
	char* maxKey;
	PageKVScan<RecordID> pageScanner;
	char* currentKey;
	RecordID currentValue;
	oldPage->GetMaxKey(maxKey);

	while (!oldPage->IsEmpty() && strcmp(maxKey, newKey) > 0) {
		// move max key and its values from oldPage to newPage
		oldPage->Search(maxKey, pageScanner);
		while (pageScanner.GetNext(currentKey, currentValue) == OK) {
			newPage->Insert(currentKey, currentValue);
		}
		oldPage->DeleteKey(maxKey);
		oldPage->GetMaxKey(maxKey);
	}

	if (oldPage->AvailableSpace() < newPage->AvailableSpace()) {
		newPage->Insert(newKey, newValue);
		while (oldPage->AvailableSpace() < newPage->AvailableSpace()) {
			// move max key and its values from oldPage to newPage
			oldPage->GetMaxKey(maxKey);
			oldPage->Search(maxKey, pageScanner);
			while (pageScanner.GetNext(currentKey, currentValue) == OK) {
				newPage->Insert(currentKey, currentValue);
			}
			oldPage->DeleteKey(maxKey);
		}
	} else {
		char* minKey;
		oldPage->Insert(newKey, newValue);
		while (oldPage->AvailableSpace() > newPage->AvailableSpace()) {
			newPage->GetMinKey(minKey);
			newPage->Search(minKey, pageScanner);
			while (pageScanner.GetNext(currentKey, currentValue) == OK && currentKey == minKey) {
				oldPage->Insert(currentKey, currentValue);
			}
			newPage->DeleteKey(minKey);
		}
	}
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
	/*Find the leaf page with the lowKey in it*/
	PageID root_pid = header->GetRootPageID();
	Page *root_pg;
	if (root_pid == INVALID_PAGE) {
		return NULL;
	}
	if (MINIBASE_BM->PinPage(root_pid, root_pg) == OK) {
		Page *current_pg = root_pg;
		IndexPage *index_pg;
		LeafPage *leaf_pg;
		//TODO loop iteration through index pages to find a specific leaf page.
		//Exactly the same as the function in INSERT, except don't need to keep all
		//the pages pinned this time.
		/*while (((ResizableRecordPage *) current_pg)->GetType() == INDEX_PAGE) {
			index_pg = (IndexPage *)current_pg;
		}*/
		leaf_pg = (LeafPage *)current_pg;
		BTreeFileScan *btfs = new BTreeFileScan();
		/*Initialize scan with these low and high values*/
		if (lowKey != NULL) {
			btfs->low = new char[MAX_KEY_LENGTH];
			strcpy(btfs->low, lowKey);
		}
		if (highKey != NULL) {
			btfs->high = new char[MAX_KEY_LENGTH];
			strcpy(btfs->high, highKey);
		}

		/*Initialize with this leaf page*/
		btfs->current_leaf = leaf_pg; //PAGE STAYS PINNED!
		return btfs;
	}
	else {
		return NULL;
	}
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
