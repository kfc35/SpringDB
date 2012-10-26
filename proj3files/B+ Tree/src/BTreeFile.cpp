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
Status BTreeFile::DestroyFile() {
	PageID root_pid = header->GetRootPageID();

	if (root_pid != INVALID_PAGE) {
		Status freeStatus = FreeTree(root_pid);
		if (freeStatus != OK) {
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

Status BTreeFile::FreeTree(PageID root_pid) {
	Page* root;
	PIN(root_pid, root);
	if (((ResizableRecordPage*) root)->GetType() == INDEX_PAGE) {
		// index page
		PageKVScan<PageID> scanner;
		char* currentKey;
		PageID currentValue;
		IndexPage* index_pg = (IndexPage*) root;
		FreeTree(index_pg->GetPrevPage());
		index_pg->OpenScan(&scanner);
		while (scanner.GetNext(currentKey, currentValue) == OK) {
			Status status = FreeTree(currentValue);
			if (status != OK) return FAIL;
		}
	}
	FREEPAGE(root_pid);
	return OK;
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
		if (MINIBASE_BM->NewPage(root_pid, root_pg) != OK) {
			std::cerr << "Error getting new page in Insert." << std::endl;
			return FAIL; //cannot allocate new page	
		}
		header->SetRootPageID(root_pid);

		LeafPage *leaf_pg = (LeafPage *)root_pg;
		leaf_pg->Init(root_pid, LEAF_PAGE);
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
		PageID* traversed_pages = new PageID [MAX_TREE_DEPTH];
		
		int tree_depth = 0;
		while (((ResizableRecordPage *) current_pg)->GetType() == INDEX_PAGE) {
			index_pg = (IndexPage *)current_pg;
			traversed_pages[tree_depth] = index_pg->PageNo();
			tree_depth++;
			
			Status searchResult = index_pg->Search(key, possiblePages);

			PageID next_search_pg;
			char *key_ptr;
			if (searchResult == DONE || searchResult == OK) {
				possiblePages.GetNext(key_ptr, next_search_pg);
			} else {
				// there is no key smaller than the search key, go to pointer0
				next_search_pg = index_pg->GetPrevPage();
			}
			
			if (MINIBASE_BM->UnpinPage(index_pg->PageNo(), CLEAN) != OK) {
				std::cerr << "Error unpinning page " << index_pg->PageNo() << std::endl;
				delete [] traversed_pages;
				return FAIL;
			}
			if (MINIBASE_BM->PinPage(next_search_pg, (Page *&) current_pg) != OK) {
				std::cerr << "Error pinning page " << next_search_pg << std::endl;
				delete [] traversed_pages;
				return FAIL;
			}
		}

		// at leaf level
		leaf_pg = (LeafPage *)current_pg;
		if (leaf_pg->HasSpaceForValue(key)) {
			if (leaf_pg->Insert(key, rid) != OK) { //Should not happen, there is space on the page.
				std::cerr << "Error in inserting record in leaf page in Insert." << std::endl;
				delete [] traversed_pages;
				return FAIL;
			}
			delete [] traversed_pages;
			return MINIBASE_BM->UnpinPage(leaf_pg->PageNo(), DIRTY);
		} else { // splitting
			// create a new page
			PageID split_pid;
			LeafPage* new_page;
			if (MINIBASE_BM->NewPage(split_pid, (Page*&)new_page) != OK) {
				std::cerr << "Error allocating new page " << split_pid << " for page split." << std::endl;
				delete [] traversed_pages;
				return FAIL;
			}

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
			PageID index_pid = leaf_pg->PageNo(); // for creating new index root
			bool update_index = true;
			
			new_page->GetMinKey(new_index_key);
			new_index_value = new_page->PageNo();

			// unpin the leaf pages
			if (MINIBASE_BM->UnpinPage(leaf_pg->PageNo(), DIRTY) != OK) {
				std::cerr << "Error unpinning page " << leaf_pg->PageNo() << std::endl;
				delete [] traversed_pages;
				return FAIL;
			}
			if (MINIBASE_BM->UnpinPage(new_page->PageNo(), DIRTY) != OK) {
				std::cerr << "Error unpinning page " << leaf_pg->PageNo() << std::endl;
				delete [] traversed_pages;
				return FAIL;
			}

			if (tree_depth == 0) {
				// tree contains a single leaf page, need to create new index root
				IndexPage* new_root_pg;
				PageID new_root_pid;
				if (MINIBASE_BM->NewPage(new_root_pid, (Page*&)new_root_pg) != OK) {
					std::cerr << "Error allocating new index page " << new_root_pid << std::endl;
					delete [] traversed_pages;
					return FAIL;
				}
				new_root_pg->Init(new_root_pid, INDEX_PAGE);
				new_root_pg->Insert(new_index_key, new_index_value);
				new_root_pg->SetPrevPage(index_pid);
				header->SetRootPageID(new_root_pid);
				if (MINIBASE_BM->UnpinPage(new_root_pid, DIRTY) != OK) {
					std::cerr << "Error unpinning page " << new_root_pid << std::endl;
					delete [] traversed_pages;
					return FAIL;
				}
				delete [] traversed_pages;
				return OK;
			}
			
			tree_depth = tree_depth-1;
			while (tree_depth >= 0 && update_index) {
				index_pid = traversed_pages[tree_depth];
				tree_depth--;
				if (MINIBASE_BM->PinPage(index_pid, (Page *&) index_pg) != OK) {
					std::cerr << "Error pinning page " << index_pid << std::endl;
					return FAIL;
				}
				if (index_pg->HasSpaceForValue(new_index_key)) {
					if (index_pg->Insert(new_index_key, new_index_value) != OK) {
						std::cerr << "Error inserting index key." << std::endl;
						delete [] traversed_pages;
						return FAIL;
					}
					update_index = false;
					if (MINIBASE_BM->UnpinPage(index_pid, DIRTY) != OK) {
						std::cerr << "Error unpinning page " << index_pid << std::endl;
						delete [] traversed_pages;
						return FAIL;
					}
				} else {
					// split index node
					PageID new_index_pid;
					IndexPage* new_index;
					if (MINIBASE_BM->NewPage(new_index_pid, (Page*&)new_index) != OK) {
						std::cerr << "Error allocating new page " << new_index_pid << " for index splitting" << std::endl;
						delete [] traversed_pages;
						return FAIL;
					}
					new_index->Init(new_index_pid, INDEX_PAGE);
					SplitIndex(new_index, index_pg, new_index_key, new_index_value, new_index_key, new_index_value);

					new_index->SetPrevPage(new_index_value);
					new_index_value = new_index->PageNo();

					// special case: index node == root node
					if (index_pg->PageNo() == header->GetRootPageID()) {
						PageID new_root_pid;
						IndexPage* new_root;
						if (MINIBASE_BM->NewPage(new_root_pid, (Page*&) new_root) != OK) {
							std::cerr << "error allocating new page " << new_root_pid << " for root split" << std::endl;
							delete [] traversed_pages;
							return FAIL;
						}
						new_root->Init(new_root_pid, INDEX_PAGE);
						new_root->SetPrevPage(index_pg->PageNo());
						new_root->Insert(new_index_key, new_index_value);
						header->SetRootPageID(new_root_pid);
						//std::cout << "Root split, old root_pid: " << index_pg->PageNo() << ". new root_pid: " << new_root_pid << std::endl;
						if (MINIBASE_BM->UnpinPage(new_root_pid, DIRTY) != OK) {
							std::cerr << "Error unpinning page " << new_root_pid << std::endl;
							delete [] traversed_pages;
							return FAIL;
						}
					}

					if (MINIBASE_BM->UnpinPage(index_pid, DIRTY) != OK) {
						std::cerr << "Error unpinning page " << index_pid << std::endl;
						delete [] traversed_pages;
						return FAIL;
					}
					if (MINIBASE_BM->UnpinPage(new_index_pid, DIRTY) != OK) {
						std::cerr << "Error unpinning page " << new_index_pid << std::endl;
						delete [] traversed_pages;
						return FAIL;
					}
				}
			}
			delete [] traversed_pages;
			return OK;
		}
	}
}


//-------------------------------------------------------------------
// BTreeFile::SplitIndex
//
// Input   : newPage - newly created index page
//           oldPage - the full page to be split
//			 newKey - the key to be inserted
//			 newValue - the new value to be inserted
// Output  : propagatedKey - the key to be propagated up a level
//			 propagatedValue - the value to be propagated up a level
// Purpose : Splitting an index page
//-------------------------------------------------------------------
void BTreeFile::SplitIndex(IndexPage* newPage, IndexPage* oldPage, const char* newKey, PageID newValue, 
						   char*& propagatedKey, PageID& propagatedValue) {
	char* currentKey;
	PageID currentValue;

	oldPage->GetMaxKey(currentKey);
	while (!oldPage->IsEmpty() && strcmp(currentKey, newKey) > 0) {
		// move max key and its value from oldPage to newPage
		oldPage->GetMaxKeyValue(currentKey, currentValue);
		newPage->Insert(currentKey, currentValue);
		oldPage->DeleteKey(currentKey);
		oldPage->GetMaxKey(currentKey);
	}

	// keeping both pages balanced with one entry taken out as the new propagated index
	if (oldPage->AvailableSpace() < newPage->AvailableSpace()) {
		newPage->Insert(newKey, newValue);
		oldPage->GetMaxKeyValue(propagatedKey, propagatedValue);
		oldPage->DeleteKey(propagatedKey);
		while (oldPage->AvailableSpace() < newPage->AvailableSpace()) {
			// move max key from oldPage to newPage
			newPage->Insert(propagatedKey, propagatedValue);
			oldPage->GetMaxKeyValue(propagatedKey, propagatedValue);
			oldPage->DeleteKey(propagatedKey);
		}
	} else {
		oldPage->Insert(newKey, newValue);
		newPage->GetMinKeyValue(propagatedKey, propagatedValue);
		newPage->DeleteKey(propagatedKey);
		while (oldPage->AvailableSpace() > newPage->AvailableSpace()) {
			oldPage->Insert(propagatedKey, propagatedValue);
			newPage->GetMinKeyValue(propagatedKey, propagatedValue);
			newPage->DeleteKey(propagatedKey);
		}
	}
}

//-------------------------------------------------------------------
// BTreeFile::SplitPage
//
// Input   : newPage - newly created leaf page
//           oldPage - the full page to be split
//			 newKey - the key to be inserted
//			 newValue - the new value to be inserted
// Output  : None
// Purpose : Splitting a leaf page
//-------------------------------------------------------------------
void BTreeFile::SplitPage(LeafPage* newPage, LeafPage* oldPage, const char* newKey, RecordID newValue) {
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
		oldPage->GetMaxKey(maxKey);
		// making sure existing values to newKey are moved along with the new value of newKey
		if (strcmp(newKey, maxKey) == 0) {
			oldPage->Search(maxKey, pageScanner);
			while (pageScanner.GetNext(currentKey, currentValue) == OK) {
				newPage->Insert(currentKey, currentValue);
			}
			oldPage->DeleteKey(maxKey);
		}
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
		PageID next_search_pg;

		while (((ResizableRecordPage *) current_pg)->GetType() == INDEX_PAGE) {
			index_pg = (IndexPage *)current_pg;
			PageKVScan<PageID> indexScanner;
			char* key;
			Status searchResult;
			if (lowKey != NULL) {
				searchResult = index_pg->Search(lowKey, indexScanner);
			}
			if (lowKey != NULL && (searchResult == OK || searchResult == DONE)) {
				indexScanner.GetNext(key, next_search_pg);
			} else {
				next_search_pg = index_pg->GetPrevPage();
			}

			if (MINIBASE_BM->UnpinPage(index_pg->PageNo(), CLEAN) != OK) {
				return NULL;
			}
			if (MINIBASE_BM->PinPage(next_search_pg, current_pg) != OK) {
				return NULL;
			}
		}

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
