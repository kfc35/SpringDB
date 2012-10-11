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
		if (MINIBASE_BM->NewPage(root_pid, root_pg) != OK) {
			std::cerr << "Error getting new page in Insert." << std::endl;
			return FAIL; //cannot allocate new page	
		}
		header->SetRootPageID(root_pid);
		/*Flush the setting of the new root page to disk*/
		//TODO this flushing comes up with errors
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
		//TODO: we probably should keep track of the path taken all the way down to the leaf page
		//For future reference when we need to split nodes, since it can propogate up.
		//COMMENTED OUT BECAUSE WE ARE ONLY DEALING WITH ONE LEAF NODE TREES
		/*while (((ResizableRecordPage *) current_pg)->GetType() == INDEX_PAGE) {
			index_pg = (IndexPage *)current_pg;
			Status searchResult = index_pg->Search(key, possiblePages);
			
		}*/
		leaf_pg = (LeafPage *)current_pg;
		if (leaf_pg->HasSpaceForValue(key)) {
			if (leaf_pg->Insert(key, rid) != OK) { //Should not happen, there is space on the page.
				std::cerr << "Error in inserting record in root leaf page in Insert." << std::endl;
				return FAIL;	
			}
			//TODO later probably have to unpin the whole IndexPage traversal.
			return MINIBASE_BM->UnpinPage(leaf_pg->PageNo(), DIRTY);
		}
		else {
			//TODO splitting.
			//Insert propagates up the IndexPage traversal.
			std::cout << "have to split" << std::endl;
			return OK;
		}
	}

	//PIN(root_pid, root_pg);
	//PIN(leaf_pid, leaf_pg);
	//IndexPage* root_index = (IndexPage*) root_pg;
	
	//return FAIL;
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
