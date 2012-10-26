#ifndef _B_TREE_FILE_H_
#define _B_TREE_FILE_H_

#include "BTreeheaderPage.h"
#include "BTreeFileScan.h"
#include "BTreeTest.h"
#include "BTreeInclude.h"

class BTreeFile
{
public:
	friend class BTreeDriver;

	BTreeFile(Status &status, const char *filename);

	Status DestroyFile();

	~BTreeFile();

	Status Insert(const char *key, const RecordID rid);

	BTreeFileScan *OpenScan(const char *lowKey, const char *highKey);

	Status PrintTree(PageID pageID, bool printContents);
	Status PrintWhole(bool printContents = false);

private:
	BTreeHeaderPage *header;

	//Please don't delete this method. It's used for testing,
	// and may be useful for you.
	PageID GetLeftLeaf();

	Status FreeTree(PageID root_pid);
	void SplitIndex(IndexPage* newPage, IndexPage* oldPage, const char* newKey, PageID newValue, char*& newIndexKey, PageID& newIndexValue);
	void SplitPage(LeafPage* newPage, LeafPage* oldPage, const char* newKey, RecordID newValue);

	//Added variable: filename
	char *fname;
};


#endif