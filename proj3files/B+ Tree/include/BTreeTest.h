#ifndef _B_TREE_DRIVER_H_
#define _B_TREE_DRIVER_H_

#include "BTreeFile.h"

#define BTREE_DEFAULT_PAD 4
#define BTREE_DEFAULT_RID_OFFSET 1

class BTreeDriver
{
public:
	static void toString(const int n, char *str, int pad = BTREE_DEFAULT_PAD);

	static bool InsertKey(BTreeFile *btf, int key,
						  int ridOffset = BTREE_DEFAULT_RID_OFFSET,
						  int pad = BTREE_DEFAULT_PAD);

	static bool InsertRange(BTreeFile *btf, int low, int high,
							int ridOffset = BTREE_DEFAULT_RID_OFFSET,
							int pad = BTREE_DEFAULT_PAD,
							bool reverse = false);

	static bool InsertDuplicates(BTreeFile *btf, int key, int numDups,
								 int startOffset = BTREE_DEFAULT_RID_OFFSET,
								 int pad = BTREE_DEFAULT_PAD);


	static bool DeleteStride(BTreeFile *btf, int low, int high, int stride,
							 int pad = BTREE_DEFAULT_PAD);

	static bool TestPresent(BTreeFile *btf, int key,
							int ridOffset = BTREE_DEFAULT_RID_OFFSET,
							int pad = BTREE_DEFAULT_PAD);

	static bool TestAbsent(BTreeFile *btf, int key,
						   int ridOffset = BTREE_DEFAULT_RID_OFFSET,
						   int pad = BTREE_DEFAULT_PAD);

	static bool TestNumLeafPages(BTreeFile *btf, int expected);
	static bool TestScanCount(BTreeFileScan *scan, int expected);
	static bool TestNumEntries(BTreeFile *btf, int expected);
	
	static bool SizeForKeyOnLeafPage(ResizableRecordPage *page,
									 const char *key,
									 int &result);

	static bool TestBalance(BTreeFile *btf,
							ResizableRecordPage *left,
							ResizableRecordPage *right);

	// Larger test cases.
	static bool TestSinglePage();

	static bool TestInsertsWithLeafSplits();

	static bool TestInsertsWithIndexSplits();

	static bool TestLargeWorkload();
};

#endif
