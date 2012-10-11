#ifndef _BTREE_FILESCAN_H
#define _BTREE_FILESCAN_H

#include "BTreeFile.h"
#include "BTreeInclude.h"

class BTreeFileScan
{
public:
	// BTreeFile is a friend, which means that it can access the private
	// members of BTreeFileScan. You should use this to initialize the
	// scan appropriately.
	friend class BTreeFile;

	typedef SortedKVPage<RecordID> LeafPage;

	// Retrieves the next (key, value) pair in the tree.
	Status GetNext(RecordID &rid, char *&keyptr);

	// Deletes the key value pair most recently returned from
	// GetNext. Note that this should delete the key value pair
	// from the appropriate leaf page, but does not need to
	// merge/redistribute keys.
	Status DeleteCurrent();

	~BTreeFileScan();

private:
	BTreeFileScan();

	// You may add private methods and variables here.
	/*Low and High keys*/
	char *low;
	char *high;

	/*Current leaf page and consecutive key between low and high*/
	LeafPage *current_leaf;
	bool currentIsDirty;
	char *current_key;
	int current_record_index;
	RecordID current_record;

	void AdvanceCurrentLeaf();
};

#endif