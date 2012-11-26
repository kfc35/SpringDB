#ifndef _HASHDIR_H
#define _HASHDIR_H

#include "page.h"
#include "new_error.h"
#include "da_types.h"
#include "DataPage.h"
#include "BufMgr.h"


class HashDirectory : public Page {

public:
	Status InitializeDirectory(PageID firstPageID, int initNumberOfBuckets);

	Status GetPageID(int index, PageID &pid);
	Status SetPageID(int index, PageID pid);
	Status GetLevel(int &Level);
	Status SetLevel(int Level);
	Status GetNext(int &nextBucket);
	Status SetNext(int bucketOffSet);
	Status GetInitNumBuckets(int &initNumBuckets);
	Status SetInitNumBuckets(int initNumBuckets);
};


#endif
