#ifndef _LRU_H
#define _LRU_H

#include "db.h"
#include "replacer.h"

class LRU : public Replacer
{
private:
	struct lFrame
	{
		int f_no;
		lFrame* next;
	};

	lFrame* first;
	lFrame* last;
	int numFrames;
	bool IsPresent(int f);
	bool IsEmpty();

public:
	LRU();
	~LRU();
	virtual int PickVictim();
	virtual void AddFrame(int f);
	virtual void RemoveFrame(int f); 
	
};

#endif