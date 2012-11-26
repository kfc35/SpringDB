#ifndef _LRU_H
#define _LRU_H

#include "db.h"

class LRU
{
private:
	struct lFrame
	{
		int f_no;
		lFrame *next;
	};

	lFrame *first;
	lFrame *last;
	int numFrames;


public:
	LRU(int n);
	~LRU();
	int PickVictim();
	void AddFrame(int f);
	void RemoveFrame(int f);
	void PrintQueue();
	int GetNumFrames();
	bool IsPresent(int f);
};

#endif