#ifndef _clock_H
#define _clock_H

#include "db.h"

class clock_br
{
private:
	struct lFrame
	{
		int f_no;
		bool referenced;
		bool inuse;

		lFrame *next;
	};

	lFrame *first;
	lFrame *last;
	lFrame *current;
	int numFrames;
	int totalFrames;


public:
	clock_br(int n);
	~clock_br();
	int PickVictim();
	void AddFrame(int f);
	//void RemoveFrame(int f);

	bool PinFrame(int f);
	bool UnpinFrame(int f);

	void PrintQueue();
	int GetNumFrames();

	bool IsPresent(int f);
};

#endif