#ifndef _TSBUF_H
#define _TSBUF_H

public ref class ThreadSafeBufMgr
{
	Status PinPage(PageID pid, Page *&page, bool IsEmpty);
	Status UnpinPage(PageID pid, bool dirty);
	Status NewPage(PageID &firstPid, Page *&firstPage,int howMany);
	Status FreePage(PageID pid);
};

#endif //_TSBUF_H
