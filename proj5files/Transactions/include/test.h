#include "transaction.h"

public ref class T1 : TranscationExecution
{
public:
	T1(int id, HashIndex *HI) {
		T->tid = id;
		T->TSHI = new ThreadSafeHashIndex(id, HI);
	}

	void run();
};

public ref class T2 : TranscationExecution
{
public:

	T2(int id, HashIndex *HI) {
		T->tid = id;
		T->TSHI = new ThreadSafeHashIndex(id, HI);
	}

	void run();
};



public ref class T3 : TranscationExecution
{
public:
	T3(int id, HashIndex *HI) {
		T->tid = id;
		T->TSHI = new ThreadSafeHashIndex(id, HI);
	}

	void run();
};


public ref class T4 : TranscationExecution
{
public:
	T4(int id, HashIndex *HI) {
		T->tid = id;
		T->TSHI = new ThreadSafeHashIndex(id, HI);
	}

	void run();
};



public ref class T5 : TranscationExecution
{
public:
	T5(int id, HashIndex *HI) {
		T->tid = id;
		T->TSHI = new ThreadSafeHashIndex(id, HI);
	}

	void run();
};

public ref class T6 : TranscationExecution
{
public:
	T6(int id, HashIndex *HI) {
		T->tid = id;
		T->TSHI = new ThreadSafeHashIndex(id, HI);
	}

	void run();
};

public ref class T7 : TranscationExecution
{
public:
	T7(int id, HashIndex *HI) {
		T->tid = id;
		T->TSHI = new ThreadSafeHashIndex(id, HI);
	}

	void run();
};

public ref class T8 : TranscationExecution
{
public:
	T8(int id, HashIndex *HI) {
		T->tid = id;
		T->TSHI = new ThreadSafeHashIndex(id, HI);
	}

	void run();

};

public ref class T9 : TranscationExecution
{
public:
	T9(int id, HashIndex *HI) {
		T->tid = id;
		T->TSHI = new ThreadSafeHashIndex(id, HI);
	}

	void run();

};

public ref class T10 : TranscationExecution
{
public:
	T10(int id, HashIndex *HI) {
		T->tid = id;
		T->TSHI = new ThreadSafeHashIndex(id, HI);
	}

	void run();

};

public ref class T11 : TranscationExecution
{
public:
	T11(int id, HashIndex *HI) {
		T->tid = id;
		T->TSHI = new ThreadSafeHashIndex(id, HI);
	}

	void run();
};

// A no-deadlock sequence of transactions.
void test1(HashIndex *HI);

// An example of a deadlock due to simultaneous upgrades.
void test2(HashIndex *HI);

// A circular wait deadlock.
void test3(HashIndex *HI);

// A mixture of the deadlocks above.
void test4(HashIndex *HI);
