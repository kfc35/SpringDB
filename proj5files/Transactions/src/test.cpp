#include "test.h"

void T1::run()
{
	DataType v;
	T->StartTranscation();

	for (int i = 0; i < 1000; i++) {
		T->AddWritePair(i, 1000 - i, INSERT);
	}

	T->GroupWrite();
	T->CommitTransaction();
	T->EndTransaction();
}

void T2::run()
{
	DataType v;
	T->StartTranscation();

	for (int i = 0; i < 500; i++) {
		T->Read(i,v);
		System::Threading::Thread::Sleep(1);

		if (v > 1000 || v < 0) {
			cout << "Error .." << endl;
			getchar();
		}
	}

	T->GroupWrite();
	T->CommitTransaction();
	T->EndTransaction();
}

void T3::run()
{
	DataType v;
	T->StartTranscation();

	for (int i = 500; i < 1000; i++) {
		T->AddWritePair(i, -1, DELETE);
	}

	T->GroupWrite();
	T->CommitTransaction();
	T->EndTransaction();
}

void T4::run()
{
	DataType v;
	T->StartTranscation();

	for (int i = 500; i < 1000; i++) {
		T->AddWritePair(i, i, UPDATE);
	}

	T->GroupWrite();
	T->CommitTransaction();
	T->EndTransaction();
}

void T5::run()
{
	DataType v;
	T->StartTranscation();

	for (int i = 500; i < 1000; i++) {
		T->Read(i, v);

		if (v != i) {
			cout << " (" << i <<","<< v << ")" << endl;
			getchar();
		}
	}

	T->GroupWrite();
	T->CommitTransaction();
	T->EndTransaction();
}

void test1(HashIndex *HI)
{
	T1^ t1 = gcnew T1(1, HI);
	Thread^ oThread1 = gcnew Thread(gcnew ThreadStart(t1,  &T1::run));

	T2^ t2 = gcnew T2(2, HI);
	Thread^ oThread2 = gcnew Thread(gcnew ThreadStart(t2,  &T2::run));

	T3^ t3 = gcnew T3(3, HI);
	Thread^ oThread3 = gcnew Thread(gcnew ThreadStart(t3,  &T3::run));

	T2^ t4 = gcnew T2(4, HI);
	Thread^ oThread4 = gcnew Thread(gcnew ThreadStart(t4,  &T2::run));

	T2^ t5 = gcnew T2(5, HI);
	Thread^ oThread5 = gcnew Thread(gcnew ThreadStart(t5,  &T2::run));

	T2^ t6 = gcnew T2(6, HI);
	Thread^ oThread6 = gcnew Thread(gcnew ThreadStart(t6,  &T2::run));

	T4^ t7 = gcnew T4(7, HI);
	Thread^ oThread7 = gcnew Thread(gcnew ThreadStart(t7,  &T4::run));

	T5^ t8 = gcnew T5(8, HI);
	Thread^ oThread8 = gcnew Thread(gcnew ThreadStart(t8,  &T5::run));

	oThread1->Start();
	System::Threading::Thread::Sleep(10000);
	oThread2->Start();

	System::Threading::Thread::Sleep(10000);
	oThread3->Start();
	oThread4->Start();
	oThread5->Start();
	oThread6->Start();

	System::Threading::Thread::Sleep(10000);
	oThread7->Start();


	System::Threading::Thread::Sleep(10000);
	oThread8->Start();


	//getchar();
}

void T6::run()
{
	DataType v;
	T->StartTranscation();

	for (int i = 1; i < 4; i++) {
		T->AddWritePair(i, i, INSERT);
	}

	T->GroupWrite();
	T->CommitTransaction();
	T->EndTransaction();
}

void T7::run()
{
	DataType v;
	T->StartTranscation();

	for (int i = 1; i < 4; i++) {
		T->Read(i, v);

		if (v != i) {
			cout << " (" << i <<","<< v << ")" << endl;
			getchar();
		}
	}

	Thread::Sleep(1000);

	for (int i = 1; i < 4; i++) {
		T->AddWritePair(i, 2*i, UPDATE);
	}

	T->GroupWrite();
	T->CommitTransaction();
	T->EndTransaction();
}

void T8::run()
{
	DataType v;
	T->StartTranscation();

	for (int i = 1; i < 4; i++) {
		T->Read(i, v);

		if (v != i) {
			cout << " (" << i <<","<< v << ")" << endl;
			getchar();
		}
	}

	Thread::Sleep(1000);

	for (int i = 1; i < 4; i++) {
		T->AddWritePair(i, -i, UPDATE);
	}

	T->GroupWrite();
	T->CommitTransaction();
	T->EndTransaction();
}

void test2(HashIndex *HI)
{

	T6^ t6 = gcnew T6(6, HI);
	Thread^ oThread6 = gcnew Thread(gcnew ThreadStart(t6,  &T6::run));

	T7^ t7 = gcnew T7(7, HI);
	Thread^ oThread7 = gcnew Thread(gcnew ThreadStart(t7,  &T7::run));

	T8^ t8 = gcnew T8(8, HI);
	Thread^ oThread8 = gcnew Thread(gcnew ThreadStart(t8,  &T8::run));

	oThread6->Start();
	System::Threading::Thread::Sleep(2000);
	oThread7->Start();
	oThread8->Start();

	//getchar();
}

void T9::run()
{
	DataType v;
	T->StartTranscation();

	T->Read(1, v);

	Thread::Sleep(2000);

	T->AddWritePair(2, -2, UPDATE);

	T->GroupWrite();
	T->CommitTransaction();
	T->EndTransaction();
}

void T10::run()
{
	DataType v;
	T->StartTranscation();

	T->Read(2, v);

	Thread::Sleep(2000);

	T->AddWritePair(3, -3, UPDATE);

	T->GroupWrite();
	T->CommitTransaction();
	T->EndTransaction();
}

void T11::run()
{
	DataType v;
	T->StartTranscation();

	T->Read(3, v);

	Thread::Sleep(2000);

	T->AddWritePair(1, -1, UPDATE);

	T->GroupWrite();
	T->CommitTransaction();
	T->EndTransaction();
}

void test3(HashIndex *HI)
{
	T6^ t12 = gcnew T6(12, HI);
	Thread^ oThread12 = gcnew Thread(gcnew ThreadStart(t12,  &T6::run));

	T9^ t9 = gcnew T9(9, HI);
	Thread^ oThread9 = gcnew Thread(gcnew ThreadStart(t9,  &T9::run));

	T10^ t10 = gcnew T10(10, HI);
	Thread^ oThread10 = gcnew Thread(gcnew ThreadStart(t10,  &T10::run));

	T11^ t11 = gcnew T11(11, HI);
	Thread^ oThread11 = gcnew Thread(gcnew ThreadStart(t11,  &T11::run));

	oThread12->Start();
	Thread::Sleep(2000);
	oThread9->Start();
	Thread::Sleep(10);
	oThread10->Start();
	Thread::Sleep(10);
	oThread11->Start();

	//getchar();
}

void test4(HashIndex *HI)
{
	T6^ t6 = gcnew T6(6, HI);
	Thread^ oThread6 = gcnew Thread(gcnew ThreadStart(t6,  &T6::run));

	T7^ t7 = gcnew T7(7, HI);
	Thread^ oThread7 = gcnew Thread(gcnew ThreadStart(t7,  &T7::run));

	T8^ t8 = gcnew T8(8, HI);
	Thread^ oThread8 = gcnew Thread(gcnew ThreadStart(t8,  &T8::run));

	oThread6->Start();
	System::Threading::Thread::Sleep(2000);
	oThread7->Start();
	Thread::Sleep(10);
	oThread8->Start();

	T9^ t9 = gcnew T9(9, HI);
	Thread^ oThread9 = gcnew Thread(gcnew ThreadStart(t9,  &T9::run));

	T10^ t10 = gcnew T10(10, HI);
	Thread^ oThread10 = gcnew Thread(gcnew ThreadStart(t10,  &T10::run));

	T11^ t11 = gcnew T11(11, HI);
	Thread^ oThread11 = gcnew Thread(gcnew ThreadStart(t11,  &T11::run));

	System::Threading::Thread::Sleep(1000);
	oThread9->Start();
	Thread::Sleep(10);
	oThread10->Start();
	Thread::Sleep(10);
	oThread11->Start();
}
