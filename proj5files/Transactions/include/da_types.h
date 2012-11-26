#ifndef da_types_h
#define da_types_h

#ifndef NULL
#define NULL 0
#endif

typedef unsigned long ulong;
typedef unsigned int uint;
typedef unsigned short ushort;
typedef unsigned char uchar;
typedef unsigned int HashValue;
typedef unsigned int KeyType;
typedef unsigned int DataType;
typedef unsigned int TransactionID;

enum ErrorCode
{
	ErrRANGE, ErrMEM, ErrNULLPTR, ErrSIZE, ErrCOPY
};

void FatalError(ErrorCode ec);
#endif
