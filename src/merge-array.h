

#ifndef _MERGE_ARRAY_H_
#define _MERGE_ARRAY_H_

#include <stdio.h>
#include <stdlib.h>
#include <string.h>


typedef struct VArray{
	int rank;
  unsigned long long *dims; //size of the whole array
	unsigned long long *offset; //Coordinate
	unsigned long long *count;  
	int   datatype_size;
	void *data_buf;
}VArray;

VArray  *VArray_init(int rank, unsigned long long *dims, unsigned long long *offset, unsigned long long *count, void *data_buf, int datatype_size);

void VArray_set_value(VArray  *va, unsigned long long *coordinate, void *value_buf);

//get value from VArray [va] at [coordinate]
void VArray_get_value(VArray *va, unsigned long long *coordinate,  void *value_buf);

void VArray_merge(VArray *va_big, VArray *va_small, int flag);

void VArray_copy(int rank, 
  int datatype_size, 
  unsigned long long *dims_large, 
  unsigned long long *offset_large, 
  unsigned long long *count_large, 
  void *data_buf_large, 
  unsigned long long *dims_small, 
  unsigned long long *offset_small, 
  unsigned long long *count_small, 
  void *data_buf_small, 
  int flag);

  



#endif