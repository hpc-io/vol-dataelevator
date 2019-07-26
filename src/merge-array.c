
#include "merge-array.h"



VArray  *VArray_init(int rank, unsigned long long *dims, unsigned long long *offset, unsigned long long *count, void *data_buf, int datatype_size){
	VArray *va = malloc(sizeof(VArray));
	va->rank = rank;
	va->offset = malloc(sizeof(unsigned long long) * rank);
	va->count  = malloc(sizeof(unsigned long long) * rank);
  va->dims   = malloc(sizeof(unsigned long long) * rank);
    for(int i = 0; i < rank ; i++){
    	va->offset[i] = offset[i];
    	va->count[i] = count[i];
      va->dims[i] = dims[i];
	}
	va->data_buf = data_buf;
	va->datatype_size = datatype_size;
	return va;
}
void VArray_finalize(VArray  * va){
	if(va != NULL){
		if(va->offset != NULL) free(va->offset);
		if(va->count  != NULL) free(va->count);
    if(va->dims   != NULL) free(va->dims);
		free(va);
	}
}

//set value from VArray [va] at [coordinate]
void VArray_set_value(VArray  *va, unsigned long long *coordinate, void *value_buf){
    unsigned long long data_offset;
    for(int i = 0 ; i < va->rank ; i++){
      coordinate[i] =  coordinate[i] + va->offset[i];
    }
    data_offset = coordinate[0];
    for(int i = 0 ; i < va->rank -1; i++)
      data_offset = data_offset * va->dims[i+1] + coordinate[i+1];

  	memcpy(va->data_buf+data_offset * va->datatype_size, value_buf,  va->datatype_size);
}

//get value from VArray [va] at [coordinate]
void VArray_get_value(VArray *va, unsigned long long *coordinate,  void *value_buf){
	unsigned long long data_offset;
    for(int i = 0 ; i < va->rank ; i++){
      coordinate[i] =  coordinate[i] + va->offset[i];
    }
    data_offset = coordinate[0];
    for(int i = 0 ; i < va->rank-1; i++)
      data_offset = data_offset* va->dims[i+1] + coordinate[i+1];

    float *debug_value;
    debug_value = va->data_buf+data_offset*va->datatype_size;
    //printf("VArray_get_value, coordinate = (%lld, %lld), data_offset=%lld, value = %f \n", coordinate[0], coordinate[1], data_offset, *debug_value);
  	memcpy(value_buf,  va->data_buf+data_offset*va->datatype_size, va->datatype_size);
}

#define GET_COORDINATE(offset, dsize, dsize_len, result_coord_v, result_coord_v2){ \
  unsigned long long temp_offset = offset; \
  for (int iii = dsize_len-1; iii >= 1; iii--){ \
    result_coord_v[iii] = temp_offset % dsize[iii]; \
    result_coord_v2[iii] = result_coord_v[iii]; \
    temp_offset   = temp_offset / dsize[iii];  \
  } \
  result_coord_v[0] = temp_offset;  \
  result_coord_v2[0] = result_coord_v[0]; \
}


//Flag = 0:
// extract data from va_big [va_big->offset, va_big->offset+va_big->count] to va_small
// va_small->offset = 0,   va_small->count = va_big->offset
//Flag != 0:
//  file data from va_small to va_big
//
void VArray_merge(VArray *va_big, VArray *va_small, int flag){
	//need to check: va_small->count = va_big->count
	unsigned long long *coordinate_big = malloc(sizeof(unsigned long long) * va_small->rank);
  unsigned long long *coordinate_small = malloc(sizeof(unsigned long long) * va_small->rank);

	void *temp_value_space = malloc(va_small->datatype_size);

	unsigned long long total_cell = 1;
	for(int i = 0; i < va_small->rank; i++)
		total_cell = total_cell * va_small->count[i];
	
	for (unsigned long long j = 0; j < total_cell; j++){
 		   GET_COORDINATE(j, va_small->dims, va_small->rank, coordinate_big, coordinate_small);
 		   if(flag == 0){
			    VArray_get_value(va_big,    coordinate_big, temp_value_space);
          VArray_set_value(va_small,  coordinate_small, temp_value_space);
        }else{
          VArray_get_value(va_small,  coordinate_small, temp_value_space);
          VArray_set_value(va_big,    coordinate_big, temp_value_space);
        }
        //printf("j = %lld, coordinate=(%lld, %lld), value=%f \n ", j, coordinate_small[0], coordinate_small[1], *(float *)temp_value_space);
    }
    free(coordinate_big);
    free(coordinate_small);
}


// extract data from va_big [va_big->offset, va_big->offset+va_big->count] to va_small
// va_small->offset = 0,   va_small->count = va_big->offset
//Flag != 0:
//  file data from va_small to va_big
//
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
  int flag){

  VArray *AView = VArray_init(rank, dims_large, offset_large, count_large, data_buf_large, datatype_size);


  VArray *BView = VArray_init(rank, dims_small, offset_small, count_small, data_buf_small, datatype_size);



  VArray_merge(AView, BView, flag);

 
  VArray_finalize(AView);
  VArray_finalize(BView);
}