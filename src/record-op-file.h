#ifndef DE_RECORD_OP_H
#define DE_RECORD_OP_H

#include "de-error.h"

//#define B2D_NEW_FILE_BB     000
//#define B2D_WRITE_BB        001
//#define B2D_WRITE_BB_DONE   010
//#define B2D_NEW_FILE_DISK   011
//#define B2D_WRITE_DISK      100
//#define B2D_WRITE_DISK_DONE 101

#define DE_NEW_FILE_BB 0
#define DE_WRITE_BB 1
#define DE_WRITE_BB_DONE 2
#define DE_NEW_FILE_DISK 3
#define DE_WRITE_DISK 4
#define DE_WRITE_DISK_DONE 5
#define DE_READ_CACHED 7
#define DE_READ_CACHED_DONE 8

#define DE_TYPE_K 0 //key value (disk file name)
#define DE_TYPE_S 1 //status
#define DE_TYPE_B 2 //burst buffer file name
#define DE_TYPE_E 3 //size
#define DE_TYPE_R 4 //Rank of dataset
#define DE_TYPE_C 4 //chunk size

#define DE_DB_NAME "DataElevatorMetaTable.db"

#define NAME_LENGTH 1024
#define DE_MAX_DIMS 20

extern char de_db_name_str[NAME_LENGTH];
// Define client structure (register)
typedef struct
{
  char dkname[NAME_LENGTH]; //key of the record
  char bbname[NAME_LENGTH];
  char status[NAME_LENGTH];
  char size[NAME_LENGTH];
  char groupname[NAME_LENGTH];
  char dsetname[NAME_LENGTH];
  char readdirbb[NAME_LENGTH];          //directory on BB for read cache
  int read_mpi_process;                 //the size of mpi processes to read, each read a single chunk
  int rank;                             //Rank of the dimension of the dsetname
  unsigned long long dims[DE_MAX_DIMS]; //Size of each dimension of the dsetname
  unsigned long long chks[DE_MAX_DIMS]; //Size of chunk size for each diemnsion of the dsetname
  char chkbitmap[NAME_LENGTH];          //Existing bitmap
  unsigned long long mpi_file_handle;
} DEMetaRecord;

int CreateDB();

//Function on each record
int AppendRecord(char *fnd, char *fnb, int status, int size);
//Append the record for the read file
int AppendReadRecord(const char *fnd, const char *fnb, const char *groupname, const char *dsetname, int status, int size);
//Append the record for written file
char *AppendRecordVol(const char *fnd, int status, int size);
int UpdateRecord(char *fnd, int type, char *value);
int UpdateRecordVOL(char *fnb, int type, char *value);
int QueryRecord(char *fnd, int type, char *value);
//Obtain next record with status == B2D_WRITE_BB_DONE
int NextBBDoneRecord(DEMetaRecord *d2bmetast);
int NameDB();
void AddMPIFileHandle(char *fn, unsigned long long fh);
int SetMPIFileClose(unsigned long long fh);

char *FindPreviousCreatedFileOnBB(const char *name);

//int  QueryReadRecord(char *fnd, int rank,  unsigned long long *start, unsigned long long *end, unsigned long long *dim_size, char *chunk_file_name);

int convert_cache_file_name_to_coordinate(char *chunk_file_name, unsigned long long *array_size, int array_rank, unsigned long long *chunk_start_coordinate, unsigned long long *chunk_count);

int QueryReadRecord(char *fnd, int rank, unsigned long long *start, unsigned long long *end, unsigned long long *dim_size, char ***chunk_file_name_list, int *chunk_file_name_list_size);

int NextBBPrefetchRecord(DEMetaRecord *d2bmetast, unsigned long long *chunk_size, int rank);
//int  NextBBPrefetchRecord(DEMetaRecord *d2bmetast);

int UpdateReadRecordVOLChunk(char *fn, char *gn, char *dn, int rank, unsigned long long *chks, unsigned long long *dims_size);

unsigned long long convert_start_end_offset_to_chunk_id(int rank, unsigned long long start_offset, unsigned long long end_offset, unsigned long long *dim_size, unsigned long long *chunk_size);

int merge_chunk_file_name_start_end(char *dir, unsigned long long chunk_start, unsigned long long chunk_end, char *chunk_file_name);

#define ROW_MAJOR_ORDER_MACRO(dsize, dsize_len, coordinate, offset) \
  {                                                                 \
    offset = coordinate[0];                                         \
    int iii;                                                        \
    for (iii = 1; iii < dsize_len; iii++)                           \
    {                                                               \
      offset = offset * dsize[iii] + coordinate[iii];               \
    }                                                               \
  }

#define ROW_MAJOR_ORDER_REVERSE_MACRO(offset, dsize, dsize_len, result_coord_v) \
  {                                                                             \
    unsigned long long temp_offset = offset;                                    \
    int iii;                                                                    \
    for (iii = dsize_len - 1; iii >= 1; iii--)                                  \
    {                                                                           \
      result_coord_v[iii] = temp_offset % dsize[iii];                           \
      temp_offset = temp_offset / dsize[iii];                                   \
    }                                                                           \
    result_coord_v[0] = temp_offset;                                            \
  }

#endif
