#ifndef DE_MDB_FILE_H_
#define DE_MDB_FILE_H_

#include <stdio.h>
#include "de-error.h"

typedef enum
{
  MDB_FALSE, /**< Boolean value true.*/
  MDB_TRUE,  /**< Boolean value false.*/
} mdb_bool;

typedef struct mdt_header_struct
{
  unsigned int record_size; // record size in bytes
  unsigned int amount;
} mdt_header;

typedef struct mdt_struct
{
  char *file_name;                      //File name to store metadata record
  FILE *fp;                             //Pointer to the opened file
  unsigned int file_header_size;        //Size of header
  unsigned int record_size;             //Size of each record
  unsigned int record_amount;           //The amount of records
  unsigned int current_record_position; //Pointer to current record
} mdt;

void mdt_create(const char *file_name, unsigned int record_size);
void mdt_open(const char *file_name);
void mdt_close();
unsigned int mdt_get_amount();
void mdt_save();

void mdt_ap_record(void *data);
void mdt_write_crecord(void *data);
void mdt_read_crecord(void *data);

mdb_bool mdt_goto_next_crecord();
mdb_bool mdt_goto_first_record();
#endif
