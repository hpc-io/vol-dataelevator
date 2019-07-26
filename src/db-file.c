
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <errno.h>
#include <unistd.h>
#include <stdarg.h>
#include "db-file.h"

mdt    *current_mdt_ptr;

void mdt_create(const char*file_name, unsigned int record_size){
  if(strlen(file_name) == 0)
  {
    de_error("File name for mdt_create is NULL!");
  }

  if(record_size <= 0)
  {
    de_error("Record size is <= 0 !");
  }
  
  // Create database header
  mdt_header file_header;
  file_header.record_size = record_size;
  file_header.amount = 0;
  
  // Create new file and save database header
  FILE* database_file = fopen(file_name, "wb");
  if(database_file == NULL)
  {
    de_error("Open MDT file error !");
  }
  
  fwrite(&file_header, 1, sizeof(mdt_header), database_file);
  fflush(database_file);
  fclose(database_file);
}

void         mdt_open(const char*file_name){
  if(strlen(file_name) == 0)
  {
    de_error("File name for mdt_open is NULL!");
  }

  FILE* file = fopen(file_name, "r+b");
  if(file == NULL)
  {
    de_error("Failed to open mdt file !");
  }
  
#ifdef DEBUG
  printf("Open metadata file sucessfully [%s] at BE Job ! \n", file_name);
#endif

  // Read file header
  mdt_header  file_header;
  fread(&file_header, 1, sizeof(mdt_header), file);

  current_mdt_ptr = malloc(sizeof(mdt));
  current_mdt_ptr->fp = file;
  current_mdt_ptr->file_header_size = sizeof(mdt_header);
  current_mdt_ptr->record_size = file_header.record_size;
  fseek(file, 0, SEEK_END);
  current_mdt_ptr->record_amount = (ftell(file) - current_mdt_ptr->file_header_size)/file_header.record_size;
  current_mdt_ptr->current_record_position = 0;
  //current_mdt_ptr->file_name = (char*) malloc(strlen(file_name)+1);
  //snprintf(current_mdt_ptr->file_name, strlen(file_name)+1, "%s", file_name);
}

void   mdt_close(){
  mdt_save();
  fclose(current_mdt_ptr->fp);
  //free(current_mdt_ptr->file_name);
  free(current_mdt_ptr);
}

void  mdt_save(){
  if(current_mdt_ptr == NULL){
    de_error("No metadata table is opened !");
  }
  fflush(current_mdt_ptr->fp);
}

unsigned int mdt_get_amount(){
  if(current_mdt_ptr == NULL){
    de_error("No metadata table is opened !");
  }

  return current_mdt_ptr->record_amount;
}

void  mdt_ap_record(void *data){
  if(data == NULL){
    de_error("Data is NULL in mdt_ap_record !");
  }

  if(current_mdt_ptr == NULL){
    de_error("No metadata table is opened !");
  }
  
  fseek(current_mdt_ptr->fp, 0, SEEK_END );
  char deleted = MDB_FALSE;
  fwrite(&deleted, 1, 1, current_mdt_ptr->fp);
  fwrite(data, 1, current_mdt_ptr->record_size, current_mdt_ptr->fp);
  current_mdt_ptr->current_record_position = current_mdt_ptr->record_amount;
  current_mdt_ptr->record_amount++;
}

void  mdt_write_crecord(void *data){
  if(data == NULL){
    de_error("Data is NULL in mdt_write_c_record !");
  }

  if(current_mdt_ptr == NULL){
    de_error("No metadata table is opened !");
  }

  if(current_mdt_ptr->record_amount == 0){
    de_error("Metadata table is empty, write failed !");
  }
  
  unsigned int current_record_position = current_mdt_ptr->current_record_position;
  unsigned int position = current_mdt_ptr->file_header_size +  (current_record_position*(current_mdt_ptr->record_size+1))+1;
  fseek(current_mdt_ptr->fp, position, SEEK_SET);
  fwrite(data, 1, current_mdt_ptr->record_size, current_mdt_ptr->fp);
}



void  mdt_read_crecord(void *data){
  if(data == NULL){
    de_error("Data is NULL in mdt_write_c_record !");
  }

  if(current_mdt_ptr == NULL){
    de_error("No metadata table is opened !");
  }

  if(current_mdt_ptr->record_amount == 0){
    de_error("Metadata table is empty, write failed !");
  }
  
  unsigned int current_record_position = current_mdt_ptr->current_record_position;
  unsigned int position = current_mdt_ptr->file_header_size +  (current_record_position*(current_mdt_ptr->record_size+1))+1;
  fseek(current_mdt_ptr->fp, position, SEEK_SET);
  fread(data, 1, current_mdt_ptr->record_size, current_mdt_ptr->fp);
}

mdb_bool  mdt_goto_next_crecord(){
  if(current_mdt_ptr == NULL){
    de_error("No metadata table is opened !");
  }

  if(current_mdt_ptr->record_amount == 0){
    return MDB_FALSE;
  }
  
  if(current_mdt_ptr->current_record_position  ==  (current_mdt_ptr->record_amount -1)){
    return MDB_FALSE;
  }

  current_mdt_ptr->current_record_position++;
  return MDB_TRUE;
}

mdb_bool  mdt_goto_first_record(){
  if(current_mdt_ptr == NULL){
    de_error("No metadata table is opened !");
  }
  current_mdt_ptr->current_record_position = 0;
  return MDB_TRUE;
}
