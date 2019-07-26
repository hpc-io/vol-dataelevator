#include "record-op-file.h"
#include "db-file.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <mpi.h> /* MPI and MPI-IO live here */
#include <libgen.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h> /* getopt lives here */
#include <assert.h>
#include "tinydir.h"

char de_mdb_name_str[NAME_LENGTH];

//return
//1 file exists
//0 file does not exist
int file_exist(char *filename)
{
  struct stat buffer;
  return (stat(filename, &buffer) == 0);
}

//auto set this flag to indicate that DE is working
//it here works for READ only
int de_global_working_flag = 0;

int CreateDB()
{
  int my_rank;
  static int create_flag = 0;

  MPI_Comm_rank(MPI_COMM_WORLD, &my_rank);
  //printf("In create db file !\n");
  if (my_rank == 0 && create_flag == 0)
  {
    //DEMetaRecord d2bmetast;
    // Create database
    mdt_create(de_mdb_name_str, sizeof(DEMetaRecord));
    //db_CloseAll();
    //mdt_close();
    create_flag = 1;
    printf("Create Data Elevator metadat table [%s]\n", de_mdb_name_str);
  }

  return 0;
}

//return
//1 file exists
//0 file does not exist
int DBExistingCheck()
{
  //int me;
  //MPI_Comm_rank(MPI_COMM_WORLD, &me);
  //printf("At rank %d: db name= %s \n", me, de_mdb_name_str);
  return file_exist(de_mdb_name_str);
}

int NameDB()
{
  int my_rank;
  static int updated_flag = 0;
  MPI_Comm_rank(MPI_COMM_WORLD, &my_rank);
  //Use root (rank == 0) process to update name once
  //Comment this line for netcdf test
  //if (my_rank == 0 && updated_flag == 0){
  if (updated_flag == 0)
  {
    const char *bb_path = getenv("DW_JOB_STRIPED");
    if (bb_path != NULL)
    {
      sprintf(de_mdb_name_str, "%s/%s", bb_path, DE_DB_NAME);
    }
    else
    {
      const char *de_path = getenv("DE_DATA_DIR");
      if (de_path != NULL)
      {
        sprintf(de_mdb_name_str, "%s/%s", de_path, DE_DB_NAME);
      }
      else
      {
        sprintf(de_mdb_name_str, "./%s", DE_DB_NAME);
      }
    }

    if (my_rank == 0)
      printf("Naming the metadata table: %s \n", de_mdb_name_str);
    updated_flag = 1;
  }
  return 0;
}

int AppendRecord(char *fnd, char *fnb, int status, int size)
{
  // Open database
  int my_rank;
  MPI_Comm_rank(MPI_COMM_WORLD, &my_rank);
  if ((my_rank == 0) && (de_global_working_flag == 1))
  {
    mdt_open(de_mdb_name_str);
    DEMetaRecord d2bmetast;
    sprintf(d2bmetast.status, "%d", DE_NEW_FILE_BB);
    sprintf(d2bmetast.size, "%d", 0);
    strncpy(d2bmetast.bbname, fnb, sizeof(d2bmetast.bbname));
    strncpy(d2bmetast.dkname, fnd, sizeof(d2bmetast.dkname));
    //printf("append: bb name %s, dk name %s \n", d2bmetast.bbname, d2bmetast.dkname);
    printf("Rediect [write] Disk file [%s] to BB file [%s] by Data Elevator \n", fnd, fnb);
    mdt_ap_record(&d2bmetast);
    mdt_close();
  }
  return 0;
}

void path_merge(const char *p1, const char *p2, const char *p3, const char *p4, char *output)
{
  //add path
  while (*p1 != '\0')
    *output++ = *p1++;
  if (*(output - 1) != '/')
    *output++ = '/'; //delete '/' at the end if

  //add file name
  while (*p2 != '\0')
    *output++ = *p2++;

  //add "-dir"
  *output++ = '-';
  *output++ = 'd';
  *output++ = 'e';
  *output++ = '-';
  *output++ = 'd';
  *output++ = 'i';
  *output++ = 'r';

  //Group always stats with a '/'
  //if(*(output-1) != '/')   *output++='/';
  while (*p3 != '\0')
    *output++ = *p3++;
  if (*(output - 1) != '/')
    *output++ = '/';

  while (*p4 != '\0')
    *output++ = *p4++;

  //if(*(output-1) != '/')  *output++='/';
  *output++ = '/';
  *output = '\0';
}

#define PATH_MAX_DE 2046

void de_mkdir_recursive(const char *path, mode_t mode)
{
  char opath[PATH_MAX_DE];
  char *p;
  size_t len;

  strncpy(opath, path, sizeof(opath));
  opath[sizeof(opath) - 1] = '\0';
  len = strlen(opath);
  if (len == 0)
    return;
  else if (opath[len - 1] == '/')
    opath[len - 1] = '\0';
  for (p = opath; *p; p++)
    if (*p == '/')
    {
      *p = '\0';
      if (access(opath, F_OK))
        mkdir(opath, mode);
      *p = '/';
    }
  if (access(opath, F_OK)) /* if path is not terminated with / */
    mkdir(opath, mode);
}

//Here fnb is the directory to Burst Buffer
int AppendReadRecord(const char *fnd, const char *fnb, const char *groupname, const char *dsetname, int status, int size)
{
  // Open database
  de_global_working_flag = 1;
  int my_rank;
  MPI_Comm_rank(MPI_COMM_WORLD, &my_rank);
  if (my_rank == 0)
  {
    mdt_open(de_mdb_name_str);
    DEMetaRecord d2bmetast;
    //db_Open(b2d_db_name_str, "b2ddb");
    //D2BMetaSt d2bmetast;

    sprintf(d2bmetast.status, "%d", status);
    sprintf(d2bmetast.size, "%d", 0);
    strncpy(d2bmetast.dkname, fnd, sizeof(d2bmetast.dkname));
    strncpy(d2bmetast.groupname, groupname, sizeof(d2bmetast.groupname));
    strncpy(d2bmetast.dsetname, dsetname, sizeof(d2bmetast.dsetname));
    strncpy(d2bmetast.bbname, fnb, sizeof(d2bmetast.bbname));
    int read_mpi_process;
    MPI_Comm_size(MPI_COMM_WORLD, &read_mpi_process);
    d2bmetast.read_mpi_process = read_mpi_process;
    d2bmetast.rank = 0;
    int i;
    for (i = 0; i < DE_MAX_DIMS; i++)
    {
      d2bmetast.dims[i] = 0;
      d2bmetast.chks[i] = 0;
    }
    //Insert some code to get the only file name

    char *filename_only = basename(fnd);
    //char *filename_only_final = malloc(strlen(filename_only)+strlen("_de_cahe_dir")+1);//+1 for the null-terminator
    //char *full_path = malloc(strlen(fnb)+1+strlen(fnd)+4+strlen(groupname)+1+strlen(dsetname)+1);
    char *full_path = malloc(strlen(fnb) + 1 + strlen(filename_only) + 7 + strlen(groupname) + 1 + strlen(dsetname) + 1);
    path_merge(fnb, filename_only, groupname, dsetname, full_path);
    printf("full_path at rank 0 : %s (filename = %s) \n", full_path, filename_only);
    de_mkdir_recursive(full_path, S_IRWXU);
    strncpy(d2bmetast.readdirbb, full_path, sizeof(d2bmetast.readdirbb));
    //printf("append: bb name %s, dk name %s \n", d2bmetast.bbname, d2bmetast.dkname);
    printf("Rediect read disk file [%s] to bb file [%s] by DataElevator, DIR on BB is: %s \n", fnd, fnb, full_path);
    mdt_ap_record(&d2bmetast);
    mdt_close();
    free(full_path);
    return 0;
  }

  return 0;
  //create directory with fnb
  //create directoreis with  groupname
  //https://stackoverflow.com/questions/2336242/recursive-mkdir-system-call-on-unix
}

char *AppendRecordVol(const char *fnd, int status, int size)
{
  de_global_working_flag = 1;

  // Open database
  int my_rank;
  char *fnb = (char *)malloc(sizeof(char) * NAME_LENGTH);
  char *ts2 = strdup(fnd);
  char *filename = basename(ts2);
  const char *bb_path = getenv("DW_JOB_STRIPED");
  if (bb_path != NULL)
  {
    sprintf(fnb, "%s/%s.on.bb", bb_path, filename);
  }
  else
  {
    const char *de_path = getenv("DE_DATA_DIR");
    if (de_path != NULL)
    {
      sprintf(de_mdb_name_str, "%s/%s", de_path, DE_DB_NAME);
    }
    else
    {
      sprintf(fnb, "./%s.on.bb", filename);
    }
  }

  MPI_Comm_rank(MPI_COMM_WORLD, &my_rank);
  if (my_rank == 0)
  {
    mdt_open(de_mdb_name_str);
    DEMetaRecord d2bmetast;
    sprintf(d2bmetast.status, "%d", DE_NEW_FILE_BB);
    sprintf(d2bmetast.size, "%d", 0);
    strncpy(d2bmetast.bbname, fnb, sizeof(d2bmetast.bbname));
    strncpy(d2bmetast.dkname, fnd, sizeof(d2bmetast.dkname));
    printf("Redirect [write] to Disk file [%s] to BB file [%s] by Data Elevator\n", fnd, fnb);
    //printf("append new record: bb name %s, dk name %s \n", d2bmetast.bbname, d2bmetast.dkname);
    mdt_ap_record(&d2bmetast);
    mdt_close();
  }

  return fnb;
}

int UpdateRecord(char *fnd, int type, char *value)
{
  int my_rank;
  MPI_Comm_rank(MPI_COMM_WORLD, &my_rank);
  if ((my_rank == 0) && (de_global_working_flag == 1))
  {
    //db_Open(b2d_db_name_str, "b2ddb");
    mdt_open(de_mdb_name_str);
    DEMetaRecord d2bmetast;
    mdt_goto_first_record();
    do
    {
      mdt_read_crecord(&d2bmetast);
      //printf("1 fnd = %s, fn =  %s \n", fnd, d2bmetast.dkname);
      if ((strcmp(d2bmetast.dkname, fnd) == 0))
      {
        switch (type)
        {
        case DE_TYPE_K:
          strncpy(d2bmetast.dkname, value, sizeof(d2bmetast.dkname));
          mdt_write_crecord(&d2bmetast);
          break;
        case DE_TYPE_S:
          strncpy(d2bmetast.status, value, sizeof(d2bmetast.status));
          //printf("In UpdateRecord: fn =  %s, Status = %s \n", d2bmetast.dkname, value);
          mdt_write_crecord(&d2bmetast);
          break;
        case DE_TYPE_B:
          strncpy(d2bmetast.bbname, value, sizeof(d2bmetast.bbname));
          mdt_write_crecord(&d2bmetast);
          break;
        case DE_TYPE_E:
          strncpy(d2bmetast.size, value, sizeof(d2bmetast.size));
          mdt_write_crecord(&d2bmetast);
          break;
        default:
          printf("Unknow type in QueryRecord ! \n");
          break;
        }
      }
    } while (mdt_goto_next_crecord() != MDB_FALSE);

    mdt_close();
  }
  return 0;
}

int UpdateRecordVOL(char *fnb, int type, char *value)
{
  int my_rank;
  MPI_Comm_rank(MPI_COMM_WORLD, &my_rank);
  int current_status, new_status;
  if (my_rank == 0)
  {
    mdt_open(de_mdb_name_str);
    DEMetaRecord d2bmetast;
    mdt_goto_first_record();
    do
    {
      mdt_read_crecord(&d2bmetast);
      //printf("1 fnd = %s, fn =  %s \n", fnd, d2bmetast.dkname);
      if ((strcmp(d2bmetast.bbname, fnb) == 0))
      {
        switch (type)
        {
        case DE_TYPE_K:
          strncpy(d2bmetast.dkname, value, sizeof(d2bmetast.dkname));
          mdt_write_crecord(&d2bmetast);
          break;
        case DE_TYPE_S:
          //We never go back with status
          current_status = atoi(d2bmetast.status);
          new_status = atoi(value);
          if (current_status < new_status)
          {
            strncpy(d2bmetast.status, value, sizeof(d2bmetast.status));
            //printf("In UpdateRecord: fn =  %s, Status = %s \n", d2bmetast.dkname, value);
            mdt_write_crecord(&d2bmetast);
          }
          break;
        case DE_TYPE_B:
          strncpy(d2bmetast.bbname, value, sizeof(d2bmetast.bbname));
          mdt_write_crecord(&d2bmetast);
          break;
        case DE_TYPE_E:
          strncpy(d2bmetast.size, value, sizeof(d2bmetast.size));
          mdt_write_crecord(&d2bmetast);
          break;
        default:
          printf("Unknow type in QueryRecord ! \n");
          break;
        }
      }
    } while (mdt_goto_next_crecord() != MDB_FALSE);

    mdt_close();
  }
  return 0;
}

int UpdateReadRecordVOL(char *fn, char *gn, char *dn, int type, char *value)
{
  int my_rank;
  MPI_Comm_rank(MPI_COMM_WORLD, &my_rank);
  if ((my_rank == 0) && (de_global_working_flag == 1))
  {
    mdt_open(de_mdb_name_str);
    DEMetaRecord d2bmetast;
    mdt_goto_first_record();
    do
    {
      mdt_read_crecord(&d2bmetast);
      //printf("1 fnd = %s, fn =  %s \n", fnd, d2bmetast.dkname);
      if ((strcmp(d2bmetast.dkname, fn) == 0) && (strcmp(d2bmetast.groupname, gn) == 0) && (strcmp(d2bmetast.dsetname, dn) == 0))
      {
        switch (type)
        {
        case DE_TYPE_K:
          strncpy(d2bmetast.dkname, value, sizeof(d2bmetast.dkname));
          mdt_write_crecord(&d2bmetast);
          break;
        case DE_TYPE_S:
          strncpy(d2bmetast.status, value, sizeof(d2bmetast.status));
          //printf("In UpdateRecord: fn =  %s, Status = %s \n", d2bmetast.dkname, value);
          mdt_write_crecord(&d2bmetast);
          break;
        case DE_TYPE_B:
          strncpy(d2bmetast.bbname, value, sizeof(d2bmetast.bbname));
          mdt_write_crecord(&d2bmetast);
          break;
        case DE_TYPE_E:
          strncpy(d2bmetast.size, value, sizeof(d2bmetast.size));
          mdt_write_crecord(&d2bmetast);
          break;
        default:
          printf("Unknow type in QueryRecord ! \n");
          break;
        }
      }
    } while (mdt_goto_next_crecord() != MDB_FALSE);

    mdt_close();
  }
  return 0;
}

int QueryRecord(char *fnd, int type, char *value)
{
  int my_rank;
  MPI_Comm_rank(MPI_COMM_WORLD, &my_rank);
  if (my_rank == 0)
  {
    mdt_open(de_mdb_name_str);
    DEMetaRecord d2bmetast;
    mdt_goto_first_record();
    do
    {
      mdt_read_crecord(&d2bmetast);
      if (strcmp(d2bmetast.dkname, fnd) == 0)
      {
        switch (type)
        {
        case DE_TYPE_K:
          strncpy(value, d2bmetast.dkname, sizeof(d2bmetast.dkname));
          break;
        case DE_TYPE_S:
          strncpy(value, d2bmetast.status, sizeof(d2bmetast.status));
          break;
        case DE_TYPE_B:
          strncpy(value, d2bmetast.bbname, sizeof(d2bmetast.bbname));
          break;
        case DE_TYPE_E:
          strncpy(value, d2bmetast.size, sizeof(d2bmetast.size));
          break;
        default:
          printf("Unknow type in QueryRecord ! \n");
          break;
        }
      }
    } while (mdt_goto_next_crecord() != MDB_FALSE);

    mdt_close();
  }
  return 0;
}

char *FindPreviousCreatedFileOnBB(const char *name)
{
  // int my_rank;
  //MPI_Comm_rank(MPI_COMM_WORLD, &my_rank);
  char *fnb = (char *)malloc(sizeof(char) * NAME_LENGTH);
  int status;
  if (de_global_working_flag == 0)
    return NULL;

  //if (my_rank == 0){
  mdt_open(de_mdb_name_str);
  DEMetaRecord d2bmetast;
  mdt_goto_first_record();
  do
  {
    mdt_read_crecord(&d2bmetast);
    status = atoi(d2bmetast.status);
    //Find the file with the name and its status is finished by application
    // printf("Name to find: %s, names existing: %s, status = %d \n", name, d2bmetast.dkname, status);
    if (strcmp(d2bmetast.dkname, name) == 0 && status <= DE_WRITE_DISK_DONE && status >= DE_WRITE_BB_DONE)
    {
      sprintf(fnb, "%s", d2bmetast.bbname);
      return fnb;
    }
  } while (mdt_goto_next_crecord() != MDB_FALSE);
  mdt_close();
  //}
  free(fnb);
  return NULL;
}

int create_chunk_file(char *filename)
{
  FILE *ck_file = fopen(filename, "wb");
  //printf("create chunk file %s \n ", filename);
  fclose(ck_file);
  return 0;
}

unsigned long long linear_coordiate(int rank, unsigned long long *coord, unsigned long long *array_size)
{

  unsigned long long offset = coord[0];
  int i;
  for (i = 1; i < rank; i++)
  {
    offset = offset * array_size[i] + coord[i];
  }
  //printf("Read rank: %d, cood = (%llu, %llu), array_size=(%llu, %u), offset = %llu\n ", rank, coord[0], coord[1], array_size[0], array_size[1], offset);
  return offset;
}

int merge_chunk_file_name_start_end(char *dir, unsigned long long chunk_start, unsigned long long chunk_end, char *chunk_file_name)
{
  while (*dir != '\0')
    *chunk_file_name++ = *dir++;
  //find the length
  int n = snprintf(NULL, 0, "%llu-%llu", chunk_start, chunk_end);
  //put chunk start as the end
  snprintf(chunk_file_name, n + 1, "%llu-%llu", chunk_start, chunk_end);
  chunk_file_name = chunk_file_name + n;

  *chunk_file_name = '\0';
  return 0;
}

/* int merge_chunk_file_name_id(char *dir, char *dset, unsigned long long chunk_id, char *chunk_file_name){ */
/*   while (*dir != '\0'){ */
/*     *chunk_file_name ++= *dir++; */
/*   } */

/*   //while (*dset != '\0') */
/*   //  *chunk_file_name ++= *dset++; */
/*   //\*chunk_file_name ++='-'; */
/*   //find the length */
/*   int n = snprintf(NULL, 0, "%llu", chunk_id,); */
/*   //put chunk start as the end */
/*   snprintf(chunk_file_name, n+1,  "%llu", chunk_id); */
/*   chunk_file_name = chunk_file_name+n; */

/*   *chunk_file_name='\0'; */
/* } */

//return values
// 1: chunk file existing
// otherwise: no
int chunk_existing_on_bb(char *chunk_file)
{
  int ret = 0;
  if (file_exist(chunk_file) == 1)
    ret = 1;
  //printf("chunk file : %s , existing ret =%d \n", chunk_file, ret);
  return ret;
}

int convert_cache_file_name_to_coordinate(char *chunk_file_name, unsigned long long *array_size, int array_rank, unsigned long long *chunk_start_coordinate, unsigned long long *chunk_count)
{
  unsigned long long chunk_start_offset, chunk_end_offset;

  unsigned long long *chunk_end_coordinate = (unsigned long long *)malloc(sizeof(unsigned long long) * array_rank);

  char *token = strtok(chunk_file_name, "-");
  chunk_start_offset = strtoull(token, NULL, 10);
  token = strtok(NULL, "-");
  chunk_end_offset = strtoull(token, NULL, 10);

  ROW_MAJOR_ORDER_REVERSE_MACRO(chunk_start_offset, array_size, array_rank, chunk_start_coordinate);
  ROW_MAJOR_ORDER_REVERSE_MACRO(chunk_end_offset, array_size, array_rank, chunk_end_coordinate);

  int i;
  for (i = 0; i < array_rank; i++)
    chunk_count[i] = chunk_end_coordinate[i] - chunk_start_coordinate[i] + 1;

  free(chunk_end_coordinate);
  return 0;
}

//Check a (small) chunk is located within another (large) chunk
// 1: small chunk is contained within large chunk
// 0: small chunk is not contained within large chunk
int chunk_matched_within(unsigned long long *array_size, int array_rank, unsigned long long small_chunk_start_offset, unsigned long long small_chunk_end_offset, unsigned long long large_chunk_start_offset, unsigned long long large_chunk_end_offset)
{

  unsigned long long *small_chunk_start_coordinate = (unsigned long long *)malloc(sizeof(unsigned long long) * array_rank);
  unsigned long long *small_chunk_end_coordinate = (unsigned long long *)malloc(sizeof(unsigned long long) * array_rank);
  unsigned long long *large_chunk_start_coordinate = (unsigned long long *)malloc(sizeof(unsigned long long) * array_rank);
  unsigned long long *large_chunk_end_coordinate = (unsigned long long *)malloc(sizeof(unsigned long long) * array_rank);

  ROW_MAJOR_ORDER_REVERSE_MACRO(small_chunk_start_offset, array_size, array_rank, small_chunk_start_coordinate);
  ROW_MAJOR_ORDER_REVERSE_MACRO(small_chunk_end_offset, array_size, array_rank, small_chunk_end_coordinate);
  ROW_MAJOR_ORDER_REVERSE_MACRO(large_chunk_start_offset, array_size, array_rank, large_chunk_start_coordinate);
  ROW_MAJOR_ORDER_REVERSE_MACRO(large_chunk_end_offset, array_size, array_rank, large_chunk_end_coordinate);

  //printf("array size = (%llu, %llu), array_rank = %d, small chunk : (%llu, %llu) -> (%llu, %llu), large chunk: (%llu, %llu) -> (%llu, %llu) \n", array_size[0], array_size[1], array_rank, small_chunk_start_coordinate[0], small_chunk_start_coordinate[1], small_chunk_end_coordinate[0], small_chunk_end_coordinate[1], large_chunk_start_coordinate[0], large_chunk_start_coordinate[1], large_chunk_end_coordinate[0], large_chunk_end_coordinate[1]);

  int i, matched_within_flag = 0;
  for (i = 0; i < array_rank; i++)
  {
    if ((small_chunk_start_coordinate[i] >= large_chunk_start_coordinate[i]) && (small_chunk_end_coordinate[i] <= large_chunk_end_coordinate[i]))
    {
      matched_within_flag = 1;
    }
    else
    {
      matched_within_flag = 0;
      break;
    }
  }

  free(small_chunk_start_coordinate);
  free(small_chunk_end_coordinate);
  free(large_chunk_start_coordinate);
  free(large_chunk_end_coordinate);

  return matched_within_flag;
}

//Todo add merge multiple chunks

//return values
// 1: chunk file existing
// 2: chunk is within another chunk
// 3: chunk of multiple chunks
// otherwise: no
int chunk_existing_on_bb_patitial(char *chunk_file, unsigned long long start_offset, unsigned long long end_offset, int array_rank, unsigned long long *array_size, char ***chunk_file_list, int *chunk_file_list_size, char *d2bmetast_readdirbb, unsigned long long *d2bmetast_chunk_size)
{
  int my_rank;
  MPI_Comm_rank(MPI_COMM_WORLD, &my_rank);

#ifdef DEBUG
  if (my_rank == 0)
    printf("check existing partiial:  %s \n! ", chunk_file);
#endif
  int ret = 0;
  if (file_exist(chunk_file) == 1)
  {
    *chunk_file_list_size = 1;
    *chunk_file_list = (char **)malloc(sizeof(char *) * 1);
    (*chunk_file_list)[0] = (char *)malloc(sizeof(char) * NAME_LENGTH);
    strncpy((*chunk_file_list)[0], chunk_file, NAME_LENGTH);

#ifdef DEBUG
    if (my_rank == 0)
      printf("Exact: find the cached file %s (chunk_file_path_name = %s, n file = %d)\n !", (*chunk_file_list)[0], chunk_file, *chunk_file_list_size);
#endif
    return 1;
  }

  if (my_rank == 0)
    printf("not Exact match  \n! ");

  //Go to find the partial chunk
  //Get all chunks cached.
  //First, target area is within a current chunk
  //Second, target area has multiple chunks.
  tinydir_dir dir;
  int i;
  unsigned long long *chunk_start_offset, *chunk_end_offset;
  tinydir_open_sorted(&dir, d2bmetast_readdirbb);
  //No prefetch file
  if (dir.n_files < 2)
    return 0;

  chunk_start_offset = malloc(dir.n_files * sizeof(unsigned long long));
  chunk_end_offset = malloc(dir.n_files * sizeof(unsigned long long));
  int chunk_file_id_size = 0;
  for (i = 0; i < dir.n_files; i++)
  {
    tinydir_file file;
    tinydir_readfile_n(&dir, &file, i);
    if ((!file.is_dir) && strcmp(file.name, ".") && strcmp(file.name, ".."))
    {
      if (file._s.st_size <= 0)
      {
        //printf("Skip zero sized file %s \n", file.name);
        continue;
      }
      //printf("%s ( ", file.name);
      char *token = strtok(file.name, "-");
      chunk_start_offset[chunk_file_id_size] = strtoull(token, NULL, 10);
      token = strtok(NULL, "-");
      chunk_end_offset[chunk_file_id_size] = strtoull(token, NULL, 10);
      chunk_file_id_size++;
    }
  }
  tinydir_close(&dir);

  //No prefetch file
  if (chunk_file_id_size == 0)
  {
    free(chunk_start_offset);
    free(chunk_end_offset);
    chunk_start_offset = NULL;
    chunk_end_offset = NULL;
    return 0;
  }

  char *chunk_file_path_name = malloc(sizeof(char) * NAME_LENGTH);

  //Search for the first case: target area is within a cached chunk
  for (i = 0; i < chunk_file_id_size; i++)
  {
    if (chunk_matched_within(array_size, array_rank, start_offset, end_offset, chunk_start_offset[i], chunk_end_offset[i]) == 1)
    {
      merge_chunk_file_name_start_end(d2bmetast_readdirbb, chunk_start_offset[i], chunk_end_offset[i], chunk_file_path_name);
      //if(file_exist(chunk_file_path_name) == 1){
      *chunk_file_list_size = 1;
      *chunk_file_list = (char **)malloc(sizeof(char *) * 1);
      (*chunk_file_list)[0] = (char *)malloc(sizeof(char) * NAME_LENGTH);
      strncpy((*chunk_file_list)[0], chunk_file_path_name, NAME_LENGTH);
      if (my_rank == 0)
        printf("Interval: find the cached file %s (chunk_file_path_name = %s, n file = %d)\n! ", (*chunk_file_list)[0], chunk_file_path_name, *chunk_file_list_size);
      free(chunk_file_path_name);
      chunk_file_path_name = NULL;
      free(chunk_start_offset);
      free(chunk_end_offset);
      chunk_start_offset = NULL;
      chunk_end_offset = NULL;
      return 2;
      //}
    }
  }

  if (my_rank == 0)
    printf("not Within match. array_rank = %d,  chunk size = (%llu, %llu) \n", array_rank, d2bmetast_chunk_size[0], d2bmetast_chunk_size[1]);

  //Search for the second case: target area contains multiple chunks.
  //Here I only consider the chunks WITHOUT ghost zone
  int subchunk_exsiting_flag = 1, subchunk_accounts = 1;
  unsigned long long *start_coordinate = malloc(sizeof(unsigned long long) * array_rank);
  unsigned long long *end_coordinate = malloc(sizeof(unsigned long long) * array_rank);
  unsigned long long *subchunk_start_coordinate = malloc(sizeof(unsigned long long) * array_rank);
  unsigned long long *subchunk_end_coordinate = malloc(sizeof(unsigned long long) * array_rank);
  unsigned long long *subchunk_chunk_size = malloc(sizeof(unsigned long long) * array_rank);
  unsigned long long *subchunk_chunk_coordinate = malloc(sizeof(unsigned long long) * array_rank);

  ROW_MAJOR_ORDER_REVERSE_MACRO(start_offset, array_size, array_rank, start_coordinate);
  ROW_MAJOR_ORDER_REVERSE_MACRO(end_offset, array_size, array_rank, end_coordinate);

  //How many chunks of my target area
  for (i = 0; i < array_rank; i++)
  {
    if ((end_coordinate[i] - start_coordinate[i] + 1) < d2bmetast_chunk_size[i])
      goto return_with_zero;
    subchunk_chunk_size[i] = (end_coordinate[i] - start_coordinate[i] + 1) / d2bmetast_chunk_size[i];
    subchunk_accounts = subchunk_accounts * subchunk_chunk_size[i];
  }

  if (my_rank == 0)
    printf("not Within match. subchunk chunk_size = (%llu, %llu), subchunk_accounts = %d, chunk_file_id_size = %d \n", subchunk_chunk_size[0], subchunk_chunk_size[1], subchunk_accounts, chunk_file_id_size);

  if (chunk_file_id_size < subchunk_accounts)
  {
    free(start_coordinate);
    free(end_coordinate);
    free(subchunk_start_coordinate);
    free(subchunk_end_coordinate);
    free(subchunk_chunk_size);
    free(subchunk_chunk_coordinate);
    free(chunk_start_offset);
    free(chunk_end_offset);
    chunk_start_offset = NULL;
    chunk_end_offset = NULL;
    return 0;
  }

  //Go through each chunks within target area
  unsigned long long subchunk_start_offset, subchunk_end_offset;
  *chunk_file_list_size = subchunk_accounts;
  *chunk_file_list = (char **)malloc(sizeof(char *) * subchunk_accounts);
  int chunk_file_list_index = 0, j;
  for (i = 0; i < subchunk_accounts; i++)
  {
    ROW_MAJOR_ORDER_REVERSE_MACRO(i, subchunk_chunk_size, array_rank, subchunk_chunk_coordinate);
    for (j = 0; j < array_rank; j++)
    {
      subchunk_start_coordinate[j] = start_coordinate[j] + subchunk_chunk_coordinate[j] * d2bmetast_chunk_size[j];
      subchunk_end_coordinate[j] = subchunk_start_coordinate[j] + d2bmetast_chunk_size[j] - 1;
    }

    memset(chunk_file_path_name, 0, NAME_LENGTH);
    subchunk_start_offset = linear_coordiate(array_rank, subchunk_start_coordinate, array_size);
    subchunk_end_offset = linear_coordiate(array_rank, subchunk_end_coordinate, array_size);
    merge_chunk_file_name_start_end(d2bmetast_readdirbb, subchunk_start_offset, subchunk_end_offset, chunk_file_path_name);
    if (my_rank == 0)
    {
      printf("Exact: check the %d-th cached file subchunk_chunk_coordinate = (%llu, %llu), subchunk_start_coordinate=(%llu, %llu), subchunk_end_coordinate=(%llu, %llu), chunk_file_path_name=%s !\n", i, subchunk_chunk_coordinate[0], subchunk_chunk_coordinate[1], subchunk_start_coordinate[0], subchunk_start_coordinate[1], subchunk_end_coordinate[0], subchunk_end_coordinate[1], chunk_file_path_name);
    }

    if (file_exist(chunk_file_path_name) == 0)
    {
      subchunk_exsiting_flag = 0;
      break;
    }
    else
    {
      (*chunk_file_list)[chunk_file_list_index] = (char *)malloc(sizeof(char) * NAME_LENGTH);
      strncpy((*chunk_file_list)[chunk_file_list_index], chunk_file_path_name, NAME_LENGTH);
      if (my_rank == 0)
      {
        printf("Exact: find the %d-th cached file %s (chunk_file_path_name = %s, n file = %d) !\n", chunk_file_list_index, (*chunk_file_list)[chunk_file_list_index], chunk_file_path_name, *chunk_file_list_size);
        fflush(stdout);
      }
      chunk_file_list_index++;
    }
  }

  if (chunk_file_path_name != NULL)
  {
    free(chunk_file_path_name);
    chunk_file_path_name = NULL;
  }
  free(start_coordinate);
  free(end_coordinate);
  free(subchunk_start_coordinate);
  free(subchunk_end_coordinate);
  free(subchunk_chunk_size);
  free(subchunk_chunk_coordinate);
  free(chunk_start_offset);
  free(chunk_end_offset);
  chunk_start_offset = NULL;
  chunk_end_offset = NULL;

  if (subchunk_exsiting_flag == 1)
  {
    return 3;
  }
  else
  {
    for (i = 0; i < chunk_file_list_index; i++)
    {
      if ((*chunk_file_list)[i] != NULL)
      {
        free((*chunk_file_list)[i]);
        (*chunk_file_list)[i] = NULL;
      }
    }
    if (*chunk_file_list != NULL)
    {
      free(*chunk_file_list);
      *chunk_file_list = NULL;
    }
    *chunk_file_list_size = 0;
  }

return_with_zero:
  if (my_rank == 0)
    printf("not Without match  \n! ");

  return 0;
}

unsigned long long find_target_chunk_file_id(int rank, unsigned long long *start, unsigned long long *end, unsigned long long *dim_size)
{
  int i;
  unsigned long long *chunk_size, *chunk_coordinate, *chunk_dim_size;
  chunk_size = malloc(rank * sizeof(unsigned long long));
  chunk_coordinate = malloc(rank * sizeof(unsigned long long));
  chunk_dim_size = malloc(rank * sizeof(unsigned long long));
  //In this for loop, we assume that array is evenly divided by a chunk
  //The chunk size is euqal to the read size (end-start)
  //In other words, each read consumes a single chunk
  for (i = 0; i < rank; i++)
  {
    chunk_size[i] = end[i] - start[i] + 1;
    chunk_coordinate[i] = (start[i] + end[i]) / 2; //Use the center to represent this chunk
    chunk_coordinate[i] = chunk_coordinate[i] / chunk_size[i];
    chunk_dim_size[i] = dim_size[i] / chunk_size[i];
  }

  //printf("Read rank: %d, cood = (%u, %u), array_size=(%u, %u), offset = %u\n ", rank, coord[0], coord[1], array_size[0], array_size[1], offset);

  unsigned long long ret = linear_coordiate(rank, chunk_coordinate, chunk_dim_size);
  free(chunk_size);
  free(chunk_coordinate);
  free(chunk_dim_size);
  return ret;
}

unsigned long long convert_start_end_offset_to_chunk_id(int rank, unsigned long long start_offset, unsigned long long end_offset, unsigned long long *dim_size, unsigned long long *chunk_size)
{
  int i;
  unsigned long long *chunk_coordinate, *chunk_dim_size, *start_coordinate, *end_coordinate;
  chunk_coordinate = malloc(rank * sizeof(unsigned long long));
  chunk_dim_size = malloc(rank * sizeof(unsigned long long));
  start_coordinate = malloc(rank * sizeof(unsigned long long));
  end_coordinate = malloc(rank * sizeof(unsigned long long));

  //In this for loop, we assume that array is evenly divided by a chunk
  //The chunk size is euqal to the read size (end-start)
  //In other words, each read consumes a single chunk
  ROW_MAJOR_ORDER_REVERSE_MACRO(start_offset, dim_size, rank, start_coordinate);
  ROW_MAJOR_ORDER_REVERSE_MACRO(end_offset, dim_size, rank, end_coordinate);

  for (i = 0; i < rank; i++)
  {
    chunk_dim_size[i] = dim_size[i] / chunk_size[i];
    chunk_coordinate[i] = (start_coordinate[i] + end_coordinate[i]) / (2 * chunk_size[i]);
  }
  //printf("data rank = %d, dims = (%llu, %llu), chunk_size = (%llu, %llu), chunk_dim_size= (%llu, %llu), chunk_coord=(%llu, %llu), dim/c = %llu \n",rank, dim_size[0],dim_size[1], chunk_size[0], chunk_size[1], chunk_dim_size[0], chunk_dim_size[1], chunk_coordinate[0], chunk_coordinate[1],  dim_size[0]/chunk_size[0]);
  unsigned long long ret = linear_coordiate(rank, chunk_coordinate, chunk_dim_size);
  free(chunk_coordinate);
  free(chunk_dim_size);
  free(start_coordinate);
  free(end_coordinate);
  return ret;
}

// return values
//  1: find cached chunk file (one-to-one matched)
//     2: find cached chunk file (one-to-one contained within)
//     3: find cahced chunk files (one-to-many containing many files)
//  0: chunk file is not cached on BB
// -1: some error happens
int QueryReadRecord(char *fnd, int rank, unsigned long long *start, unsigned long long *end, unsigned long long *dim_size, char ***chunk_file_name_list, int *chunk_file_name_list_size)
{
  DEMetaRecord d2bmetast;
  int status, ret = 0, ret_global = 0;
  if (file_exist(de_mdb_name_str) != 1)
  {
    goto retrun_ret;
  }
  mdt_open(de_mdb_name_str);
  int s = mdt_get_amount();
  if (s == 0)
  {
    mdt_close();
    goto retrun_ret;
  }
  mdt_goto_first_record();
  do
  {
    mdt_read_crecord(&d2bmetast);
    if (strcmp(d2bmetast.dkname, fnd) == 0 && atoi(d2bmetast.status) == DE_READ_CACHED)
    {
      //go to chenk the chunk
      //printf("In QueryReadRecord rank == %d, chks[0] = %d \n", d2bmetast.rank, d2bmetast.chks[0]);
      //if(d2bmetast.rank <= 0 ) goto retrun_ret;
      unsigned long long ii;
      for (ii = 0; ii < rank; ii++)
      {
        if (d2bmetast.chks[ii] <= 0)
          goto retrun_ret;
      }

      //for(ii =0 ; ii < d2bmetast.rank; ii++){
      //if(d2bmetast.dims[ii] <=  0)  goto retrun_ret;
      //}

      char *chunk_file_path_nmae = malloc(NAME_LENGTH);
      //unsigned long long chunk_offset_l = find_target_chunk_file_id(rank, start, end, dim_size);
      //merge_chunk_file_name(d2bmetast.readdirbb, d2bmetast.dsetname,  chunk_offset_l,  chunk_file_path_nmae);
      unsigned long long chunk_offset_start = linear_coordiate(rank, start, dim_size);
      unsigned long long chunk_offset_end = linear_coordiate(rank, end, dim_size);
      merge_chunk_file_name_start_end(d2bmetast.readdirbb, chunk_offset_start, chunk_offset_end, chunk_file_path_nmae);

      int my_rank;
      MPI_Comm_rank(MPI_COMM_WORLD, &my_rank);

#ifdef DEBUG
      if (my_rank == 0)
        printf("In QueryReadRecord  chunk_file_path_nmae = %s \n", chunk_file_path_nmae);
#endif
      //strncpy(chunk_file_name, chunk_file_path_nmae, NAME_LENGTH);
      //if(chunk_existing_on_bb(chunk_file_path_nmae) == 1){
      ret = chunk_existing_on_bb_patitial(chunk_file_path_nmae, chunk_offset_start, chunk_offset_end, rank, dim_size, chunk_file_name_list, chunk_file_name_list_size, d2bmetast.readdirbb, d2bmetast.chks);

      //Todo: disable the create chunk file
      //if(ret == 0){
      //create the chunk file, we assume read will happen soon
      //create_chunk_file(chunk_file_path_nmae);
      //}
      if (chunk_file_path_nmae != NULL)
      {
        free(chunk_file_path_nmae);
        chunk_file_path_nmae = NULL;
      }
      break;
    }
  } while (mdt_goto_next_crecord() != MDB_FALSE);
  mdt_close();

retrun_ret:
  MPI_Allreduce(&ret, &ret_global, 1, MPI_INT, MPI_MIN, MPI_COMM_WORLD);
  return ret_global;
}

// return values
//  0: find some finished record
//  1: no one is finished (or no record)
// -1: some error happens
int NextBBDoneRecord(DEMetaRecord *d2bmetast)
{
  //D2BMetaSt d2bmetast;
  int status, ret = 1;
  if (file_exist(de_mdb_name_str) != 1)
  {
    return ret;
  }

  mdt_open(de_mdb_name_str);

  int s = mdt_get_amount();
  if (s == 0)
  {
    mdt_close();
    return ret;
  }

  mdt_goto_first_record();
  do
  {
    mdt_read_crecord(d2bmetast);
    status = atoi(d2bmetast->status);
    //printf("In NextBBDoneRecord -> Status id = %d, dkname=%s , bbname =%s\n", status, d2bmetast->dkname, d2bmetast->bbname);
    if (status == DE_WRITE_BB_DONE)
    {
      ret = 0;
      break;
    }
  } while (mdt_goto_next_crecord() != MDB_FALSE);

  mdt_close();

  return ret;
}

// return values
//  0: find some finished record
//  1: no one is finished (or no record)
// -1: some error happens
int NextBBPrefetchRecord(DEMetaRecord *d2bmetast, unsigned long long *chunk_size, int rank)
{
  //D2BMetaSt d2bmetast;
  int status, ret = 1, i;
  printf("DB file: %s \n", de_mdb_name_str);
  if (file_exist(de_mdb_name_str) != 1)
  {
    return ret;
  }

  mdt_open(de_mdb_name_str);

  int s = mdt_get_amount();
  if (s == 0)
  {
    mdt_close();
    return ret;
  }

  mdt_goto_first_record();
  do
  {
    mdt_read_crecord(d2bmetast);
    status = atoi(d2bmetast->status);
    //printf("In NextBBDoneRecord -> Status id = %d \n", status);
    if (status == DE_READ_CACHED)
    {
      for (i = 0; i < rank; i++)
        d2bmetast->chks[i] = chunk_size[i];
      mdt_write_crecord(d2bmetast);
      ret = 0;
      break;
    }
  } while (mdt_goto_next_crecord() != MDB_FALSE);

  mdt_close();

  return ret;
}

// return values
//  1: find the file, and bbname contains the temp tile's name on BB
//  0: no
int DBRecordExistCheck(char *dkname, char *bbname)
{
  DEMetaRecord d2bmetast;
  int status, ret = 0;
  if (file_exist(de_mdb_name_str) != 1)
  {
    return ret;
  }
  mdt_open(de_mdb_name_str);
  int s = mdt_get_amount();
  if (s == 0)
  {
    mdt_close();
    return ret;
  }

  mdt_goto_first_record();
  do
  {
    mdt_read_crecord(&d2bmetast);
    //status = atoi(d2bmetast->status);
    //printf("In NextBBDoneRecord -> Status id = %d \n", status);
    if (strcmp(d2bmetast.dkname, dkname) == 0)
    {
      ret = 1;
      //printf("Found the name : %s \n",d2bmetast.bbname);
      //bbname = strdup(d2bmetast.bbname);
      strcpy(bbname, d2bmetast.bbname);
      break;
    }
  } while (mdt_goto_next_crecord() != MDB_FALSE);

  mdt_close();

  return ret;
}

void AddMPIFileHandle(char *fn, unsigned long long fh)
{
  int my_rank;
  MPI_Comm_rank(MPI_COMM_WORLD, &my_rank);
  if (my_rank == 0)
  {
    mdt_open(de_mdb_name_str);
    DEMetaRecord d2bmetast;
    mdt_goto_first_record();
    do
    {
      mdt_read_crecord(&d2bmetast);
      printf("In Add Handle: target file handle = %lld, target fn =  %s, current fs=%s \n", fh, fn, d2bmetast.dkname);
      if (strcmp(d2bmetast.bbname, fn) == 0)
      {
        d2bmetast.mpi_file_handle = fh;
        mdt_write_crecord(&d2bmetast);
        break;
      }
    } while (mdt_goto_next_crecord() != MDB_FALSE);
    mdt_close();
  }
}

int SetMPIFileClose(unsigned long long fh)
{
  int my_rank;
  MPI_Comm_rank(MPI_COMM_WORLD, &my_rank);
  if (my_rank == 0)
  {
    mdt_open(de_mdb_name_str);
    DEMetaRecord d2bmetast;
    mdt_goto_first_record();
    do
    {
      mdt_read_crecord(&d2bmetast);
      printf("Set close  fh = %lld, fn =  %s, current fh %lld \n", fh, d2bmetast.dkname, d2bmetast.mpi_file_handle);
      if (d2bmetast.mpi_file_handle == fh)
      {
        char value[NAME_LENGTH];
        //printf("update status for %s with %d \n", bb_filename,B2D_WRITE_BB_DONE);
        sprintf(value, "%d", DE_WRITE_BB_DONE);
        strncpy(d2bmetast.status, value, sizeof(d2bmetast.status));
        printf("In UpdateRecord: fn =  %s, Status = %s \n", d2bmetast.dkname, value);
        mdt_write_crecord(&d2bmetast);
        break;
      }
    } while (mdt_goto_next_crecord() != MDB_FALSE);
    mdt_close();
  }
  return 0;
}

int UpdateReadRecordVOLChunk(char *fn, char *gn, char *dn, int rank, unsigned long long *chks, unsigned long long *dims_size)
{
  int my_rank;
  static int set_chunk_size_flag = 0; //Only set up chunk_size once

  MPI_Comm_rank(MPI_COMM_WORLD, &my_rank);
  if (my_rank == 0 && set_chunk_size_flag == 0)
  {
    mdt_open(de_mdb_name_str);
    DEMetaRecord d2bmetast;
    mdt_goto_first_record();
    do
    {
      mdt_read_crecord(&d2bmetast);
      //printf("1 fnd = %s, fn =  %s \n", fnd, d2bmetast.dkname);
      if ((strcmp(d2bmetast.dkname, fn) == 0) && (strcmp(d2bmetast.groupname, gn) == 0) && (strcmp(d2bmetast.dsetname, dn) == 0))
      {
        d2bmetast.rank = rank;
        assert(rank < DE_MAX_DIMS);
        int i;
        for (i = 0; i < rank; i++)
        {
          d2bmetast.chks[i] = chks[i];
          d2bmetast.dims[i] = dims_size[i];
        }
        mdt_write_crecord(&d2bmetast);
        set_chunk_size_flag = 1;
        printf("dataset_read: rank = %d, chunk size = (%llu, %llu)\n", rank, d2bmetast.chks[0], d2bmetast.chks[1]);
      }
    } while (mdt_goto_next_crecord() != MDB_FALSE);

    mdt_close();
  }
  return 0;
}

/* int  read_block_from_bb(int file_name, void *buf, int type_size, size_t buf_size){ */
/*   FILE *fp; */
/*   fp=fopen(file_name, "rb"); */
/*   printf("size of buf = %d \n", sizeof(*buf)); */
/*   fread(buf, type_size, buf_size, fp); */
/*   fclose(fp); */
/* } */
