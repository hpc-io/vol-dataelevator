#ifndef _DATAELEVATOR_VOL_H_
#define _DATAELEVATOR_VOL_H_

/* * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * *
 * Copyright by The HDF Group.                                               *
 * All rights reserved.                                                      *
 *                                                                           *
 * This file is part of HDF5.  The full HDF5 copyright notice, including     *
 * terms governing use, modification, and redistribution, is contained in    *
 * the COPYING file, which can be found at the root of the source code       *
 * distribution tree, or in https://support.hdfgroup.org/ftp/HDF5/releases.  *
 * If you do not have access to either file, you may request a copy from     *
 * help@hdfgroup.org.                                                        *
 * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * */

/*
 * Purpose: This is a "pass through" VOL connector, which forwards each
 *      VOL callback to an underlying connector.
 *
 *      It is designed as an example VOL connector for developers to
 *      use when creating new connectors, especially connectors that
 *      are outside of the HDF5 library.  As such, it should _NOT_
 *      include _any_ private HDF5 header files.  This connector should
 *      therefore only make public HDF5 API calls and use standard C /
 *              POSIX calls.
 */

/* Header files needed */
/* (Public HDF5 and standard C / POSIX only) */
#include <assert.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "hdf5.h"

//DE Code starts
#include "merge-array.h"
#include "record-op-file.h"
#include <unistd.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <sys/types.h>

void dataset_get_wrapper(void *dset, hid_t driver_id, H5VL_dataset_get_t get_type, hid_t dxpl_id, void **req, ...);
//DE Code ends

/* Identifier for the pass-through VOL connector */
#define H5VL_DATAELEVATOR (H5VL_dataelevator_register())

/* Characteristics of the pass-through VOL connector */
#define H5VL_DATAELEVATOR_NAME "dataelevator"
#define H5VL_DATAELEVATOR_VALUE 10027 /* VOL connector ID */
#define H5VL_DATAELEVATOR_VERSION 0

/* Pass-through VOL connector info */
typedef struct H5VL_dataelevator_info_t
{
    hid_t under_vol_id;   /* VOL ID for under VOL */
    void *under_vol_info; /* VOL info for under VOL */
} H5VL_dataelevator_info_t;

#ifdef __cplusplus
extern "C"
{
#endif

    H5_DLL hid_t H5VL_dataelevator_register(void);

#ifdef __cplusplus
}
#endif

/**********/
/* Macros */
/**********/

/* Whether to display log messge when callback is invoked */
/* (Uncomment to enable) */
/* #define ENABLE_LOGGING */

/************/
/* Typedefs */
/************/

/* The pass through VOL info object */
typedef struct H5VL_dataelevator_t
{
    hid_t under_vol_id; /* ID for underlying VOL connector */
    void *under_object; /* Info object for underlying VOL connector */
    //DE Code starts
    char *de_open_file_name;
    char *de_open_group_name;
    char *de_open_dataset_name;
    //DE Code ends
} H5VL_dataelevator_t;

/* The pass through VOL wrapper context */
typedef struct H5VL_dataelevator_wrap_ctx_t
{
    hid_t under_vol_id;   /* VOL ID for under VOL */
    void *under_wrap_ctx; /* Object wrapping context for under VOL */
} H5VL_dataelevator_wrap_ctx_t;

/********************* */
/* Function prototypes */
/********************* */

/* Helper routines */
herr_t H5VL_dataelevator_file_specific_reissue(void *obj, hid_t connector_id,
                                               H5VL_file_specific_t specific_type, hid_t dxpl_id, void **req, ...);
herr_t H5VL_dataelevator_request_specific_reissue(void *obj, hid_t connector_id,
                                                  H5VL_request_specific_t specific_type, ...);
H5VL_dataelevator_t *H5VL_dataelevator_new_obj(void *under_obj,
                                               hid_t under_vol_id);
herr_t H5VL_dataelevator_free_obj(H5VL_dataelevator_t *obj);

/* "Management" callbacks */
herr_t H5VL_dataelevator_init(hid_t vipl_id);
herr_t H5VL_dataelevator_term(void);
void *H5VL_dataelevator_info_copy(const void *info);
herr_t H5VL_dataelevator_info_cmp(int *cmp_value, const void *info1, const void *info2);
herr_t H5VL_dataelevator_info_free(void *info);
herr_t H5VL_dataelevator_info_to_str(const void *info, char **str);
herr_t H5VL_dataelevator_str_to_info(const char *str, void **info);

static void *H5VL_dataelevator_get_object(const void *obj);
static herr_t H5VL_dataelevator_get_wrap_ctx(const void *obj, void **wrap_ctx);
static void *H5VL_dataelevator_wrap_object(void *under_under_in, H5I_type_t obj_type, void *wrap_ctx);
static void *H5VL_dataelevator_unwrap_object(void *under);
static herr_t H5VL_dataelevator_free_wrap_ctx(void *obj);

/* Dataset callbacks */
void *H5VL_dataelevator_dataset_create(void *obj, const H5VL_loc_params_t *loc_params, const char *name, hid_t lcpl_id, hid_t type_id, hid_t space_id, hid_t dcpl_id, hid_t dapl_id, hid_t dxpl_id, void **req);
void *H5VL_dataelevator_dataset_open(void *obj, const H5VL_loc_params_t *loc_params, const char *name, hid_t dapl_id, hid_t dxpl_id, void **req);
herr_t H5VL_dataelevator_dataset_read(void *dset, hid_t mem_type_id, hid_t mem_space_id,
                                      hid_t file_space_id, hid_t plist_id, void *buf, void **req);
herr_t H5VL_dataelevator_dataset_write(void *dset, hid_t mem_type_id, hid_t mem_space_id, hid_t file_space_id, hid_t plist_id, const void *buf, void **req);
herr_t H5VL_dataelevator_dataset_get(void *dset, H5VL_dataset_get_t get_type, hid_t dxpl_id, void **req, va_list arguments);
herr_t H5VL_dataelevator_dataset_specific(void *obj, H5VL_dataset_specific_t specific_type, hid_t dxpl_id, void **req, va_list arguments);
herr_t H5VL_dataelevator_dataset_optional(void *obj, hid_t dxpl_id, void **req, va_list arguments);
herr_t H5VL_dataelevator_dataset_close(void *dset, hid_t dxpl_id, void **req);

/* File callbacks */
void *H5VL_dataelevator_file_create(const char *name, unsigned flags, hid_t fcpl_id, hid_t fapl_id, hid_t dxpl_id, void **req);
void *H5VL_dataelevator_file_open(const char *name, unsigned flags, hid_t fapl_id, hid_t dxpl_id, void **req);
herr_t H5VL_dataelevator_file_get(void *file, H5VL_file_get_t get_type, hid_t dxpl_id, void **req, va_list arguments);
herr_t H5VL_dataelevator_file_specific(void *file, H5VL_file_specific_t specific_type, hid_t dxpl_id, void **req, va_list arguments);
herr_t H5VL_dataelevator_file_optional(void *file, hid_t dxpl_id, void **req, va_list arguments);
herr_t H5VL_dataelevator_file_close(void *file, hid_t dxpl_id, void **req);

/* Group callbacks */
void *H5VL_dataelevator_group_create(void *obj, const H5VL_loc_params_t *loc_params, const char *name, hid_t lcpl_id, hid_t gcpl_id, hid_t gapl_id, hid_t dxpl_id, void **req);
void *H5VL_dataelevator_group_open(void *obj, const H5VL_loc_params_t *loc_params, const char *name, hid_t gapl_id, hid_t dxpl_id, void **req);
herr_t H5VL_dataelevator_group_get(void *obj, H5VL_group_get_t get_type, hid_t dxpl_id, void **req, va_list arguments);
herr_t H5VL_dataelevator_group_specific(void *obj, H5VL_group_specific_t specific_type, hid_t dxpl_id, void **req, va_list arguments);
herr_t H5VL_dataelevator_group_optional(void *obj, hid_t dxpl_id, void **req, va_list arguments);
herr_t H5VL_dataelevator_group_close(void *grp, hid_t dxpl_id, void **req);

extern const H5VL_class_t H5VL_dataelevator_g;

#endif