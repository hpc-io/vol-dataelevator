#include "data_elevator_vol.h"

/*******************/
/* Local variables */
/*******************/

/* Pass through VOL connector class struct */
const H5VL_class_t H5VL_dataelevator_g = {
    H5VL_DATAELEVATOR_VERSION,                   /* version      */
    (H5VL_class_value_t)H5VL_DATAELEVATOR_VALUE, /* value        */
    H5VL_DATAELEVATOR_NAME,                      /* name         */
    0,                                           /* capability flags */
    H5VL_dataelevator_init,                      /* initialize   */
    H5VL_dataelevator_term,                      /* terminate    */
    {
        sizeof(H5VL_dataelevator_info_t), /* info size    */
        H5VL_dataelevator_info_copy,      /* info copy    */
        H5VL_dataelevator_info_cmp,       /* info compare */
        H5VL_dataelevator_info_free,      /* info free    */
        H5VL_dataelevator_info_to_str,    /* info to str  */
        H5VL_dataelevator_str_to_info,    /* str to info  */
    },
    {
        H5VL_dataelevator_get_object,    /* get_object   */
        H5VL_dataelevator_get_wrap_ctx,  /* get_wrap_ctx */
        H5VL_dataelevator_wrap_object,   /* wrap_object  */
        H5VL_dataelevator_unwrap_object, /* unwrap_object */
        H5VL_dataelevator_free_wrap_ctx, /* free_wrap_ctx */
    },
    {
        /* attribute_cls */
        NULL, /* create */
        NULL, /* open */
        NULL, /* read */
        NULL, /* write */
        NULL, /* get */
        NULL, /* specific */
        NULL, /* optional */
        NULL  /* close */
    },
    {
        /* dataset_cls */
        H5VL_dataelevator_dataset_create,   /* create */
        H5VL_dataelevator_dataset_open,     /* open */
        H5VL_dataelevator_dataset_read,     /* read */
        H5VL_dataelevator_dataset_write,    /* write */
        H5VL_dataelevator_dataset_get,      /* get */
        H5VL_dataelevator_dataset_specific, /* specific */
        H5VL_dataelevator_dataset_optional, /* optional */
        H5VL_dataelevator_dataset_close     /* close */
    },
    {
        /* datatype_cls */
        NULL, /* commit */
        NULL, /* open */
        NULL, /* get_size */
        NULL, /* specific */
        NULL, /* optional */
        NULL  /* close */
    },
    {
        /* file_cls */
        H5VL_dataelevator_file_create,   /* create */
        H5VL_dataelevator_file_open,     /* open */
        H5VL_dataelevator_file_get,      /* get */
        H5VL_dataelevator_file_specific, /* specific */
        H5VL_dataelevator_file_optional, /* optional */
        H5VL_dataelevator_file_close     /* close */
    },
    {
        /* group_cls */
        H5VL_dataelevator_group_create,   /* create */
        H5VL_dataelevator_group_open,     /* open */
        H5VL_dataelevator_group_get,      /* get */
        H5VL_dataelevator_group_specific, /* specific */
        H5VL_dataelevator_group_optional, /* optional */
        H5VL_dataelevator_group_close     /* close */
    },
    {
        /* link_cls */
        NULL, /* create */
        NULL, /* copy */
        NULL, /* move */
        NULL, /* get */
        NULL, /* specific */
        NULL, /* optional */
    },
    {
        /* object_cls */
        NULL, /* open */
        NULL, /* copy */
        NULL, /* get */
        NULL, /* specific */
        NULL, /* optional */
    },
    {
        /* request_cls */
        NULL, /* wait */
        NULL, /* notify */
        NULL, /* cancel */
        NULL, /* specific */
        NULL, /* optional */
        NULL  /* free */
    },
    NULL /* optional */
};

H5PL_type_t H5PLget_plugin_type(void) { return H5PL_TYPE_VOL; }
const void *H5PLget_plugin_info(void) { return &H5VL_dataelevator_g; }

int de_create_flag__indicator = 0;
int de_prefetch_flag__indicator = 0;

/* The connector identification number, initialized at runtime */
hid_t H5VL_DATAELEVATOR_g = H5I_INVALID_HID;

void dataset_get_wrapper(void *dset, hid_t driver_id, H5VL_dataset_get_t get_type, hid_t dxpl_id, void **req, ...)
{
    va_list args;
    va_start(args, req);
    H5VLdataset_get(dset, driver_id, get_type, dxpl_id, req, args);
}

/*-------------------------------------------------------------------------
 * Function:    H5VL__dataelevator_new_obj
 *
 * Purpose: Create a new pass through object for an underlying object
 *
 * Return:  Success:    Pointer to the new pass through object
 *      Failure:    NULL
 *
 * Programmer:  Quincey Koziol
 *              Monday, December 3, 2018
 *
 *-------------------------------------------------------------------------
 */
H5VL_dataelevator_t *
H5VL_dataelevator_new_obj(void *under_obj, hid_t under_vol_id)
{
    H5VL_dataelevator_t *new_obj;

    new_obj = (H5VL_dataelevator_t *)calloc(1, sizeof(H5VL_dataelevator_t));
    new_obj->under_object = under_obj;
    new_obj->under_vol_id = under_vol_id;
    //new_obj->de_open_file_name =  NULL;
    //new_obj->de_open_group_name = NULL;
    //new_obj->de_open_dataset_name = NULL;
    H5Iinc_ref(new_obj->under_vol_id);

    return (new_obj);
} /* end H5VL__dataelevator_new_obj() */

/*-------------------------------------------------------------------------
 * Function:    H5VL__dataelevator_free_obj
 *
 * Purpose: Release a pass through object
 *
 * Return:  Success:    0
 *      Failure:    -1
 *
 * Programmer:  Quincey Koziol
 *              Monday, December 3, 2018
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5VL_dataelevator_free_obj(H5VL_dataelevator_t *obj)
{
    hid_t err_id = H5Eget_current_stack();

    H5Idec_ref(obj->under_vol_id);

    H5Eset_current_stack(err_id);

    free(obj);

    return (0);
} /* end H5VL__dataelevator_free_obj() */

/*-------------------------------------------------------------------------
 * Function:    H5VL_dataelevator_register
 *
 * Purpose: Register the pass-through VOL connector and retrieve an ID
 *      for it.
 *
 * Return:  Success:    The ID for the pass-through VOL connector
 *      Failure:    -1
 *
 * Programmer:  Quincey Koziol
 *              Wednesday, November 28, 2018
 *
 *-------------------------------------------------------------------------
 */
hid_t H5VL_dataelevator_register(void)
{
    /* Clear the error stack */
    //H5Eclear2(H5E_DEFAULT);

    /* Singleton register the pass-through VOL connector ID */
    if (H5VL_DATAELEVATOR_g < 0)
        H5VL_DATAELEVATOR_g = H5VLregister_connector(&H5VL_dataelevator_g, H5P_DEFAULT);

    return H5VL_DATAELEVATOR_g;
} /* end H5VL_dataelevator_register() */

/*-------------------------------------------------------------------------
 * Function:    H5VL_dataelevator_init
 *
 * Purpose:     Initialize this VOL connector, performing any necessary
 *      operations for the connector that will apply to all containers
 *              accessed with the connector.
 *
 * Return:  Success:    0
 *      Failure:    -1
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5VL_dataelevator_init(hid_t vipl_id)
{
#ifdef ENABLE_LOGGING
    printf("------- PASS THROUGH VOL INIT\n");
#endif

    /* Shut compiler up about unused parameter */
    vipl_id = vipl_id;

    return (0);
} /* end H5VL_dataelevator_init() */

/*---------------------------------------------------------------------------
 * Function:    H5VL_dataelevator_term
 *
 * Purpose:     Terminate this VOL connector, performing any necessary
 *      operations for the connector that release connector-wide
 *      resources (usually created / initialized with the 'init'
 *      callback).
 *
 * Return:  Success:    0
 *      Failure:    (Can't fail)
 *
 *---------------------------------------------------------------------------
 */
herr_t
H5VL_dataelevator_term(void)
{
#ifdef ENABLE_LOGGING
    printf("------- PASS THROUGH VOL TERM\n");
#endif

    /* Reset VOL ID */
    H5VL_DATAELEVATOR_g = H5I_INVALID_HID;

    return (0);
} /* end H5VL_dataelevator_term() */

/*---------------------------------------------------------------------------
 * Function:    H5VL_dataelevator_info_copy
 *
 * Purpose:     Duplicate the connector's info object.
 *
 * Returns: Success:    New connector info object
 *      Failure:    NULL
 *
 *---------------------------------------------------------------------------
 */
void *
H5VL_dataelevator_info_copy(const void *_info)
{
    const H5VL_dataelevator_info_t *info = (const H5VL_dataelevator_info_t *)_info;
    H5VL_dataelevator_info_t *new_info;

#ifdef ENABLE_LOGGING
    printf("------- PASS THROUGH VOL INFO Copy\n");
#endif

    /* Allocate new VOL info struct for the pass through connector */
    new_info = (H5VL_dataelevator_info_t *)calloc(1, sizeof(H5VL_dataelevator_info_t));

    /* Increment reference count on underlying VOL ID, and copy the VOL info */
    new_info->under_vol_id = info->under_vol_id;
    H5Iinc_ref(new_info->under_vol_id);
    if (info->under_vol_info)
        H5VLcopy_connector_info(new_info->under_vol_id, &(new_info->under_vol_info), info->under_vol_info);

    return (new_info);
} /* end H5VL_dataelevator_info_copy() */

/*---------------------------------------------------------------------------
 * Function:    H5VL_dataelevator_info_cmp
 *
 * Purpose:     Compare two of the connector's info objects, setting *cmp_value,
 *      following the same rules as strcmp().
 *
 * Return:  Success:    0
 *      Failure:    -1
 *
 *---------------------------------------------------------------------------
 */
herr_t
H5VL_dataelevator_info_cmp(int *cmp_value, const void *_info1, const void *_info2)
{
    const H5VL_dataelevator_info_t *info1 = (const H5VL_dataelevator_info_t *)_info1;
    const H5VL_dataelevator_info_t *info2 = (const H5VL_dataelevator_info_t *)_info2;

#ifdef ENABLE_LOGGING
    printf("------- PASS THROUGH VOL INFO Compare\n");
#endif

    /* Sanity checks */
    assert(info1);
    assert(info2);

    /* Initialize comparison value */
    *cmp_value = 0;

    /* Compare under VOL connector classes */
    H5VLcmp_connector_cls(cmp_value, info1->under_vol_id, info2->under_vol_id);
    if (*cmp_value != 0)
        return (0);

    /* Compare under VOL connector info objects */
    H5VLcmp_connector_info(cmp_value, info1->under_vol_id, info1->under_vol_info, info2->under_vol_info);
    if (*cmp_value != 0)
        return (0);

    return (0);
} /* end H5VL_dataelevator_info_cmp() */

/*---------------------------------------------------------------------------
 * Function:    H5VL_dataelevator_info_free
 *
 * Purpose:     Release an info object for the connector.
 *
 * Return:  Success:    0
 *      Failure:    -1
 *
 *---------------------------------------------------------------------------
 */
herr_t
H5VL_dataelevator_info_free(void *_info)
{
    H5VL_dataelevator_info_t *info = (H5VL_dataelevator_info_t *)_info;
    hid_t err_id;

#ifdef ENABLE_LOGGING
    printf("------- PASS THROUGH VOL INFO Free\n");
#endif
    err_id = H5Eget_current_stack();

    /* Release underlying VOL ID and info */
    if (info->under_vol_info)
        H5VLfree_connector_info(info->under_vol_id, info->under_vol_info);
    H5Idec_ref(info->under_vol_id);

    H5Eset_current_stack(err_id);

    /* Free pass through info object itself */
    free(info);

    return (0);
} /* end H5VL_dataelevator_info_free() */

/*---------------------------------------------------------------------------
 * Function:    H5VL_dataelevator_info_to_str
 *
 * Purpose:     Serialize an info object for this connector into a string
 *
 * Return:  Success:    0
 *      Failure:    -1
 *
 *---------------------------------------------------------------------------
 */
herr_t
H5VL_dataelevator_info_to_str(const void *_info, char **str)
{
    const H5VL_dataelevator_info_t *info = (const H5VL_dataelevator_info_t *)_info;
    H5VL_class_value_t under_value = (H5VL_class_value_t)-1;
    char *under_vol_string = NULL;
    size_t under_vol_str_len = 0;

#ifdef ENABLE_LOGGING
    printf("------- PASS THROUGH VOL INFO To String\n");
#endif

    /* Get value and string for underlying VOL connector */
    H5VLget_value(info->under_vol_id, &under_value);
    H5VLconnector_info_to_str(info->under_vol_info, info->under_vol_id, &under_vol_string);

    /* Determine length of underlying VOL info string */
    if (under_vol_string)
        under_vol_str_len = strlen(under_vol_string);

    /* Allocate space for our info */
    *str = (char *)H5allocate_memory(32 + under_vol_str_len, (hbool_t)0);
    assert(*str);

    /* Encode our info */
    snprintf(*str, 32 + under_vol_str_len, "under_vol=%u;under_info={%s}", (unsigned)under_value, (under_vol_string ? under_vol_string : ""));

    return (0);
} /* end H5VL_dataelevator_info_to_str() */

/*---------------------------------------------------------------------------
 * Function:    H5VL_dataelevator_str_to_info
 *
 * Purpose:     Deserialize a string into an info object for this connector.
 *
 * Return:  Success:    0
 *      Failure:    -1
 *
 *---------------------------------------------------------------------------
 */
herr_t
H5VL_dataelevator_str_to_info(const char *str, void **_info)
{
    H5VL_dataelevator_info_t *info;
    unsigned under_vol_value;
    const char *under_vol_info_start, *under_vol_info_end;
    hid_t under_vol_id;
    void *under_vol_info = NULL;

#ifdef ENABLE_LOGGING
    printf("------- PASS THROUGH VOL INFO String To Info\n");
#endif

    /* Retrieve the underlying VOL connector value and info */
    sscanf(str, "under_vol=%u;", &under_vol_value);
    under_vol_id = H5VLregister_connector_by_value((H5VL_class_value_t)under_vol_value, H5P_DEFAULT);
    under_vol_info_start = strchr(str, '{');
    under_vol_info_end = strrchr(str, '}');
    assert(under_vol_info_end > under_vol_info_start);
    if (under_vol_info_end != (under_vol_info_start + 1))
    {
        char *under_vol_info_str;

        under_vol_info_str = (char *)malloc((size_t)(under_vol_info_end - under_vol_info_start));
        memcpy(under_vol_info_str, under_vol_info_start + 1, (size_t)((under_vol_info_end - under_vol_info_start) - 1));
        *(under_vol_info_str + (under_vol_info_end - under_vol_info_start)) = '\0';

        H5VLconnector_str_to_info(under_vol_info_str, under_vol_id, &under_vol_info);

        free(under_vol_info_str);
    } /* end else */

    /* Allocate new pass-through VOL connector info and set its fields */
    info = (H5VL_dataelevator_info_t *)calloc(1, sizeof(H5VL_dataelevator_info_t));
    info->under_vol_id = under_vol_id;
    info->under_vol_info = under_vol_info;

    /* Set return value */
    *_info = info;

    return (0);
} /* end H5VL_dataelevator_str_to_info() */

/*---------------------------------------------------------------------------
 * Function:    H5VL_dataelevator_get_object
 *
 * Purpose:     Retrieve the 'data' for a VOL object.
 *
 * Return:  Success:    0
 *      Failure:    -1
 *
 *---------------------------------------------------------------------------
 */
void *
H5VL_dataelevator_get_object(const void *obj)
{
    const H5VL_dataelevator_t *o = (const H5VL_dataelevator_t *)obj;

#ifdef ENABLE_LOGGING
    printf("------- PASS THROUGH VOL Get object\n");
#endif

    return H5VLget_object(o->under_object, o->under_vol_id);
} /* end H5VL_dataelevator_get_object() */

/*---------------------------------------------------------------------------
 * Function:    H5VL_dataelevator_get_wrap_ctx
 *
 * Purpose:     Retrieve a "wrapper context" for an object
 *
 * Return:  Success:    0
 *      Failure:    -1
 *
 *---------------------------------------------------------------------------
 */
herr_t
H5VL_dataelevator_get_wrap_ctx(const void *obj, void **wrap_ctx)
{
    const H5VL_dataelevator_t *o = (const H5VL_dataelevator_t *)obj;
    H5VL_dataelevator_wrap_ctx_t *new_wrap_ctx;

#ifdef ENABLE_LOGGING
    printf("------- PASS THROUGH VOL WRAP CTX Get\n");
#endif

    /* Allocate new VOL object wrapping context for the pass through connector */
    new_wrap_ctx = (H5VL_dataelevator_wrap_ctx_t *)calloc(1, sizeof(H5VL_dataelevator_wrap_ctx_t));

    /* Increment reference count on underlying VOL ID, and copy the VOL info */
    new_wrap_ctx->under_vol_id = o->under_vol_id;
    H5Iinc_ref(new_wrap_ctx->under_vol_id);
    H5VLget_wrap_ctx(o->under_object, o->under_vol_id, &new_wrap_ctx->under_wrap_ctx);

    /* Set wrap context to return */
    *wrap_ctx = new_wrap_ctx;

    return 0;
} /* end H5VL_dataelevator_get_wrap_ctx() */

/*---------------------------------------------------------------------------
 * Function:    H5VL_dataelevator_wrap_object
 *
 * Purpose:     Use a "wrapper context" to wrap a data object
 *
 * Return:  Success:    0
 *      Failure:    -1
 *
 *---------------------------------------------------------------------------
 */
void *
H5VL_dataelevator_wrap_object(void *obj, H5I_type_t obj_type, void *_wrap_ctx)
{
    H5VL_dataelevator_wrap_ctx_t *wrap_ctx = (H5VL_dataelevator_wrap_ctx_t *)_wrap_ctx;
    H5VL_dataelevator_t *new_obj;
    void *under;

#ifdef ENABLE_LOGGING
    printf("------- PASS THROUGH VOL WRAP Object\n");
#endif

    /* Wrap the object with the underlying VOL */
    under = H5VLwrap_object(obj, obj_type, wrap_ctx->under_vol_id, wrap_ctx->under_wrap_ctx);
    if (under)
        new_obj = H5VL_dataelevator_new_obj(under, wrap_ctx->under_vol_id);
    else
        new_obj = NULL;

    return new_obj;
} /* end H5VL_dataelevator_wrap_object() */

/*---------------------------------------------------------------------------
 * Function:    H5VL_dataelevator_free_wrap_ctx
 *
 * Purpose:     Release a "wrapper context" for an object
 *
 * Return:  Success:    0
 *      Failure:    -1
 *
 *---------------------------------------------------------------------------
 */
herr_t
H5VL_dataelevator_free_wrap_ctx(void *_wrap_ctx)
{
    H5VL_dataelevator_wrap_ctx_t *wrap_ctx = (H5VL_dataelevator_wrap_ctx_t *)_wrap_ctx;
    hid_t err_id;

#ifdef ENABLE_LOGGING
    printf("------- PASS THROUGH VOL WRAP CTX Free\n");
#endif

    err_id = H5Eget_current_stack();

    /* Release underlying VOL ID and wrap context */
    if (wrap_ctx->under_wrap_ctx)
        H5VLfree_wrap_ctx(wrap_ctx->under_wrap_ctx, wrap_ctx->under_vol_id);
    H5Idec_ref(wrap_ctx->under_vol_id);

    H5Eset_current_stack(err_id);

    /* Free pass through wrap context object itself */
    free(wrap_ctx);

    return (0);
} /* end H5VL_dataelevator_free_wrap_ctx() */

/*---------------------------------------------------------------------------
 * Function:    H5VL_dataelevator_unwrap_object
 *
 * Purpose:     Unwrap a wrapped object, discarding the wrapper, but returning
 *		underlying object.
 *
 * Return:      Success:    Pointer to unwrapped object
 *              Failure:    NULL
 *
 *---------------------------------------------------------------------------
 */
static void *
H5VL_dataelevator_unwrap_object(void *obj)
{
    H5VL_dataelevator_t *o = (H5VL_dataelevator_t *)obj;
    void *under;

#ifdef ENABLE_PASSTHRU_LOGGING
    printf("------- PASS THROUGH VOL UNWRAP Object\n");
#endif

    /* Unrap the object with the underlying VOL */
    under = H5VLunwrap_object(o->under_object, o->under_vol_id);

    if (under)
        H5VL_dataelevator_free_obj(o);

    return under;
} /* end H5VL_dataelevator_unwrap_object() */

/*-------------------------------------------------------------------------
 * Function:    H5VL_dataelevator_dataset_create
 *
 * Purpose:     Creates a dataset in a container
 *
 * Return:      Success:    Pointer to a dataset object
 *              Failure:    NULL
 *
 *-------------------------------------------------------------------------
 */
void *
H5VL_dataelevator_dataset_create(void *obj, const H5VL_loc_params_t *loc_params,
                                 const char *name, hid_t lcpl_id, hid_t type_id, hid_t space_id, hid_t dcpl_id, hid_t dapl_id, hid_t dxpl_id, void **req)
{
    H5VL_dataelevator_t *dset;
    H5VL_dataelevator_t *o = (H5VL_dataelevator_t *)obj;
    void *under;

#ifdef ENABLE_LOGGING
    printf("------- PASS THROUGH VOL DATASET Create\n");
#endif

    under = H5VLdataset_create(o->under_object, loc_params, o->under_vol_id, name, lcpl_id, type_id, space_id, dcpl_id, dapl_id, dxpl_id, req);
    if (under)
    {
        dset = H5VL_dataelevator_new_obj(under, o->under_vol_id);

        /* Check for async request */
        if (req && *req)
            *req = H5VL_dataelevator_new_obj(*req, o->under_vol_id);
    } /* end if */
    else
        dset = NULL;

    return ((void *)dset);
} /* end H5VL_dataelevator_dataset_create() */

/*-------------------------------------------------------------------------
 * Function:    H5VL_dataelevator_dataset_open
 *
 * Purpose:     Opens a dataset in a container
 *
 * Return:      Success:    Pointer to a dataset object
 *              Failure:    NULL
 *
 *-------------------------------------------------------------------------
 */
void *
H5VL_dataelevator_dataset_open(void *obj, const H5VL_loc_params_t *loc_params,
                               const char *name, hid_t dapl_id, hid_t dxpl_id, void **req)
{
    H5VL_dataelevator_t *dset;
    H5VL_dataelevator_t *o = (H5VL_dataelevator_t *)obj;
    void *under;

#ifdef ENABLE_LOGGING
    printf("------- PASS THROUGH VOL DATASET Open\n");
#endif

    under = H5VLdataset_open(o->under_object, loc_params, o->under_vol_id, name, dapl_id, dxpl_id, req);
    if (under)
    {
        dset = H5VL_dataelevator_new_obj(under, o->under_vol_id);

        /* Check for async request */
        if (req && *req)
            *req = H5VL_dataelevator_new_obj(*req, o->under_vol_id);

        //DE Code starts
        dset->de_open_file_name = o->de_open_file_name;
        dset->de_open_group_name = o->de_open_group_name;
        dset->de_open_dataset_name = strdup(name);
        char *de_bb_dir = NULL;
        if (de_prefetch_flag__indicator == 1)
        {
            NameDB();
            CreateDB();
            const char *bb_path = getenv("DW_JOB_STRIPED");
            if (bb_path != NULL)
            {
                de_bb_dir = strdup(bb_path);
            }
            else
            {
                const char *de_path = getenv("DE_DATA_DIR");
                if (de_path != NULL)
                {
                    //sprintf(de_mdb_name_str, "%s/%s", de_path, DE_DB_NAME);
                    de_bb_dir = strdup(de_path);
                }
                else
                {
                    de_bb_dir = strdup("./");
                }
            }
            AppendReadRecord(dset->de_open_file_name, de_bb_dir, dset->de_open_group_name, name, DE_READ_CACHED, 0);
        }
        //DE Code ends
    } /* end if */
    else
        dset = NULL;

    return (void *)dset;
} /* end H5VL_dataelevator_dataset_open() */

/*-------------------------------------------------------------------------
 * Function:    H5VL_dataelevator_dataset_read
 *
 * Purpose:     Reads data elements from a dataset into a buffer.
 *
 * Return:  Success:    0
 *      Failure:    -1
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5VL_dataelevator_dataset_read(void *dset, hid_t mem_type_id, hid_t mem_space_id,
                               hid_t file_space_id, hid_t plist_id, void *buf, void **req)
{
    H5VL_dataelevator_t *o = (H5VL_dataelevator_t *)dset;
    herr_t ret_value;

#ifdef ENABLE_LOGGING
    printf("------- PASS THROUGH VOL DATASET Read\n");
#endif

    if (de_prefetch_flag__indicator == 1)
    {
        //ret_value = H5VLdataset_read(d->under_object, native_driver_id_g, mem_type_id, mem_space_id, file_space_id,
        //   plist_id, buf, req);
        //DE Code starts
        double start_time, open_time, read_time, close_time;
        double func_start_time = MPI_Wtime();
        double meta_check_time;
        int rank = H5Sget_simple_extent_ndims(file_space_id);
        hsize_t *start, *end, *dims_size, *chunk_size;
        start = malloc(sizeof(hsize_t) * rank);
        end = malloc(sizeof(hsize_t) * rank);
        dims_size = malloc(sizeof(hsize_t) * rank);
        chunk_size = malloc(sizeof(hsize_t) * rank);

        H5Sget_select_bounds(file_space_id, start, end);
        H5Sget_simple_extent_dims(file_space_id, dims_size, NULL);
        int mpi_rank, mpi_size;
        MPI_Comm_rank(MPI_COMM_WORLD, &mpi_rank);
        MPI_Comm_size(MPI_COMM_WORLD, &mpi_size);

        //#ifdef DEBUG
        //if (mpi_rank == 0)
        //    printf("Read file [%s : %s : %s ] in VOL rank 0:  array rank: %d, start = (%u, %u), end=(%u, %u), array_size=(%u, %u)\n ", o->de_open_file_name, o->de_open_group_name, o->de_open_dataset_name, rank, start[0], start[1], end[0], end[1], dims_size[0], dims_size[1]);
        //#endif
        hsize_t buf_size = 1;
        int temp_i;
        for (temp_i = 0; temp_i < rank; temp_i++)
        {
            chunk_size[temp_i] = end[temp_i] - start[temp_i] + 1;
            buf_size = buf_size * chunk_size[temp_i];
        }
        //UpdateReadRecordVOLChunk(de_file_name_ptr, de_group_name_ptr, de_dataset_name_ptr, rank, chunk_size, dims_size);
        //char  chunk_file_name[1024];
        char **chunk_file_name_list = NULL;
        int chunk_file_name_list_size = 0;

        //hid_t  type_id = H5Dget_type(d->under_object);
        hid_t type_id;
        dataset_get_wrapper(o->under_object, o->under_vol_id, H5VL_DATASET_GET_TYPE, plist_id, req, &type_id);
        int type_size;
        switch (H5Tget_class(type_id))
        {
        case H5T_INTEGER:
            type_size = sizeof(int);
            break;
        case H5T_FLOAT:
            type_size = sizeof(float);
            break;
        default:
            printf("Unsupported datatype in pre-fetch in Data Elevator! \n");
            exit(-1);
            break;
        }

        // return values
        //  1: find cached chunk file (one-to-one matched)
        //     2: find cached chunk file (one-to-one contained within)
        //     3: find cahced chunk files (one-to-many containing many files)
        //  0: chunk file is not cached on BB
        // -1: some error happens
        int query_ret = QueryReadRecord(o->de_open_file_name, rank, start, end, dims_size, &chunk_file_name_list, &chunk_file_name_list_size);
        meta_check_time = MPI_Wtime() - func_start_time;

        if (query_ret == 1)
        {
            if (mpi_rank == 0)
                printf("Find exactly macthed chunk file on BB ^_^, name = %s (and other - [%d] - file ), chunk_size = (%llu, %llu), buf_size = %llu, type_size = %d !\n", chunk_file_name_list[0], chunk_file_name_list_size, chunk_size[0], chunk_size[1], buf_size, type_size);
            start_time = MPI_Wtime();
            int fp, k;
            struct stat st;
            open_time = MPI_Wtime();
            for (k = 0; k < chunk_file_name_list_size; k++)
            {
                if (chunk_file_name_list[0] == NULL)
                    break;
                fp = open(chunk_file_name_list[0], O_RDONLY);
                assert(fp >= 0);
                stat(chunk_file_name_list[0], &st);
                //Need to deal it more carefully
                if (st.st_size < type_size * buf_size)
                {
                    read(fp, buf, st.st_size);
                }
                else
                {
                    read(fp, buf, type_size * buf_size);
                }
                close(fp);
                free(chunk_file_name_list[0]);
                chunk_file_name_list[0] = NULL;
            }
            if (chunk_file_name_list != NULL)
            {
                free(chunk_file_name_list);
                chunk_file_name_list = NULL;
            }
            read_time = MPI_Wtime();
            close_time = MPI_Wtime();
        }
        else if (query_ret == 2)
        {
            if (mpi_rank == 0)
                printf("Find macthed within-files on BB ^_^, name = %s (and other - [%d] - file ), chunk_size = (%llu, %llu), buf_size = %llu, type_size = %d !\n", chunk_file_name_list[0], chunk_file_name_list_size, chunk_size[0], chunk_size[1], buf_size, type_size);

            hsize_t *file_start_coordinate = malloc(sizeof(hsize_t) * rank);
            hsize_t *file_count = malloc(sizeof(hsize_t) * rank);
            hsize_t *A_start_coordinate = malloc(sizeof(hsize_t) * rank);
            hsize_t *B_start_coordinate = malloc(sizeof(hsize_t) * rank);

            int fp, k;
            struct stat st;
            open_time = MPI_Wtime();
            fp = open(chunk_file_name_list[0], O_RDONLY);
            stat(chunk_file_name_list[0], &st);
            void *temp_large_buf = malloc(st.st_size);
            read(fp, temp_large_buf, st.st_size);

            convert_cache_file_name_to_coordinate(chunk_file_name_list[0], dims_size, rank, file_start_coordinate, file_count);
            int iiii;
            for (iiii = 0; iiii < rank; iiii++)
            {
                A_start_coordinate[iiii] = start[iiii] - file_start_coordinate[iiii];
                B_start_coordinate[iiii] = 0;
            }
            VArray_copy(rank, type_size, file_count, A_start_coordinate, chunk_size, temp_large_buf, chunk_size, B_start_coordinate, chunk_size, buf, 0);

            free(temp_large_buf);
            free(file_start_coordinate);
            free(A_start_coordinate);
            free(B_start_coordinate);

            close(fp);
            free(chunk_file_name_list[0]);
            chunk_file_name_list[0] = NULL;
            if (chunk_file_name_list != NULL)
            {
                free(chunk_file_name_list);
                chunk_file_name_list = NULL;
            }
            read_time = MPI_Wtime();
            close_time = MPI_Wtime();
        }
        else if (query_ret == 3)
        {
            if (mpi_rank == 0)
                printf("Find  one-to-many cached files on BB ^_^, name = %s (and other - [%d] - file ), chunk_size = (%llu, %llu), buf_size = %llu, type_size = %d !\n", chunk_file_name_list[0], chunk_file_name_list_size, chunk_size[0], chunk_size[1], buf_size, type_size);

            hsize_t *file_start_coordinate = malloc(sizeof(hsize_t) * rank);
            hsize_t *file_count = malloc(sizeof(hsize_t) * rank);
            hsize_t *A_start_coordinate = malloc(sizeof(hsize_t) * rank);
            hsize_t *B_start_coordinate = malloc(sizeof(hsize_t) * rank);

            int fp, k;
            struct stat st;
            open_time = MPI_Wtime();
            fp = open(chunk_file_name_list[0], O_RDONLY);
            stat(chunk_file_name_list[0], &st);
            void *temp_large_buf = malloc(st.st_size);
            read(fp, temp_large_buf, st.st_size);

            for (k = 0; k < chunk_file_name_list_size; k++)
            {
                if (chunk_file_name_list[k] == NULL)
                    break;
                convert_cache_file_name_to_coordinate(chunk_file_name_list[k], dims_size, rank, file_start_coordinate, file_count);
                int iiii;
                for (iiii = 0; iiii < rank; iiii++)
                {
                    A_start_coordinate[iiii] = file_start_coordinate[iiii] - start[iiii];
                    B_start_coordinate[iiii] = 0;
                }
                VArray_copy(rank, type_size, chunk_size, A_start_coordinate, file_count, buf, file_count, B_start_coordinate, file_count, temp_large_buf, 1);
            }

            free(temp_large_buf);
            free(file_start_coordinate);
            free(A_start_coordinate);
            free(B_start_coordinate);

            close(fp);
            free(chunk_file_name_list[0]);
            chunk_file_name_list[0] = NULL;
            if (chunk_file_name_list != NULL)
            {
                free(chunk_file_name_list);
                chunk_file_name_list = NULL;
            }
            read_time = MPI_Wtime();
            close_time = MPI_Wtime();
        }
        else
        {
            if (mpi_rank == 0)
                printf("Find no cached chunk on BB @_@ !\n");
            start_time = MPI_Wtime();
            open_time = MPI_Wtime();
            //ret_value = H5VLdataset_read(d->under_object, native_driver_id_g, mem_type_id, mem_space_id, file_space_id,  plist_id, buf, req);
            ret_value = H5VLdataset_read(o->under_object, o->under_vol_id, mem_type_id, mem_space_id, file_space_id, plist_id, buf, req);
            /* Check for async request */
            if (req && *req)
                *req = H5VL_dataelevator_new_obj(*req, o->under_vol_id);

            read_time = MPI_Wtime();
            close_time = MPI_Wtime();
        }

        free(start);
        free(end);
        free(dims_size);
        free(chunk_size);

        double my_read_time, max_read_time, min_read_time, func_time_max, func_time;
        my_read_time = read_time - open_time;
        func_time = close_time - func_start_time;
        MPI_Allreduce(&my_read_time, &max_read_time, 1, MPI_DOUBLE, MPI_MAX, MPI_COMM_WORLD);
        //MPI_Allreduce(&my_read_time, &min_read_time, 1, MPI_DOUBLE,   MPI_MIN, MPI_COMM_WORLD);
        //MPI_Allreduce(&func_time,   &func_time_max, 1, MPI_DOUBLE,   MPI_MAX, MPI_COMM_WORLD);
        //max_read_time =  my_read_time;
        min_read_time = my_read_time;
#ifdef DEBUG
        if (mpi_rank == 0)
            printf("In VOL: meta_check_time = %f, My rank = %d , Open time = %f, read_time=(max=%f, min=%f), my_read_time on rank 0= %f,  close_time = %f, func_time_max = %f \n", meta_check_time, mpi_rank, open_time - start_time, max_read_time, min_read_time, my_read_time, close_time - read_time, func_time_max);
#endif
    }
    else
    { //de_prefetch_flag__indicator == 0
        ret_value = H5VLdataset_read(o->under_object, o->under_vol_id, mem_type_id, mem_space_id, file_space_id, plist_id, buf, req);
        /* Check for async request */
        if (req && *req)
            *req = H5VL_dataelevator_new_obj(*req, o->under_vol_id);
    }
    //DE Code ends

    ret_value = H5VLdataset_read(o->under_object, o->under_vol_id, mem_type_id, mem_space_id, file_space_id, plist_id, buf, req);
    /* Check for async request */
    if (req && *req)
        *req = H5VL_dataelevator_new_obj(*req, o->under_vol_id);

    return ret_value;
} /* end H5VL_dataelevator_dataset_read() */

/*-------------------------------------------------------------------------
 * Function:    H5VL_dataelevator_dataset_write
 *
 * Purpose:     Writes data elements from a buffer into a dataset.
 *
 * Return:  Success:    0
 *      Failure:    -1
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5VL_dataelevator_dataset_write(void *dset, hid_t mem_type_id, hid_t mem_space_id,
                                hid_t file_space_id, hid_t plist_id, const void *buf, void **req)
{
    H5VL_dataelevator_t *o = (H5VL_dataelevator_t *)dset;
    herr_t ret_value;

#ifdef ENABLE_LOGGING
    printf("------- PASS THROUGH VOL DATASET Write\n");
#endif

    ret_value = H5VLdataset_write(o->under_object, o->under_vol_id, mem_type_id, mem_space_id, file_space_id, plist_id, buf, req);

    /* Check for async request */
    if (req && *req)
        *req = H5VL_dataelevator_new_obj(*req, o->under_vol_id);

    return ret_value;
} /* end H5VL_dataelevator_dataset_write() */

/*-------------------------------------------------------------------------
 * Function:    H5VL_dataelevator_dataset_get
 *
 * Purpose:     Gets information about a dataset
 *
 * Return:  Success:    0
 *      Failure:    -1
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5VL_dataelevator_dataset_get(void *dset, H5VL_dataset_get_t get_type,
                              hid_t dxpl_id, void **req, va_list arguments)
{
    H5VL_dataelevator_t *o = (H5VL_dataelevator_t *)dset;
    herr_t ret_value;

#ifdef ENABLE_LOGGING
    printf("------- PASS THROUGH VOL DATASET Get\n");
#endif

    ret_value = H5VLdataset_get(o->under_object, o->under_vol_id, get_type, dxpl_id, req, arguments);

    /* Check for async request */
    if (req && *req)
        *req = H5VL_dataelevator_new_obj(*req, o->under_vol_id);

    return (ret_value);
} /* end H5VL_dataelevator_dataset_get() */

/*-------------------------------------------------------------------------
 * Function:    H5VL_dataelevator_dataset_specific
 *
 * Purpose: Specific operation on a dataset
 *
 * Return:  Success:    0
 *      Failure:    -1
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5VL_dataelevator_dataset_specific(void *obj, H5VL_dataset_specific_t specific_type,
                                   hid_t dxpl_id, void **req, va_list arguments)
{
    H5VL_dataelevator_t *o = (H5VL_dataelevator_t *)obj;
    herr_t ret_value;
    hid_t under_vol_id;

#ifdef ENABLE_LOGGING
    printf("------- PASS THROUGH VOL H5Dspecific\n");
#endif
    under_vol_id = o->under_vol_id;

    ret_value = H5VLdataset_specific(o->under_object, o->under_vol_id, specific_type, dxpl_id, req, arguments);

    /* Check for async request */
    if (req && *req)
        *req = H5VL_dataelevator_new_obj(*req, under_vol_id);

    return (ret_value);
} /* end H5VL_dataelevator_dataset_specific() */

/*-------------------------------------------------------------------------
 * Function:    H5VL_dataelevator_dataset_optional
 *
 * Purpose:     Perform a connector-specific operation on a dataset
 *
 * Return:  Success:    0
 *      Failure:    -1
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5VL_dataelevator_dataset_optional(void *obj, hid_t dxpl_id, void **req,
                                   va_list arguments)
{
    H5VL_dataelevator_t *o = (H5VL_dataelevator_t *)obj;
    herr_t ret_value;

#ifdef ENABLE_LOGGING
    printf("------- PASS THROUGH VOL DATASET Optional\n");
#endif

    ret_value = H5VLdataset_optional(o->under_object, o->under_vol_id, dxpl_id, req, arguments);

    /* Check for async request */
    if (req && *req)
        *req = H5VL_dataelevator_new_obj(*req, o->under_vol_id);

    return (ret_value);
} /* end H5VL_dataelevator_dataset_optional() */

/*-------------------------------------------------------------------------
 * Function:    H5VL_dataelevator_dataset_close
 *
 * Purpose:     Closes a dataset.
 *
 * Return:  Success:    0
 *      Failure:    -1, dataset not closed.
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5VL_dataelevator_dataset_close(void *dset, hid_t dxpl_id, void **req)
{
    H5VL_dataelevator_t *o = (H5VL_dataelevator_t *)dset;
    herr_t ret_value;

#ifdef ENABLE_LOGGING
    printf("------- PASS THROUGH VOL DATASET Close\n");
#endif

    ret_value = H5VLdataset_close(o->under_object, o->under_vol_id, dxpl_id, req);

    /* Check for async request */
    if (req && *req)
        *req = H5VL_dataelevator_new_obj(*req, o->under_vol_id);

    /* Release our wrapper, if underlying dataset was closed */
    if (ret_value >= 0)
        H5VL_dataelevator_free_obj(o);

    return ret_value;
} /* end H5VL_dataelevator_dataset_close() */

/*-------------------------------------------------------------------------
 * Function:    H5VL_dataelevator_file_create
 *
 * Purpose:     Creates a container using this connector
 *
 * Return:      Success:    Pointer to a file object
 *              Failure:    NULL
 *
 *-------------------------------------------------------------------------
 */
void *
H5VL_dataelevator_file_create(const char *name, unsigned flags, hid_t fcpl_id,
                              hid_t fapl_id, hid_t dxpl_id, void **req)
{
    H5VL_dataelevator_info_t *info;
    H5VL_dataelevator_t *file;
    hid_t under_fapl_id;
    void *under;

#ifdef ENABLE_LOGGING
    printf("------- PASS THROUGH VOL FILE Create\n");
#endif

    //DE Code starts
    char *new_name;
    NameDB();
    CreateDB();
    new_name = AppendRecordVol(name, DE_NEW_FILE_BB, 0);
    //DE Code ends

    /* Get copy of our VOL info from FAPL */
    H5Pget_vol_info(fapl_id, (void **)&info);

    /* Copy the FAPL */
    under_fapl_id = H5Pcopy(fapl_id);

    /* Set the VOL ID and info for the underlying FAPL */
    H5Pset_vol(under_fapl_id, info->under_vol_id, info->under_vol_info);

    /* Open the file with the underlying VOL connector */
    under = H5VLfile_create(new_name, flags, fcpl_id, under_fapl_id, dxpl_id, req);
    if (under)
    {
        file = H5VL_dataelevator_new_obj(under, info->under_vol_id);

        /* Check for async request */
        if (req && *req)
            *req = H5VL_dataelevator_new_obj(*req, info->under_vol_id);
    } /* end if */
    else
        file = NULL;

    //DE Code starts
    file->de_open_file_name = strdup(new_name);
    de_create_flag__indicator = 1;
    //DE Code ends

    /* Close underlying FAPL */
    H5Pclose(under_fapl_id);

    /* Release copy of our VOL info */
    H5VL_dataelevator_info_free(info);

    return ((void *)file);
} /* end H5VL_dataelevator_file_create() */

/*-------------------------------------------------------------------------
 * Function:    H5VL_dataelevator_file_open
 *
 * Purpose:     Opens a container created with this connector
 *
 * Return:      Success:    Pointer to a file object
 *              Failure:    NULL
 *
 *-------------------------------------------------------------------------
 */
void *
H5VL_dataelevator_file_open(const char *name, unsigned flags, hid_t fapl_id,
                            hid_t dxpl_id, void **req)
{
    H5VL_dataelevator_info_t *info;
    H5VL_dataelevator_t *file;
    hid_t under_fapl_id;
    void *under;

#ifdef ENABLE_LOGGING
    printf("------- PASS THROUGH VOL FILE Open\n");
#endif

    /* Get copy of our VOL info from FAPL */
    H5Pget_vol_info(fapl_id, (void **)&info);

    /* Copy the FAPL */
    under_fapl_id = H5Pcopy(fapl_id);

    /* Set the VOL ID and info for the underlying FAPL */
    H5Pset_vol(under_fapl_id, info->under_vol_id, info->under_vol_info);

    //DE Code Starts
    int my_rank;
    MPI_Comm_rank(MPI_COMM_WORLD, &my_rank);
    char *new_name;

    new_name = FindPreviousCreatedFileOnBB(name);

    int has_PreviousCreatedFile_flag = 0, gloab_flag;
    if (new_name != NULL)
        has_PreviousCreatedFile_flag = 1;
    MPI_Allreduce(&has_PreviousCreatedFile_flag, &gloab_flag, 1, MPI_INT, MPI_MIN, MPI_COMM_WORLD);

    if (gloab_flag == 1)
    {
        //We are here for the open after create
        //So, we open the file on BB
#ifdef ENABLE_VOL_LOG
        printf("File_open~Find file [%s] on BB to read for orginal file !\n", new_name, name);
#endif
        if (my_rank == 0)
            printf("Redirect [read] to Disk file [%s] to BB file [%s] by Data Elevator \n", name, new_name);
        //temp_new_name = strdup(new_name);
        fflush(stdout);
        /* Open the file with the underlying VOL connector */
        under = H5VLfile_open(new_name, flags, under_fapl_id, dxpl_id, req);
        if (under)
        {
            file = H5VL_dataelevator_new_obj(under, info->under_vol_id);
            /* Check for async request */
            if (req && *req)
                *req = H5VL_dataelevator_new_obj(*req, info->under_vol_id);

            file->de_open_file_name = strdup(new_name);
        } /* end if */
        else
        {
            file = NULL;
        }
    }
    else
    {
        //#ifdef ENABLE_VOL_LOG
        printf("File_open~Find [NO] files on BB to read for original file %s, we are going to prefetch now ! \n", name);
        fflush(stdout);
        //#endif
        //We here for read and will have prefetch on the dataset to open
        //temp_new_name =strdup(name);
        under = H5VLfile_open(name, flags, under_fapl_id, dxpl_id, req);
        if (under)
        {
            file = H5VL_dataelevator_new_obj(under, info->under_vol_id);
            /* Check for async request */
            if (req && *req)
                *req = H5VL_dataelevator_new_obj(*req, info->under_vol_id);
            file->de_open_file_name = strdup(name);
        } /* end if */
        else
        {
            file = NULL;
        }
        de_prefetch_flag__indicator = 1;
    }
    //DE Code Ends

    /* Close underlying FAPL */
    H5Pclose(under_fapl_id);

    /* Release copy of our VOL info */
    H5VL_dataelevator_info_free(info);

    return (void *)file;
} /* end H5VL_dataelevator_file_open() */

/*-------------------------------------------------------------------------
 * Function:    H5VL_dataelevator_file_get
 *
 * Purpose:     Get info about a file
 *
 * Return:  Success:    0
 *      Failure:    -1
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5VL_dataelevator_file_get(void *file, H5VL_file_get_t get_type, hid_t dxpl_id,
                           void **req, va_list arguments)
{
    H5VL_dataelevator_t *o = (H5VL_dataelevator_t *)file;
    herr_t ret_value;

#ifdef ENABLE_LOGGING
    printf("------- PASS THROUGH VOL FILE Get\n");
#endif

    ret_value = H5VLfile_get(o->under_object, o->under_vol_id, get_type, dxpl_id, req, arguments);

    /* Check for async request */
    if (req && *req)
        *req = H5VL_dataelevator_new_obj(*req, o->under_vol_id);

    return ret_value;
} /* end H5VL_dataelevator_file_get() */

/*-------------------------------------------------------------------------
 * Function:    H5VL_dataelevator_file_specific_reissue
 *
 * Purpose: Re-wrap vararg arguments into a va_list and reissue the
 *      file specific callback to the underlying VOL connector.
 *
 * Return:  Success:    0
 *      Failure:    -1
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5VL_dataelevator_file_specific_reissue(void *obj, hid_t connector_id,
                                        H5VL_file_specific_t specific_type, hid_t dxpl_id, void **req, ...)
{
    va_list arguments;
    herr_t ret_value;

    va_start(arguments, req);
    ret_value = H5VLfile_specific(obj, connector_id, specific_type, dxpl_id, req, arguments);
    va_end(arguments);

    return ret_value;
} /* end H5VL_dataelevator_file_specific_reissue() */

/*-------------------------------------------------------------------------
 * Function:    H5VL_dataelevator_file_specific
 *
 * Purpose: Specific operation on file
 *
 * Return:  Success:    0
 *      Failure:    -1
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5VL_dataelevator_file_specific(void *file, H5VL_file_specific_t specific_type,
                                hid_t dxpl_id, void **req, va_list arguments)
{
    H5VL_dataelevator_t *o = (H5VL_dataelevator_t *)file;
    hid_t under_vol_id = -1;
    herr_t ret_value;

#ifdef ENABLE_LOGGING
    printf("------- PASS THROUGH VOL FILE Specific\n");
#endif

    /* Unpack arguments to get at the child file pointer when mounting a file */
    if (specific_type == H5VL_FILE_MOUNT)
    {
        H5I_type_t loc_type;
        const char *name;
        H5VL_dataelevator_t *child_file;
        hid_t plist_id;

        /* Retrieve parameters for 'mount' operation, so we can unwrap the child file */
        loc_type = va_arg(arguments, H5I_type_t);
        name = va_arg(arguments, const char *);
        child_file = (H5VL_dataelevator_t *)va_arg(arguments, void *);
        plist_id = va_arg(arguments, hid_t);

        /* Keep the correct underlying VOL ID for possible async request token */
        under_vol_id = o->under_vol_id;

        /* Re-issue 'file specific' call, using the unwrapped pieces */
        ret_value = H5VL_dataelevator_file_specific_reissue(o->under_object, o->under_vol_id, specific_type, dxpl_id, req, loc_type, name, child_file->under_object, plist_id);
    } /* end if */
    else if (specific_type == H5VL_FILE_IS_ACCESSIBLE || specific_type == H5VL_FILE_DELETE)
    {
        H5VL_dataelevator_info_t *info;
        hid_t fapl_id, under_fapl_id;
        const char *name;
        htri_t *ret;

        /* Get the arguments for the 'is accessible' check */
        fapl_id = va_arg(arguments, hid_t);
        name = va_arg(arguments, const char *);
        ret = va_arg(arguments, htri_t *);

        /* Get copy of our VOL info from FAPL */
        H5Pget_vol_info(fapl_id, (void **)&info);

        /* Copy the FAPL */
        under_fapl_id = H5Pcopy(fapl_id);

        /* Set the VOL ID and info for the underlying FAPL */
        H5Pset_vol(under_fapl_id, info->under_vol_id, info->under_vol_info);

        /* Keep the correct underlying VOL ID for possible async request token */
        under_vol_id = info->under_vol_id;

        /* Re-issue 'file specific' call */
        ret_value = H5VL_dataelevator_file_specific_reissue(NULL, info->under_vol_id, specific_type, dxpl_id, req, under_fapl_id, name, ret);

        /* Close underlying FAPL */
        H5Pclose(under_fapl_id);

        /* Release copy of our VOL info */
        H5VL_dataelevator_info_free(info);
    } /* end else-if */
    else
    {
        va_list my_arguments;

        /* Make a copy of the argument list for later, if reopening */
        if (specific_type == H5VL_FILE_REOPEN)
            va_copy(my_arguments, arguments);

        /* Keep the correct underlying VOL ID for possible async request token */
        under_vol_id = o->under_vol_id;

        ret_value = H5VLfile_specific(o->under_object, o->under_vol_id, specific_type, dxpl_id, req, arguments);

        /* Wrap file struct pointer, if we reopened one */
        if (specific_type == H5VL_FILE_REOPEN)
        {
            if (ret_value >= 0)
            {
                void **ret = va_arg(my_arguments, void **);

                if (ret && *ret)
                    *ret = H5VL_dataelevator_new_obj(*ret, o->under_vol_id);
            } /* end if */

            /* Finish use of copied vararg list */
            va_end(my_arguments);
        } /* end if */
    }     /* end else */

    /* Check for async request */
    if (req && *req)
        *req = H5VL_dataelevator_new_obj(*req, under_vol_id);

    return (ret_value);
} /* end H5VL_dataelevator_file_specific() */

/*-------------------------------------------------------------------------
 * Function:    H5VL_dataelevator_file_optional
 *
 * Purpose:     Perform a connector-specific operation on a file
 *
 * Return:  Success:    0
 *      Failure:    -1
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5VL_dataelevator_file_optional(void *file, hid_t dxpl_id, void **req,
                                va_list arguments)
{
    H5VL_dataelevator_t *o = (H5VL_dataelevator_t *)file;
    herr_t ret_value;

#ifdef ENABLE_LOGGING
    printf("------- PASS THROUGH VOL File Optional\n");
#endif

    ret_value = H5VLfile_optional(o->under_object, o->under_vol_id, dxpl_id, req, arguments);

    /* Check for async request */
    if (req && *req)
        *req = H5VL_dataelevator_new_obj(*req, o->under_vol_id);

    return (ret_value);
} /* end H5VL_dataelevator_file_optional() */

/*-------------------------------------------------------------------------
 * Function:    H5VL_dataelevator_file_close
 *
 * Purpose:     Closes a file.
 *
 * Return:  Success:    0
 *      Failure:    -1, file not closed.
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5VL_dataelevator_file_close(void *file, hid_t dxpl_id, void **req)
{
    H5VL_dataelevator_t *o = (H5VL_dataelevator_t *)file;
    herr_t ret_value;

#ifdef ENABLE_LOGGING
    printf("------- PASS THROUGH VOL FILE Close\n");
#endif

    ret_value = H5VLfile_close(o->under_object, o->under_vol_id, dxpl_id, req);

    /* Check for async request */
    if (req && *req)
        *req = H5VL_dataelevator_new_obj(*req, o->under_vol_id);

    /* Release our wrapper, if underlying file was closed */
    if (ret_value >= 0)
    {
        //DE Code starts
        char sv[256];
        sprintf(sv, "%d", DE_WRITE_BB_DONE);
        printf("Close file name: %s \n ", o->de_open_file_name);
        UpdateRecordVOL(o->de_open_file_name, DE_TYPE_S, sv);
        free(o->de_open_file_name);
        o->de_open_file_name = NULL;
        //DE Code ends
        H5VL_dataelevator_free_obj(o);
    }

    return (ret_value);
} /* end H5VL_dataelevator_file_close() */

/*-------------------------------------------------------------------------
 * Function:    H5VL_dataelevator_group_create
 *
 * Purpose:     Creates a group inside a container
 *
 * Return:      Success:    Pointer to a group object
 *              Failure:    NULL
 *
 *-------------------------------------------------------------------------
 */
void *
H5VL_dataelevator_group_create(void *obj, const H5VL_loc_params_t *loc_params,
                               const char *name, hid_t lcpl_id, hid_t gcpl_id, hid_t gapl_id, hid_t dxpl_id, void **req)
{
    H5VL_dataelevator_t *group;
    H5VL_dataelevator_t *o = (H5VL_dataelevator_t *)obj;
    void *under;

#ifdef ENABLE_LOGGING
    printf("------- PASS THROUGH VOL GROUP Create\n");
#endif

    under = H5VLgroup_create(o->under_object, loc_params, o->under_vol_id, name, lcpl_id, gcpl_id, gapl_id, dxpl_id, req);
    if (under)
    {
        group = H5VL_dataelevator_new_obj(under, o->under_vol_id);

        /* Check for async request */
        if (req && *req)
            *req = H5VL_dataelevator_new_obj(*req, o->under_vol_id);
    } /* end if */
    else
        group = NULL;

    return ((void *)group);
} /* end H5VL_dataelevator_group_create() */

/*-------------------------------------------------------------------------
 * Function:    H5VL_dataelevator_group_open
 *
 * Purpose:     Opens a group inside a container
 *
 * Return:      Success:    Pointer to a group object
 *              Failure:    NULL
 *
 *-------------------------------------------------------------------------
 */
void *
H5VL_dataelevator_group_open(void *obj, const H5VL_loc_params_t *loc_params,
                             const char *name, hid_t gapl_id, hid_t dxpl_id, void **req)
{
    H5VL_dataelevator_t *group;
    H5VL_dataelevator_t *o = (H5VL_dataelevator_t *)obj;
    void *under;

#ifdef ENABLE_LOGGING
    printf("------- PASS THROUGH VOL GROUP Open\n");
#endif

    under = H5VLgroup_open(o->under_object, loc_params, o->under_vol_id, name, gapl_id, dxpl_id, req);
    if (under)
    {
        group = H5VL_dataelevator_new_obj(under, o->under_vol_id);

        /* Check for async request */
        if (req && *req)
            *req = H5VL_dataelevator_new_obj(*req, o->under_vol_id);

        //DE Code starts
        group->de_open_file_name = o->de_open_file_name;
        group->de_open_group_name = strdup(name);
        //DE Code ends

    } /* end if */
    else
        group = NULL;

    return ((void *)group);
} /* end H5VL_dataelevator_group_open() */

/*-------------------------------------------------------------------------
 * Function:    H5VL_dataelevator_group_get
 *
 * Purpose:     Get info about a group
 *
 * Return:  Success:    0
 *      Failure:    -1
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5VL_dataelevator_group_get(void *obj, H5VL_group_get_t get_type, hid_t dxpl_id,
                            void **req, va_list arguments)
{
    H5VL_dataelevator_t *o = (H5VL_dataelevator_t *)obj;
    herr_t ret_value;

#ifdef ENABLE_LOGGING
    printf("------- PASS THROUGH VOL GROUP Get\n");
#endif

    ret_value = H5VLgroup_get(o->under_object, o->under_vol_id, get_type, dxpl_id, req, arguments);

    /* Check for async request */
    if (req && *req)
        *req = H5VL_dataelevator_new_obj(*req, o->under_vol_id);

    return (ret_value);
} /* end H5VL_dataelevator_group_get() */

/*-------------------------------------------------------------------------
 * Function:    H5VL_dataelevator_group_specific
 *
 * Purpose: Specific operation on a group
 *
 * Return:  Success:    0
 *      Failure:    -1
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5VL_dataelevator_group_specific(void *obj, H5VL_group_specific_t specific_type,
                                 hid_t dxpl_id, void **req, va_list arguments)
{
    H5VL_dataelevator_t *o = (H5VL_dataelevator_t *)obj;
    herr_t ret_value;

#ifdef ENABLE_LOGGING
    printf("------- PASS THROUGH VOL GROUP Specific\n");
#endif

    ret_value = H5VLgroup_specific(o->under_object, o->under_vol_id, specific_type, dxpl_id, req, arguments);

    /* Check for async request */
    if (req && *req)
        *req = H5VL_dataelevator_new_obj(*req, o->under_vol_id);

    return (ret_value);
} /* end H5VL_dataelevator_group_specific() */

/*-------------------------------------------------------------------------
 * Function:    H5VL_dataelevator_group_optional
 *
 * Purpose:     Perform a connector-specific operation on a group
 *
 * Return:  Success:    0
 *      Failure:    -1
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5VL_dataelevator_group_optional(void *obj, hid_t dxpl_id, void **req,
                                 va_list arguments)
{
    H5VL_dataelevator_t *o = (H5VL_dataelevator_t *)obj;
    herr_t ret_value;

#ifdef ENABLE_LOGGING
    printf("------- PASS THROUGH VOL GROUP Optional\n");
#endif

    ret_value = H5VLgroup_optional(o->under_object, o->under_vol_id, dxpl_id, req, arguments);

    /* Check for async request */
    if (req && *req)
        *req = H5VL_dataelevator_new_obj(*req, o->under_vol_id);

    return (ret_value);
} /* end H5VL_dataelevator_group_optional() */

/*-------------------------------------------------------------------------
 * Function:    H5VL_dataelevator_group_close
 *
 * Purpose:     Closes a group.
 *
 * Return:  Success:    0
 *      Failure:    -1, group not closed.
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5VL_dataelevator_group_close(void *grp, hid_t dxpl_id, void **req)
{
    H5VL_dataelevator_t *o = (H5VL_dataelevator_t *)grp;
    herr_t ret_value;

#ifdef ENABLE_LOGGING
    printf("------- PASS THROUGH VOL H5Gclose\n");
#endif

    ret_value = H5VLgroup_close(o->under_object, o->under_vol_id, dxpl_id, req);

    /* Check for async request */
    if (req && *req)
        *req = H5VL_dataelevator_new_obj(*req, o->under_vol_id);

    /* Release our wrapper, if underlying file was closed */
    if (ret_value >= 0)
        H5VL_dataelevator_free_obj(o);

    return (ret_value);
} /* end H5VL_dataelevator_group_close() */
