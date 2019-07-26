#define _SVID_SOURCE
#define _POSIX_C_SOURCE 200809L

#ifdef REGISTER_VOL_BY_HAND
#include <data_elevator_vol.h>
#endif
#include <stdio.h>
#include <stdlib.h>
#include <hdf5.h>
#include <unistd.h>
#include <getopt.h>
#include <assert.h>

#define ROW_MAJOR_ORDER_REVERSE_MACRO_2D(offset, dsize, result_coord_v) \
    {                                                                   \
        unsigned long long temp_offset = offset;                        \
        result_coord_v[1] = temp_offset % dsize[1];                     \
        temp_offset = temp_offset / dsize[1];                           \
        result_coord_v[0] = temp_offset;                                \
    }

#define ROW_MAJOR_ORDER_MACRO_2D(dsize, i, j, offset) \
    {                                                 \
        offset = i;                                   \
        offset = offset * dsize[1] + j;               \
    }

// = 0 : write (include reading back from BB after write)
// = 1 : read based on prefetch
int test_rw_flag = 0;

void print_help()
{
    const char *msg = "Usage: %s [OPTION] \n\
    -h help (--help)\n\
    -w test write (include reading back from BB after write) (default) \n\
    -p tes read based on prefetch \n\
    Example:  mpirun/srun -n 2 ./test \n";

    fprintf(stdout, msg, "test");
}

int main(int argc, char **argv)
{
    char file_name[1024];
    char group_name[] = "/group";
    char dataset_name[] = "data";
    hid_t file_id, group_id, datasetId, dataspaceId, file_space, memspace_id;
    hid_t de_fapl, de_vol_id;
    unsigned int nelem = 60;
    int *data;
    unsigned int i, j;
    hsize_t dims[1], offset, count;

    int c;
    while ((c = getopt(argc, argv, "wph")) != -1)
        switch (c)
        {
        case 'w':
            test_rw_flag = 0;
            break;
        case 'p':
            test_rw_flag = 1;
            break;
        case 'h':
            print_help();
            return 0;
            break;
        default:
            print_help();
            return -1;
            break;
        }

    //Some intialization works
    int mpi_size, mpi_rank, find_mis_matched;
    MPI_Init(&argc, &argv);
    MPI_Comm_size(MPI_COMM_WORLD, &mpi_size);
    MPI_Comm_rank(MPI_COMM_WORLD, &mpi_rank);

    //printf("%s, %d, here \n", __FILE__, __LINE__);

    //Register DataElevator plugin
    de_fapl = H5Pcreate(H5P_FILE_ACCESS);
    //printf("%s, %d, here \n", __FILE__, __LINE__);

    H5Pset_fapl_mpio(de_fapl, MPI_COMM_WORLD, MPI_INFO_NULL);
    //printf("%s, %d, here \n", __FILE__, __LINE__);

#ifdef REGISTER_VOL_BY_HAND
    de_vol_id = H5VLregister_connector(&H5VL_dataelevator_g, H5P_DEFAULT);
    H5VL_dataelevator_info_t under_vol;
    hid_t under_vol_id;
    void *under_vol_info;
    H5Pget_vol_id(de_fapl, &under_vol_id);
    H5Pget_vol_info(de_fapl, &under_vol_info);
    under_vol.under_vol_id = under_vol_id;
    under_vol.under_vol_info = under_vol_info;
    H5Pset_vol(de_fapl, de_vol_id, &under_vol);
#endif

    if (test_rw_flag == 0)
    {
        //Start to test the write functions
        for (j = 0; j < 3; j++)
        {
            sprintf(file_name, "%s-%d%s", "h5file", j, ".h5");
            if (mpi_rank == 0)
                printf("Writing file_name = %s at rank 0 \n", file_name);
            //printf("%s-%d, here \n", __FILE__, __LINE__);
            file_id = H5Fcreate(file_name, H5F_ACC_TRUNC, H5P_DEFAULT, de_fapl);
            //printf("%s-%d, here \n", __FILE__, __LINE__);
            group_id = H5Gcreate2(file_id, group_name, H5P_DEFAULT, H5P_DEFAULT, H5P_DEFAULT);

            dims[0] = nelem * mpi_size;
            dataspaceId = H5Screate_simple(1, dims, NULL);
            datasetId = H5Dcreate(group_id, dataset_name, H5T_NATIVE_INT, dataspaceId, H5P_DEFAULT, H5P_DEFAULT, H5P_DEFAULT);

            data = malloc(sizeof(int) * nelem);
            for (i = 0; i < nelem; i++)
                data[i] = i;

            offset = nelem * mpi_rank;
            count = nelem;
            file_space = H5Dget_space(datasetId);
            H5Sselect_hyperslab(file_space, H5S_SELECT_SET, &offset, NULL, &count, NULL);

            memspace_id = H5Screate_simple(1, &count, NULL);

            H5Dwrite(datasetId, H5T_NATIVE_INT, memspace_id, file_space, H5P_DEFAULT, data);

            free(data);

            H5Sclose(file_space);
            H5Sclose(memspace_id);
            H5Sclose(dataspaceId);
            H5Dclose(datasetId);
            H5Gclose(group_id);
            H5Fclose(file_id);
            fflush(stdout);
            sleep(1);
        }

        //Start to test the read-on-BB functions
        find_mis_matched = 0;
        for (j = 0; j < 3; j++)
        {
            sprintf(file_name, "%s-%d%s", "h5file", j, ".h5");
            if (mpi_rank == 0)
                printf("Read file: %s back to check at rank 0\n", file_name);
            file_id = H5Fopen(file_name, H5F_ACC_RDONLY, de_fapl);
            if (file_id < 0)
                exit(-1);
            group_id = H5Gopen(file_id, group_name, H5P_DEFAULT);
            if (group_id < 0)
                exit(-1);
            datasetId = H5Dopen(group_id, dataset_name, H5P_DEFAULT);
            if (datasetId < 0)
                exit(-1);

            offset = nelem * mpi_rank;
            count = nelem;
            file_space = H5Dget_space(datasetId);
            H5Sselect_hyperslab(file_space, H5S_SELECT_SET, &offset, NULL, &count, NULL);
            memspace_id = H5Screate_simple(1, &count, NULL);

            data = malloc(sizeof(int) * nelem);
            H5Dread(datasetId, H5T_NATIVE_INT, memspace_id, file_space, H5P_DEFAULT, data);

            for (i = 0; i < nelem; i++)
            {
                if (data[i] != i)
                {
                    find_mis_matched = 1;
                    printf("Find mismatched data at %d for reading file %s back from BB \n", i, file_name);
                    break;
                }
                fflush(stdout);
            }

            free(data);

            H5Sclose(file_space);
            H5Sclose(memspace_id);
            H5Dclose(datasetId);
            H5Gclose(group_id);
            H5Fclose(file_id);
        }
        if (find_mis_matched == 0)
            printf("[Test Passed] Read data back on BB , no error!\n");
    }
    else
    {
        //Start to test the read with prefetch functions
        sprintf(file_name, "%s", "prefetch-100by100.h5");
        //Test file contains 0 ~ 9999, in a 2D array

        if (mpi_rank == 0)
            printf("Open file: %s to test prefetch\n", file_name);
        //H5F_ACC_RDONLY
        file_id = H5Fopen(file_name, H5F_ACC_RDONLY, de_fapl);
        if (file_id < 0)
            exit(-1);
        group_id = H5Gopen(file_id, group_name, H5P_DEFAULT);
        if (group_id < 0)
            exit(-1);
        datasetId = H5Dopen(group_id, dataset_name, H5P_DEFAULT);
        if (datasetId < 0)
            exit(-1);

        //Chunk_size = 10, 10
        int my_current_chunk_id = mpi_rank, k;
        hsize_t prefetch_offset[2], prefetch_count[2];
        int chunk_size[2], total_chunks, chunked_dims_size[2], real_value, dims_size[2], my_current_chunk_id_2d[2];
        chunk_size[0] = 10;
        chunk_size[1] = 10;
        dims_size[0] = 100;
        dims_size[1] = 100;
        chunked_dims_size[0] = dims_size[0] / chunk_size[0];
        chunked_dims_size[1] = dims_size[1] / chunk_size[1];
        total_chunks = chunked_dims_size[0] * chunked_dims_size[1];
        assert(total_chunks % mpi_size == 0); //for parallel collective I/O, possible
        int my_chunks = total_chunks / mpi_size;
        assert(my_chunks > 4); //It is better to have more batches

        if (mpi_rank == 0)
            printf("total_chunks = %d,dims_size = (%d, %d), chunk_size = (%d, %d), my_chunks = %d\n", total_chunks, dims_size[0], dims_size[1], chunk_size[0], chunk_size[1], my_chunks);

        prefetch_count[0] = chunk_size[0];
        prefetch_count[1] = chunk_size[1];
        data = malloc(sizeof(int) * chunk_size[0] * chunk_size[1]);

        memspace_id = H5Screate_simple(2, prefetch_count, NULL);
        file_space = H5Dget_space(datasetId);

        int data_index = 0;
        find_mis_matched = 0;
        for (k = 0; k < my_chunks; k++)
        {
            ROW_MAJOR_ORDER_REVERSE_MACRO_2D(my_current_chunk_id, chunked_dims_size, my_current_chunk_id_2d);
            prefetch_offset[0] = chunk_size[0] * my_current_chunk_id_2d[0];
            prefetch_offset[1] = chunk_size[1] * my_current_chunk_id_2d[1];

            if (mpi_rank == 0)
                printf("At rank 0: my_current_chunk_id = %d,my_current_chunk_id_2d = (%d, %d), prefetch_count = (%llu, %llu)\n", my_current_chunk_id, my_current_chunk_id_2d[0], my_current_chunk_id_2d[1], prefetch_count[0], prefetch_count[1]);

            H5Sselect_hyperslab(file_space, H5S_SELECT_SET, prefetch_offset, NULL, prefetch_count, NULL);
            H5Dread(datasetId, H5T_NATIVE_INT, memspace_id, file_space, H5P_DEFAULT, data);

            data_index = 0;
            for (i = prefetch_offset[0]; i < prefetch_offset[0] + chunk_size[0]; i++)
            {
                for (j = prefetch_offset[1]; j < prefetch_offset[1] + chunk_size[1]; j++)
                {
                    ROW_MAJOR_ORDER_MACRO_2D(dims_size, i, j, real_value);
                    if (data[data_index] != real_value)
                    {
                        find_mis_matched = 1;
                        printf("At MPI RANK %d, Find mismatched data at (%d, %d) for file %s, prefetched value = %d, real value = %d \n", mpi_rank, i, j, file_name, data[data_index], real_value);
                        exit(-1);
                    }
                    data_index++;
                }
            }
            my_current_chunk_id = my_current_chunk_id + mpi_size;
            sleep(3);
        }
        free(data);
        H5Sclose(file_space);
        H5Sclose(memspace_id);
        H5Dclose(datasetId);
        H5Gclose(group_id);
        H5Fclose(file_id);

        if (find_mis_matched == 0)
            printf("[Test Passed] Prefetch data from Disk to BB , no error!\n");
    }

//H5Pclose(acc_tpl);
#ifdef REGISTER_VOL_BY_HAND
    H5VLunregister_connector(de_vol_id);
#endif
    H5Pclose(de_fapl);

    //H5VLclose(native_driver_id_g);
    //H5VLterminate(vol_id, H5P_DEFAULT);
    //assert(H5VLis_registered("data_elevator") == 0);

    MPI_Finalize();

    return 0;
}
