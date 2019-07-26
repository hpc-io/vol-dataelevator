CC=mpicc
HDF5_DIR=/Users/dbin/work/soft/hdf5-dev/build
CFLAGS=-I$(HDF5_DIR)/include -L$(HDF5_DIR)/lib
LIBS=-lhdf5
TARGET=libdevol.so
makeso:
	$(CC) -shared $(CFLAGS) $(LIBS) -o $(TARGET) -fPIC data_elevator_vol.c record-op-file.c db-file.c mpi-finalize.c de-error.c  merge-array.c 

clean:
	rm $(TARGET)
