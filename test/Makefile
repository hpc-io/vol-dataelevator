H5_HOME=/Users/dbin/work/soft/hdf5-dev/build
DE_HOME=../build
LIBS=-lhdf5 
export CXXFLAGS="-std=c++11"
export CFLAGS="-std=c99"
CCC=mpicc

all:test

test: test.c
	##Run this two command before make
	##export HDF5_VOL_CONNECTOR="dataelevator under_vol=0;under_info={};"
	##export HDF5_PLUGIN_PATH=../src
	@echo "${HDF5_VOL_CONNECTOR}"
	@echo "${HDF5_PLUGIN_PATH}"
	$(CCC) -g -o test2 test.c -L$(H5_HOME)/lib  -I$(H5_HOME)/include $(LIBS)


clean:
	rm test
