#include <stdio.h>
#include "mpi.h"
#include "record-op-file.h"

static int numsend = 0;
int MPI_Finalize() {
	int me;
	PMPI_Comm_rank(MPI_COMM_WORLD, &me);
        if(me == 0){
          //printf("MPI Finalize is called HHH \n");
          //DB TEst
          AppendRecord("NULL", "NULL", DE_WRITE_BB_DONE, 0);
          char  sv[256];
          sprintf(sv, "%d", DE_WRITE_BB_DONE);
          UpdateRecord("NULL", DE_TYPE_S, sv);
        }
        
	return PMPI_Finalize();
}

