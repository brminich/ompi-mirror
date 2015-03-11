/*
 * Copyright (c) 2013      Mellanox Technologies, Inc.
 *                         All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

#include "oshmem_config.h"

#include "oshmem/constants.h"
#include "oshmem/runtime/runtime.h"
#include "oshmem/runtime/params.h"

int oshmem_shmem_allgather(void *send_buf, void *rcv_buf, int elem_size)
{
    int rc;

    rc = MPI_Allgather(send_buf, elem_size, MPI_BYTE,
                        rcv_buf, elem_size, MPI_BYTE, oshmem_comm_world);

    return rc;
}

int oshmem_shmem_allgatherv(void *send_buf, void* rcv_buf, int send_count,
                            int* rcv_size, int* displs)
{
    int rc;

    rc = MPI_Allgatherv(send_buf, send_count, MPI_BYTE,
                         rcv_buf, rcv_size, displs, MPI_BYTE, oshmem_comm_world);

    return rc;
}

int oshmem_shmem_allreduce(void *send_buf, int send_count, void *rcv_buf, MPI_Datatype datatype, MPI_Op op)
{
    int rc;

    rc = MPI_Allreduce(send_buf, rcv_buf, send_count, 
            datatype, op, oshmem_comm_world);
    return rc;

}

int oshmem_shmem_alltoall(void *send_buf, int send_count,
                void *rcv_buf, int rcv_count)
{
    int rc;

    rc = MPI_Alltoall(send_buf, send_count, MPI_BYTE,
            rcv_buf, rcv_count, MPI_BYTE, oshmem_comm_world);
    return rc;
}

void oshmem_shmem_barrier(void)
{
    MPI_Barrier(oshmem_comm_world);
}
