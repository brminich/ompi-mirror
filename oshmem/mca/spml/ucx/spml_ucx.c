/*
 * Copyright (c) 2013      Mellanox Technologies, Inc.
 *                         All rights reserved.
 * Copyright (c) 2014      Research Organization for Information Science
 *                         and Technology (RIST). All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

#define _GNU_SOURCE
#include <stdio.h>

#include <sys/types.h>
#include <unistd.h>
#include <stdint.h>

#include "oshmem_config.h"
#include "opal/datatype/opal_convertor.h"
#include "orte/include/orte/types.h"
#include "orte/runtime/orte_globals.h"
#include "oshmem/mca/spml/ucx/spml_ucx.h"
#include "oshmem/include/shmem.h"
#include "oshmem/mca/memheap/memheap.h"
#include "oshmem/mca/memheap/base/base.h"
#include "oshmem/proc/proc.h"
#include "oshmem/mca/spml/base/base.h"
#include "oshmem/mca/spml/base/spml_base_putreq.h"
#include "oshmem/runtime/runtime.h"
#include "orte/util/show_help.h"

#include "oshmem/mca/spml/ucx/spml_ucx_component.h"

/* Turn ON/OFF debug output from build (default 0) */
#ifndef SPML_UCX_PUT_DEBUG
#define SPML_UCX_PUT_DEBUG    0
#endif


mca_spml_ucx_t mca_spml_ucx = {
    {
        /* Init mca_spml_base_module_t */
        mca_spml_ucx_add_procs,
        mca_spml_ucx_del_procs,
        mca_spml_ucx_enable,
        mca_spml_ucx_register,
        mca_spml_ucx_deregister,
        mca_spml_base_oob_get_mkeys,
        mca_spml_ucx_put,
        mca_spml_ucx_put_nb,
        mca_spml_ucx_get,
        NULL, //mca_spml_ucx_recv,
        NULL, //mca_spml_ucx_send,
        mca_spml_base_wait,
        mca_spml_base_wait_nb,
        mca_spml_ucx_quiet, /* At the moment fence is the same as quite for 
                               every spml */
        mca_spml_ucx_rmkey_unpack,
        mca_spml_ucx_rmkey_free,
        (void*)&mca_spml_ucx
    }
};

int mca_spml_ucx_enable(bool enable)
{
    SPML_VERBOSE(50, "*** ucx ENABLED ****");
    if (false == enable) {
        return OSHMEM_SUCCESS;
    }

    mca_spml_ucx.enabled = true;

    return OSHMEM_SUCCESS;
}

int mca_spml_ucx_del_procs(oshmem_proc_t** procs, size_t nprocs)
{
    size_t i, n;
    int my_rank = oshmem_my_proc_id();

    oshmem_shmem_barrier();

    if (!mca_spml_ucx.ucp_peers) {
        return OSHMEM_SUCCESS;
    }

     for (n = 0; n < nprocs; n++) {
         i = (my_rank + n) % nprocs;
         if (mca_spml_ucx.ucp_peers[i].ucp_conn) {
             ucp_ep_destroy(mca_spml_ucx.ucp_peers[i].ucp_conn);
         }
     }

    free(mca_spml_ucx.ucp_peers);
    return OSHMEM_SUCCESS;
}

int mca_spml_ucx_add_procs(oshmem_proc_t** procs, size_t nprocs)
{
    size_t i, n;
    int rc = OSHMEM_ERROR;
    int my_rank = oshmem_my_proc_id();
    ucs_status_t err;
    ucp_iface_attr_t attr;
    ucp_ep_addr_t *ep_local_addrs;
    char *ep_remote_addrs;
    int ep_raddr_len;


    mca_spml_ucx.ucp_peers = (ucp_peer_t *) calloc(nprocs, sizeof(*(mca_spml_ucx.ucp_peers)));
    if (NULL == mca_spml_ucx.ucp_peers) {
        goto error;
    }

    err = ucp_iface_query(mca_spml_ucx.ucp_iface, &attr);
    if (UCS_OK != err) {
        goto error2;
    }


    ep_local_addrs = (ucp_ep_addr_t *)malloc(attr.ep_addr_len*nprocs);
    if (!ep_local_addrs) {
        goto error2;
    }

    rc = oshmem_shmem_allreduce(&attr.ep_addr_len, 1, &ep_raddr_len,
            MPI_INT, MPI_MAX);
    if (MPI_SUCCESS != rc) {
        SPML_ERROR("allreduce failed");
        oshmem_shmem_abort(-1);
        goto error3;
    }


    ep_remote_addrs = (char *)malloc(ep_raddr_len*nprocs);
    if (!ep_remote_addrs) {
        goto error3;
    }

    for (i = 0; i < nprocs; ++i) {
        err = ucp_ep_create(mca_spml_ucx.ucp_iface, 
                &mca_spml_ucx.ucp_peers[i].ucp_conn);
        if (UCS_OK != err) {
            goto error5;
        }

        ucp_ep_pack_address(mca_spml_ucx.ucp_peers[i].ucp_conn,
                ep_local_addrs + i);
    }
    
    rc = oshmem_shmem_alltoall(ep_local_addrs, attr.ep_addr_len, 
                               ep_remote_addrs, ep_raddr_len);
    if (MPI_SUCCESS != rc) {
        SPML_ERROR("alltoall failed");
        oshmem_shmem_abort(-1);
        goto error5;
    }

    opal_progress_register(spml_ucx_progress);

    /* Get the EP connection requests for all the processes from modex */
    for (n = 0; n < nprocs; ++n) {
        i = (my_rank + n) % nprocs;
        mca_spml_ucx.ucp_peers[i].pe = i;

        err = ucp_ep_connect_to_ep(mca_spml_ucx.ucp_peers[i].ucp_conn, 
                (ucp_ep_addr_t *)(ep_remote_addrs + i*ep_raddr_len));
        if (UCS_OK != err) {
            goto error5;
        }
    }

    free(ep_remote_addrs);
    free(ep_local_addrs);
    SPML_VERBOSE(50, "*** ADDED PROCS ***");
    return OSHMEM_SUCCESS;

error5:
    for (i = 0; i < nprocs; ++i) {
         if (mca_spml_ucx.ucp_peers[i].ucp_conn) {
             ucp_ep_destroy(mca_spml_ucx.ucp_peers[i].ucp_conn);
         }
    }
error4:
    free(ep_remote_addrs);
error3:
    free(ep_local_addrs);
error2:
    free(mca_spml_ucx.ucp_peers);
error:
    rc = OSHMEM_ERR_OUT_OF_RESOURCE;
    SPML_ERROR("add procs FAILED rc=%d", rc);
    return rc;

}

void mca_spml_ucx_rmkey_free(sshmem_mkey_t *mkey)
{
    spml_ucx_mkey_t   *ucx_mkey;

    if (!mkey->spml_context) {
        return;
    }
    ucx_mkey = (spml_ucx_mkey_t *)(mkey->spml_context);
    ucp_rkey_destroy(ucx_mkey->rkey);
    free(ucx_mkey);
}

void mca_spml_ucx_rmkey_unpack(sshmem_mkey_t *mkey)
{
    spml_ucx_mkey_t   *ucx_mkey;
    ucs_status_t err;

    ucx_mkey = (spml_ucx_mkey_t *)malloc(sizeof(*ucx_mkey));
    if (!ucx_mkey) {
        SPML_ERROR("not enough memory to allocate mkey");
        goto error_fatal;
    }
    
    err = ucp_rkey_unpack(mca_spml_ucx.ucp_context, mkey->u.data, 
            &ucx_mkey->rkey); 
    if (UCS_OK != err) {
        SPML_ERROR("failed to unpack rkey");
        goto error_fatal;
    }

    mkey->spml_context = ucx_mkey;

    return;

error_fatal:
    oshmem_shmem_abort(-1);
    return;
}

sshmem_mkey_t *mca_spml_ucx_register(void* addr,
                                         size_t size,
                                         uint64_t shmid,
                                         int *count)
{
    int i;
    sshmem_mkey_t *mkeys;
    ucs_status_t err;
    spml_ucx_mkey_t   *ucx_mkey;
    size_t len;

    *count = 0;
    mkeys = (sshmem_mkey_t *) calloc(1, sizeof(*mkeys));
    if (!mkeys) {
        return NULL ;
    }

    ucx_mkey = (spml_ucx_mkey_t *)malloc(sizeof(*ucx_mkey));
    if (!ucx_mkey) {
        goto error_out;
    }
        
    mkeys[0].spml_context = ucx_mkey;
    err = ucp_mem_map(mca_spml_ucx.ucp_context, &addr, size, 0, &ucx_mkey->lkey);
    if (UCS_OK != err) {
        goto error_out1;
    }

    err = ucp_rkey_pack(ucx_mkey->lkey, &mkeys[0].u.data, &len); 
    if (UCS_OK != err) {
        goto error_unmap;
    }
    if (len >= 0xffff) {
        SPML_ERROR("packed rkey is too long: %llu >= %d",
                (unsigned long long)len,
                0xffff);
        oshmem_shmem_abort(-1);
    }

    mkeys[0].len = len;
    *count = 1;
    return mkeys;

error_unmap:
    ucp_mem_unmap(mca_spml_ucx.ucp_context, ucx_mkey->lkey);
error_out1:
    free(ucx_mkey);
error_out:
    free(mkeys);

    return NULL ;
}

int mca_spml_ucx_deregister(sshmem_mkey_t *mkeys)
{
    spml_ucx_mkey_t   *ucx_mkey;

    MCA_SPML_CALL(fence());
    if (!mkeys)
        return OSHMEM_SUCCESS;

    if (!mkeys[0].spml_context) 
        return OSHMEM_SUCCESS;

    ucx_mkey = (spml_ucx_mkey_t *)mkeys[0].spml_context;
    ucp_mem_unmap(mca_spml_ucx.ucp_context, ucx_mkey->lkey);

    if (0 < mkeys[0].len) {
        free(mkeys[0].u.data);
    }

    free(ucx_mkey);
    return OSHMEM_SUCCESS;
}



int mca_spml_ucx_get(void *src_addr, size_t size, void *dst_addr, int src)
{
    void *rva;
    sshmem_mkey_t *r_mkey;
    ucs_status_t err;
    spml_ucx_mkey_t *ucx_mkey;

    r_mkey = mca_memheap.memheap_get_cached_mkey(src,
            src_addr,
            0,
            &rva);

    if (!r_mkey) {
        SPML_ERROR("pe=%d: %p is not address of shared variable",
                src, src_addr);
        oshmem_shmem_abort(-1);
        return OSHMEM_ERROR;
    }

    ucx_mkey = (spml_ucx_mkey_t *)(r_mkey->spml_context);
    err = ucp_ep_get(mca_spml_ucx.ucp_peers[src].ucp_conn, dst_addr, size, 
            (uint64_t)rva, ucx_mkey->rkey);

    return UCS_OK == err ? OSHMEM_SUCCESS : OSHMEM_ERROR;
}



int mca_spml_ucx_put(void* dst_addr, size_t size, void* src_addr, int dst)
{
    void *rva;
    sshmem_mkey_t *r_mkey;
    ucs_status_t err;
    spml_ucx_mkey_t *ucx_mkey;

    r_mkey = mca_memheap.memheap_get_cached_mkey(dst,
            dst_addr,
            0,
            &rva);

    if (!r_mkey) {
        SPML_ERROR("pe=%d: %p is not address of shared variable",
                dst, dst_addr);
        oshmem_shmem_abort(-1);
        return OSHMEM_ERROR;
    }

    ucx_mkey = (spml_ucx_mkey_t *)(r_mkey->spml_context);
    err = ucp_ep_put(mca_spml_ucx.ucp_peers[dst].ucp_conn, src_addr, size, 
            (uint64_t)rva, ucx_mkey->rkey);

    return UCS_OK == err ? OSHMEM_SUCCESS : OSHMEM_ERROR;
}


int mca_spml_ucx_fence(void)
{
    ucs_status_t err;
    ucp_fence(mca_spml_ucx.ucp_context);

    if (UCS_OK != err) {
        SPML_ERROR("fence failed");
         oshmem_shmem_abort(-1);
         return OSHMEM_ERROR;
    }
    return OSHMEM_SUCCESS;
}

int mca_spml_ucx_quiet(void)
{
    ucs_status_t err;
    ucp_fence(mca_spml_ucx.ucp_context);

    if (UCS_OK != err) {
        SPML_ERROR("fence failed");
         oshmem_shmem_abort(-1);
         return OSHMEM_ERROR;
    }
    return OSHMEM_SUCCESS;
}
/* blocking receive */
int mca_spml_ucx_recv(void* buf, size_t size, int src)
{
    return OSHMEM_SUCCESS;
}

/* for now only do blocking copy send */
int mca_spml_ucx_send(void* buf,
                        size_t size,
                        int dst,
                        mca_spml_base_put_mode_t mode)
{
    return OSHMEM_SUCCESS;
}
