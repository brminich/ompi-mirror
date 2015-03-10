/*
 * Copyright (c) 2013      Mellanox Technologies, Inc.
 *                         All rights reserved.
 * $COPYRIGHT$
 * 
 * Additional copyrights may follow
 * 
 * $HEADER$
 */
/**
 *  @file 
 */

#ifndef MCA_SPML_UCX_H
#define MCA_SPML_UCX_H

#include "oshmem_config.h"
#include "oshmem/request/request.h"
#include "oshmem/mca/spml/spml.h"
#include "oshmem/util/oshmem_util.h"
#include "oshmem/mca/spml/base/spml_base_putreq.h"
#include "oshmem/proc/proc.h"
#include "oshmem/mca/spml/base/spml_base_request.h"
#include "oshmem/mca/spml/base/spml_base_getreq.h"

#include "orte/runtime/orte_globals.h"

#include <ucp/api/ucp.h>

BEGIN_C_DECLS

/**
 * UD MXM SPML module
 */
struct ucp_peer {
    ucp_ep_h        ucp_conn;
    int             pe;
};

typedef struct ucp_peer ucp_peer_t;

struct mca_spml_ucx {
    mca_spml_base_module_t   super;
    ucp_context_h            ucp_context;
    ucp_iface_h              ucp_iface;
    ucp_peer_t              *ucp_peers;

    int                      priority; /* component priority */
    bool                     enabled;
    int                      np;
};

typedef struct mca_spml_ucx mca_spml_ucx_t;

struct spml_ucx_mkey {
    ucp_lkey_h lkey;
    ucp_rkey_h rkey;
}; 

typedef struct spml_ucx_mkey spml_ucx_mkey_t;


extern mca_spml_ucx_t mca_spml_ucx;

extern int mca_spml_ucx_enable(bool enable);
extern int mca_spml_ucx_get(void* dst_addr,
                              size_t size,
                              void* src_addr,
                              int src);
/* extension. used 4 fence implementation b4 fence was added to mxm */
extern int mca_spml_ucx_get_async(void *src_addr,
                                    size_t size,
                                    void *dst_addr,
                                    int src);

extern int mca_spml_ucx_put(void* dst_addr,
                              size_t size,
                              void* src_addr,
                              int dst);

extern int mca_spml_ucx_put_nb(void* dst_addr,
                                 size_t size,
                                 void* src_addr,
                                 int dst,
                                 void **handle);

extern int mca_spml_ucx_recv(void* buf, size_t size, int src);
extern int mca_spml_ucx_send(void* buf,
                               size_t size,
                               int dst,
                               mca_spml_base_put_mode_t mode);

extern sshmem_mkey_t *mca_spml_ucx_register(void* addr,
                                                size_t size,
                                                uint64_t shmid,
                                                int *count);
extern int mca_spml_ucx_deregister(sshmem_mkey_t *mkeys);

extern void mca_spml_ucx_rmkey_unpack(sshmem_mkey_t *mkey);
extern void mca_spml_ucx_rmkey_free(sshmem_mkey_t *mkey);

extern int mca_spml_ucx_add_procs(oshmem_proc_t** procs, size_t nprocs);
extern int mca_spml_ucx_del_procs(oshmem_proc_t** procs, size_t nprocs);
extern int mca_spml_ucx_fence(void);
extern int mca_spml_ucx_quiet(void);
extern int spml_ucx_progress(void);

static inline oshmem_proc_t *mca_spml_ucx_proc_find(int dst)
{
    orte_process_name_t name;

    name.jobid = ORTE_PROC_MY_NAME->jobid;
    name.vpid = dst;
    return oshmem_proc_find(&name);
}

END_C_DECLS

#endif

