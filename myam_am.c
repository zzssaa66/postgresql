#include "myam.h"
#include "access/genam.h"
#include "access/amapi.h"
#include "access/tableam.h"
#include "catalog/index.h"
#include "miscadmin.h"
#include "storage/bufmgr.h"
#include "storage/indexfsm.h"
#include "utils/memutils.h"
#include "utils/rel.h"
#include "utils/builtins.h"
#include "utils/datum.h"
#include "utils/hsearch.h"
#include "executor/executor.h"
#include "optimizer/cost.h"
#include "utils/typcache.h"
#include "common/hashfn.h"
#include <float.h>
#include "optimizer/optimizer.h"
#include "nodes/execnodes.h"
#include "access/tupdesc.h"
#include "utils/snapmgr.h"
#include "utils/varlena.h"
#include "postmaster/bgworker.h"
#include "storage/latch.h"
#include "storage/lwlock.h"
#include "storage/proc.h"
#include "storage/procsignal.h"
#include "tcop/tcopprot.h"
#include "utils/ps_status.h"
#include "utils/tuplestore.h"
#include "postmaster/postmaster.h"
#include "funcapi.h"
#include <math.h>
#include "utils/fmgroids.h"
#include "utils/syscache.h"
#include "catalog/pg_am.h"
#include <time.h>
#include "utils/lsyscache.h"
#include "utils/selfuncs.h"
#include "access/nbtree.h"
#include "access/table.h"
#include "access/generic_xlog.h"
#include "access/xloginsert.h"
#include "storage/ipc.h"
#include "storage/bulk_write.h"

/* Include the utility functions implementation */
#include "myam_utils.c"

PG_MODULE_MAGIC;

HTAB *myam_meta_hash = NULL;

/* ==================== Phase 4: Named LWLock tranche ==================== */

#define MYAM_NUM_LWLOCKS  64   /* hash buckets; up to 64 independently-locked indexes */

static LWLockPadded            *myam_lwlock_array      = NULL;
static shmem_request_hook_type  prev_shmem_request_hook = NULL;
static shmem_startup_hook_type  prev_shmem_startup_hook = NULL;

static void
myam_shmem_request(void)
{
    if (prev_shmem_request_hook)
        prev_shmem_request_hook();
    RequestNamedLWLockTranche("MyAMIndex", MYAM_NUM_LWLOCKS);
}

static void
myam_shmem_startup(void)
{
    if (prev_shmem_startup_hook)
        prev_shmem_startup_hook();
    myam_lwlock_array = GetNamedLWLockTranche("MyAMIndex");
}

void
_PG_init(void)
{
    prev_shmem_request_hook = shmem_request_hook;
    shmem_request_hook      = myam_shmem_request;
    prev_shmem_startup_hook = shmem_startup_hook;
    shmem_startup_hook      = myam_shmem_startup;

    if (process_shared_preload_libraries_in_progress)
        myam_register_bgworker();
}

static LWLock *
myam_get_lwlock(Oid oid)
{
    if (myam_lwlock_array == NULL)
        elog(ERROR, "MyAM: not in shared_preload_libraries; cannot acquire index lock");
    return &myam_lwlock_array[oid % MYAM_NUM_LWLOCKS].lock;
}

static void
myam_meta_refresh(Relation index, MyamMeta *meta)
{
    /*
     * Only read the on-disk meta page when root_blkno is unknown.
     * After the first successful lookup the cached value is good enough;
     * writers (myam_insert, myam_bulkdelete) call this unconditionally
     * under LW_EXCLUSIVE, so structural changes are always propagated.
     * Calling ReadBuffer + RelationGetNumberOfBlocks on every point query
     * would add ~10 µs of syscall overhead per lookup (devastating at 10M
     * queries/benchmark run).
     */
    if (meta->root_blkno == InvalidBlockNumber &&
        RelationGetNumberOfBlocks(index) > 0)
        myam_read_meta_page(index, meta);
}

/* ==================== End Phase 4 ==================== */

/* Forward declaration for ambuildempty */
static void myam_buildempty(Relation index);

/* Helper to get AM name */
static char *
get_am_name(Oid am_oid)
{
    HeapTuple tuple;
    Form_pg_am amform;
    char *name;

    tuple = SearchSysCache1(AMOID, ObjectIdGetDatum(am_oid));
    if (!HeapTupleIsValid(tuple))
        return "unknown";

    amform = (Form_pg_am) GETSTRUCT(tuple);
    name = pstrdup(NameStr(amform->amname));
    ReleaseSysCache(tuple);
    return name;
}

/* ==================== B+树操作 ==================== */

/* ==================== Page Operations ==================== */

/*
 * Allocate a new page via the buffer manager (disk-backed).
 * Returns the buffer with an exclusive content lock held and the page
 * already initialised.  Caller is responsible for WAL-logging and
 * releasing the buffer (UnlockReleaseBuffer).
 */
Buffer
myam_page_new(Relation index, MyamMeta *meta, uint16 flags)
{
    Buffer          buf;
    Page            page;
    MyamPageOpaque  opaque;

    buf  = ReadBuffer(index, P_NEW);
    LockBuffer(buf, BUFFER_LOCK_EXCLUSIVE);
    page = BufferGetPage(buf);

    PageInit(page, BLCKSZ, sizeof(MyamPageOpaqueData));

    opaque          = MyamPageGetOpaque(page);
    opaque->flags   = flags;
    opaque->level   = (flags & MYAM_LEAF_PAGE) ? 0 : 1;
    opaque->max_off = 0;
    opaque->prev    = InvalidBlockNumber;
    opaque->next    = InvalidBlockNumber;
    opaque->parent  = InvalidBlockNumber;

    meta->total_pages++;

    return buf;
}

/* ── On-disk Meta Page (block 0) ─────────────────────────────────────────── */

/*
 * Write a fresh meta page at block 0.  Called once during index creation.
 */
void
myam_init_meta_page(Relation index, MyamMeta *meta)
{
    Buffer          buf;
    Page            page;
    MyamDiskMetaData dm;

    buf  = ReadBuffer(index, P_NEW);   /* first alloc → block 0 */
    Assert(BufferGetBlockNumber(buf) == MYAM_META_BLKNO);
    LockBuffer(buf, BUFFER_LOCK_EXCLUSIVE);
    page = BufferGetPage(buf);

    PageInit(page, BLCKSZ, sizeof(MyamPageOpaqueData));
    MyamPageGetOpaque(page)->flags = MYAM_META_PAGE;

    /* Pack MyamDiskMetaData as the sole item on block 0 */
    dm.magic        = MYAM_PAGE_MAGIC;
    dm.version      = MYAM_DISK_META_VERSION;
    dm.root_blkno   = meta->root_blkno;
    dm.tree_height  = meta->tree_height;
    dm.total_pages  = meta->total_pages;
    dm.total_tuples = meta->total_tuples;

    if (PageAddItem(page, (Item) &dm, sizeof(dm),
                    FirstOffsetNumber, false, false) == InvalidOffsetNumber)
        elog(ERROR, "MyAM: could not add meta item to block 0");

    {
        GenericXLogState *xlog = GenericXLogStart(index);
        GenericXLogRegisterBuffer(xlog, buf, GENERIC_XLOG_FULL_IMAGE);
        GenericXLogFinish(xlog);
    }
    UnlockReleaseBuffer(buf);
}

/*
 * Overwrite the existing meta page with current in-memory state.
 */
void
myam_write_meta_page(Relation index, MyamMeta *meta)
{
    Buffer          buf;
    Page            page;
    MyamDiskMetaData dm;

    buf  = ReadBuffer(index, MYAM_META_BLKNO);
    LockBuffer(buf, BUFFER_LOCK_EXCLUSIVE);
    page = BufferGetPage(buf);

    /* Rewrite the single item */
    dm.magic        = MYAM_PAGE_MAGIC;
    dm.version      = MYAM_DISK_META_VERSION;
    dm.root_blkno   = meta->root_blkno;
    dm.tree_height  = meta->tree_height;
    dm.total_pages  = meta->total_pages;
    dm.total_tuples = meta->total_tuples;

    /* Clear page and re-add (simplest approach for a single-item page) */
    PageInit(page, BLCKSZ, sizeof(MyamPageOpaqueData));
    MyamPageGetOpaque(page)->flags = MYAM_META_PAGE;

    if (PageAddItem(page, (Item) &dm, sizeof(dm),
                    FirstOffsetNumber, false, false) == InvalidOffsetNumber)
        elog(ERROR, "MyAM: could not rewrite meta page");

    {
        GenericXLogState *xlog = GenericXLogStart(index);
        GenericXLogRegisterBuffer(xlog, buf, GENERIC_XLOG_FULL_IMAGE);
        GenericXLogFinish(xlog);
    }
    UnlockReleaseBuffer(buf);
}

/*
 * Read meta page from disk into in-memory MyamMeta.
 */
void
myam_read_meta_page(Relation index, MyamMeta *meta)
{
    Buffer          buf;
    Page            page;
    MyamDiskMetaData dm;

    buf  = ReadBuffer(index, MYAM_META_BLKNO);
    LockBuffer(buf, BUFFER_LOCK_SHARE);
    page = BufferGetPage(buf);

    if (PageGetMaxOffsetNumber(page) < FirstOffsetNumber)
        elog(ERROR, "MyAM: meta page is empty");

    ItemId iid = PageGetItemId(page, FirstOffsetNumber);
    memcpy(&dm, PageGetItem(page, iid), sizeof(dm));

    UnlockReleaseBuffer(buf);

    if (dm.magic != MYAM_PAGE_MAGIC)
        elog(ERROR, "MyAM: bad magic in meta page");
    if (dm.version != MYAM_DISK_META_VERSION)
        elog(ERROR, "MyAM: unsupported meta version %u", dm.version);

    meta->root_blkno   = dm.root_blkno;
    meta->tree_height  = dm.tree_height;
    meta->total_pages  = dm.total_pages;
    meta->total_tuples = dm.total_tuples;
}

/*
 * Insert serialized tuple into page
 */
bool
myam_page_insert_tuple(MyamMeta *meta, MyamPage page, MyamIndexTuple tuple, OffsetNumber offset,
                       OffsetNumber *inserted_off)
{
    OffsetNumber off = PageAddItem(page, (Item) tuple, tuple->t_info, offset, false, false);
    MyamPageOpaque opaque;
    
    if (off == InvalidOffsetNumber)
    {
        /* Page full */
        return false;
    }
    
    if (inserted_off)
        *inserted_off = off;

    opaque = MyamPageGetOpaque(page);
    if (PageGetMaxOffsetNumber(page) > opaque->max_off)
        opaque->max_off = PageGetMaxOffsetNumber(page);
        
    return true;
}

static bool
myam_page_insert_item(MyamMeta *meta, MyamPage page, Item item, Size size, OffsetNumber offset,
                      OffsetNumber *inserted_off)
{
    OffsetNumber off = PageAddItem(page, item, size, offset, false, false);
    MyamPageOpaque opaque;

    if (off == InvalidOffsetNumber)
        return false;

    if (inserted_off)
        *inserted_off = off;

    opaque = MyamPageGetOpaque(page);
    if (PageGetMaxOffsetNumber(page) > opaque->max_off)
        opaque->max_off = PageGetMaxOffsetNumber(page);

    return true;
}

static int
myam_compare_tupledata_key(MyamMeta *meta, const char *tuple_data, MyamKey *key)
{
    int null_bitmap_len = (meta->nkeys + 7) / 8;
    const char *ptr = tuple_data + null_bitmap_len;

    for (int i = 0; i < meta->nkeys; i++)
    {
        bool tuple_null = (tuple_data[i/8] & (1 << (i%8)));
        bool key_null = key->isnull[i];

        if (tuple_null && key_null)
            continue;
        if (tuple_null)
            return 1;
        if (key_null)
            return -1;

        Datum tuple_val;
        Size len;

        if (meta->key_typlen[i] == -1)
        {
            struct varlena *v = (struct varlena *) ptr;
            len = VARSIZE_ANY(v);
            tuple_val = PointerGetDatum(v);
        }
        else
        {
            len = meta->key_typlen[i];
            if (meta->key_byval[i])
                memcpy(&tuple_val, ptr, len);
            else
                tuple_val = PointerGetDatum(ptr);
        }

        int32 cmp = myam_cmp_col(meta, i, tuple_val, key->keys[i]);
        if (cmp != 0)
            return cmp;

        ptr += len;
    }

    return 0;
}

static int
myam_compare_internal_tuple_key(MyamMeta *meta, MyamInternalTuple tuple, MyamKey *key)
{
    return myam_compare_tupledata_key(meta, tuple->data, key);
}

static OffsetNumber
myam_internal_find_child_off(MyamMeta *meta, MyamPage page, MyamKey *key)
{
    OffsetNumber max_off = PageGetMaxOffsetNumber(page);
    OffsetNumber lo = FirstOffsetNumber;
    OffsetNumber hi = max_off;
    OffsetNumber best = FirstOffsetNumber;

    if (max_off == 0)
        return InvalidOffsetNumber;

    while (lo <= hi)
    {
        OffsetNumber mid = lo + ((hi - lo) >> 1);
        ItemId itemid = PageGetItemId(page, mid);
        MyamInternalTuple tuple = (MyamInternalTuple) PageGetItem(page, itemid);

        int cmp = myam_compare_internal_tuple_key(meta, tuple, key);
        if (cmp <= 0)
        {
            best = mid;
            lo = mid + 1;
        }
        else
        {
            if (mid == FirstOffsetNumber)
                break;
            hi = mid - 1;
        }
    }

    return best;
}

/* Return child BlockNumber stored in an internal tuple at offset off. */
static BlockNumber
myam_internal_get_child_blkno(MyamPage page, OffsetNumber off)
{
    ItemId           iid   = PageGetItemId(page, off);
    MyamInternalTuple tuple = (MyamInternalTuple) PageGetItem(page, iid);
    return tuple->child_blkno;
}

static MyamInternalTuple
myam_form_internal_tuple(MyamMeta *meta, MyamKey *key, BlockNumber child_blkno)
{
    Size total_size = offsetof(MyamInternalTupleData, data);
    Size data_size = 0;
    int null_bitmap_len = (meta->nkeys + 7) / 8;

    total_size += null_bitmap_len;

    for (int i = 0; i < meta->nkeys; i++)
    {
        if (!key->isnull[i])
        {
            if (meta->key_typlen[i] == -1)
            {
                struct varlena *orig = (struct varlena *) DatumGetPointer(key->keys[i]);
                struct varlena *v = PG_DETOAST_DATUM_PACKED(key->keys[i]);
                data_size += VARSIZE_ANY(v);
                if ((Pointer) v != (Pointer) orig)
                    pfree(v);
            }
            else if (meta->key_typlen[i] > 0)
                data_size += meta->key_typlen[i];
            else
                data_size += strlen(DatumGetCString(key->keys[i])) + 1;
        }
    }

    total_size += data_size;

    MyamInternalTuple tuple = (MyamInternalTuple) palloc(total_size);
    tuple->child_blkno = child_blkno;
    tuple->t_info = total_size;

    char *ptr = tuple->data;

    memset(ptr, 0, null_bitmap_len);
    for (int i = 0; i < meta->nkeys; i++)
    {
        if (key->isnull[i])
            ptr[i/8] |= (1 << (i%8));
    }
    ptr += null_bitmap_len;

    for (int i = 0; i < meta->nkeys; i++)
    {
        if (!key->isnull[i])
        {
            if (meta->key_typlen[i] == -1)
            {
                struct varlena *orig = (struct varlena *) DatumGetPointer(key->keys[i]);
                struct varlena *v = PG_DETOAST_DATUM_PACKED(key->keys[i]);
                Size len = VARSIZE_ANY(v);
                memcpy(ptr, (char *) v, len);
                if ((Pointer) v != (Pointer) orig)
                    pfree(v);
                ptr += len;
            }
            else
            {
                Size len = meta->key_typlen[i];
                char *data_ptr;
                if (meta->key_byval[i])
                    data_ptr = (char *) &key->keys[i];
                else
                    data_ptr = DatumGetPointer(key->keys[i]);

                memcpy(ptr, data_ptr, len);
                ptr += len;
            }
        }
    }

    return tuple;
}

/* ==================== Bulk Build ==================== */

typedef struct SortItem
{
    ItemPointerData tid;    /* heap TID */
    MyamKey         key;    /* raw key values for sorting */
} SortItem;

static MyamMeta *build_meta = NULL;

static int
myam_sort_item_compare(const void *a, const void *b)
{
    const SortItem *s1 = (const SortItem *)a;
    const SortItem *s2 = (const SortItem *)b;
    
    return myam_key_compare(build_meta, (MyamKey *)&s1->key, (MyamKey *)&s2->key);
}

typedef struct MyamBuildState
{
    MyamMeta *meta;
    SortItem *items;
    int32 capacity;
    int32 indtuples;
} MyamBuildState;

static void
myam_build_callback(Relation index, ItemPointer tid, Datum *values, bool *isnull,
                    bool tupleIsAlive, void *state)
{
    MyamBuildState *buildstate = (MyamBuildState *) state;
    MyamMeta *meta = buildstate->meta;
    SortItem *item;
    int i;

    (void) index;
    (void) tupleIsAlive;

    if (buildstate->indtuples == buildstate->capacity)
    {
        int32 newcap = (buildstate->capacity == 0) ? 1024 : buildstate->capacity * 2;
        if (buildstate->items == NULL)
            buildstate->items = (SortItem *) palloc(sizeof(SortItem) * newcap);
        else
            buildstate->items = (SortItem *) repalloc(buildstate->items, sizeof(SortItem) * newcap);
        buildstate->capacity = newcap;
    }

    item = &buildstate->items[buildstate->indtuples];
    item->tid = *tid;
    item->key.nkeys = meta->nkeys;

    /*
     * 直接将 Datum 值存入 SortItem.key，避免 form_tuple + deform_tuple 的
     * 双重序列化/反序列化开销。byval 类型（int4/int8）直接赋值；byref 类型
     * 需要深拷贝以防原指针在回调返回后失效。
     */
    for (i = 0; i < meta->nkeys; i++)
    {
        item->key.isnull[i] = isnull[i];
        if (!isnull[i])
        {
            if (meta->key_byval[i])
            {
                /* byval: 直接赋值，无需分配 */
                item->key.keys[i] = values[i];
            }
            else if (meta->key_typlen[i] == -1)
            {
                /* varlena: 深拷贝到 index_cxt */
                MemoryContext oldcxt = MemoryContextSwitchTo(meta->index_cxt);
                item->key.keys[i] = PointerGetDatum(PG_DETOAST_DATUM_COPY(values[i]));
                MemoryContextSwitchTo(oldcxt);
            }
            else
            {
                /* fixed-size pass-by-ref */
                MemoryContext oldcxt = MemoryContextSwitchTo(meta->index_cxt);
                item->key.keys[i] = datumCopy(values[i], false, meta->key_typlen[i]);
                MemoryContextSwitchTo(oldcxt);
            }
        }
        else
        {
            item->key.keys[i] = (Datum) 0;
        }
    }

    buildstate->indtuples++;
}

void
myam_bulk_build(MyamMeta *meta, Relation heap, Relation index, struct IndexInfo *indexInfo,
                IndexBuildResult *result)
{
    MemoryContext oldcxt;
    MyamBuildState buildstate;
    double reltuples;
    int ntuples;

    /* block 0 is already the meta page written by myam_init_meta_page */
    meta->root_blkno = InvalidBlockNumber;
    meta->tree_height = 0;
    meta->total_tuples = 0;

    buildstate.meta = meta;
    buildstate.items = NULL;
    buildstate.capacity = 0;
    buildstate.indtuples = 0;

    oldcxt = MemoryContextSwitchTo(meta->index_cxt);
    reltuples = table_index_build_scan(heap, index, indexInfo, true, true,
                                       myam_build_callback, &buildstate, NULL);
    MemoryContextSwitchTo(oldcxt);

    ntuples = buildstate.indtuples;
    build_meta = meta;

    if (ntuples > 0)
    {
        typedef struct PageKeyItem
        {
            BlockNumber blkno;
            MyamKey     key;
        } PageKeyItem;

        PageKeyItem *leaf_pages = NULL;
        int32 leaf_cap = 0;
        int32 leaf_count = 0;

        Buffer      current_buf = InvalidBuffer;
        BlockNumber current_blkno = InvalidBlockNumber;
        bool        new_page_started = true;

        qsort(buildstate.items, ntuples, sizeof(SortItem), myam_sort_item_compare);

        for (int i = 0; i < ntuples; i++)
        {
            MyamIndexTuple tup = myam_form_tuple(meta, &buildstate.items[i].key,
                                                 &buildstate.items[i].tid);

            if (current_buf == InvalidBuffer ||
                PageGetFreeSpace(BufferGetPage(current_buf)) < tup->t_info + sizeof(ItemIdData))
            {
                Buffer      new_buf   = myam_page_new(index, meta, MYAM_LEAF_PAGE);
                BlockNumber new_blkno = BufferGetBlockNumber(new_buf);

                if (current_buf != InvalidBuffer)
                {
                    /* Stitch sibling links (both buffers are held exclusive) */
                    MyamPageGetOpaque(BufferGetPage(current_buf))->next = new_blkno;
                    MyamPageGetOpaque(BufferGetPage(new_buf))->prev     = current_blkno;
                    START_CRIT_SECTION();
                    MarkBufferDirty(current_buf);
                    if (RelationNeedsWAL(index))
                        log_newpage_buffer(current_buf, true);
                    END_CRIT_SECTION();
                    UnlockReleaseBuffer(current_buf);
                }

                current_buf   = new_buf;
                current_blkno = new_blkno;
                new_page_started = true;
            }

            if (new_page_started)
            {
                if (leaf_count == leaf_cap)
                {
                    int32 newcap = (leaf_cap == 0) ? 128 : leaf_cap * 2;
                    leaf_pages = leaf_pages
                        ? (PageKeyItem *) repalloc(leaf_pages, sizeof(PageKeyItem) * newcap)
                        : (PageKeyItem *) palloc(sizeof(PageKeyItem) * newcap);
                    leaf_cap = newcap;
                }

                leaf_pages[leaf_count].blkno = current_blkno;
                leaf_pages[leaf_count].key   = buildstate.items[i].key;
                leaf_count++;
                new_page_started = false;
            }

            if (!myam_page_insert_tuple(meta, BufferGetPage(current_buf), tup,
                                        InvalidOffsetNumber, NULL))
                elog(ERROR, "MyAM: page full during bulk load");

            pfree(tup);
        }

        if (current_buf != InvalidBuffer)
        {
            START_CRIT_SECTION();
            MarkBufferDirty(current_buf);
            if (RelationNeedsWAL(index))
                log_newpage_buffer(current_buf, true);
            END_CRIT_SECTION();
            UnlockReleaseBuffer(current_buf);
        }

        /* ── Build internal levels bottom-up ── */
        PageKeyItem *level_pages = leaf_pages;
        int32        level_count = leaf_count;
        int32        level_no    = 0;
        bool         need_free   = false;  /* true once we palloc'd next_level */

        while (level_count > 1)
        {
            int32       next_count = (level_count + MYAM_INTERNAL_FANOUT - 1) / MYAM_INTERNAL_FANOUT;
            PageKeyItem *next_level = (PageKeyItem *) palloc(sizeof(PageKeyItem) * next_count);
            int32        idx = 0;

            for (int32 gi = 0; gi < next_count; gi++)
            {
                int32 group_start = idx;
                int32 group_end   = Min(level_count, idx + MYAM_INTERNAL_FANOUT);

                Buffer      ibuf    = myam_page_new(index, meta, MYAM_INTERNAL_PAGE);
                BlockNumber iblkno  = BufferGetBlockNumber(ibuf);
                Page        ipage   = BufferGetPage(ibuf);
                MyamPageGetOpaque(ipage)->level = level_no + 1;

                for (int32 j = group_start; j < group_end; j++)
                {
                    MyamKey *ckey = &level_pages[j].key;
                    MyamInternalTuple itup = myam_form_internal_tuple(meta, ckey,
                                                                       level_pages[j].blkno);

                    if (!myam_page_insert_item(meta, ipage, (Item) itup, itup->t_info,
                                               InvalidOffsetNumber, NULL))
                        elog(ERROR, "MyAM: internal page full during bulk build");

                    pfree(itup);
                }

                START_CRIT_SECTION();
                MarkBufferDirty(ibuf);
                if (RelationNeedsWAL(index))
                    log_newpage_buffer(ibuf, true);
                END_CRIT_SECTION();
                UnlockReleaseBuffer(ibuf);

                next_level[gi].blkno = iblkno;
                next_level[gi].key   = level_pages[group_start].key;
                idx = group_end;
            }

            if (need_free) pfree(level_pages);
            level_pages = next_level;
            level_count = next_count;
            level_no++;
            need_free = true;
        }

        /* Mark root page */
        {
            Buffer rbuf = ReadBuffer(index, level_pages[0].blkno);
            LockBuffer(rbuf, BUFFER_LOCK_EXCLUSIVE);
            MyamPageGetOpaque(BufferGetPage(rbuf))->flags |= MYAM_ROOT_PAGE;
            START_CRIT_SECTION();
            MarkBufferDirty(rbuf);
            if (RelationNeedsWAL(index))
                log_newpage_buffer(rbuf, true);
            END_CRIT_SECTION();
            UnlockReleaseBuffer(rbuf);
        }

        meta->root_blkno  = level_pages[0].blkno;
        meta->tree_height = level_no + 1;
        meta->total_tuples = ntuples;

        if (need_free) pfree(level_pages);
        else           pfree(leaf_pages);
    }
    else
    {
        /* Empty index: single leaf page is the root */
        Buffer root_buf = myam_page_new(index, meta, MYAM_LEAF_PAGE | MYAM_ROOT_PAGE);
        meta->root_blkno  = BufferGetBlockNumber(root_buf);
        meta->tree_height = 1;
        START_CRIT_SECTION();
        MarkBufferDirty(root_buf);
        if (RelationNeedsWAL(index))
            log_newpage_buffer(root_buf, true);
        END_CRIT_SECTION();
        UnlockReleaseBuffer(root_buf);
        meta->total_tuples = 0;
    }

    /* Persist structural state to block 0 */
    myam_write_meta_page(index, meta);

    result->heap_tuples  = reltuples;
    result->index_tuples = (double) ntuples;

    if (buildstate.items)
        pfree(buildstate.items);
}

/* ==================== Search & Insert ==================== */

/*
 * Descent path entry: records which block and which child offset was
 * followed at each level so we can propagate splits bottom-up.
 */
#define MYAM_MAX_TREE_HEIGHT 32

typedef struct MyamPathItem
{
    BlockNumber  blkno;     /* block number of this level's page */
    OffsetNumber off;       /* slot followed to reach the child */
} MyamPathItem;

/* Forward declarations for helpers used before they are defined */
static void myam_deform_tupledata(MyamMeta *meta, const char *tuple_data, MyamKey *key);
static bool myam_tuple_get_col(MyamMeta *meta, MyamIndexTuple tuple, int col_idx,
                                Datum *datum, bool *is_null);
static bool myam_tuple_matches_scan_keys(IndexScanDesc scan, MyamIndexTuple tuple);

/* ── helper: key from first item on page (page already locked by caller) ─── */
static void
myam_page_get_first_key(MyamMeta *meta, MyamPage page, MyamKey *key)
{
    MyamPageOpaque opaque  = MyamPageGetOpaque(page);
    ItemId         itemid  = PageGetItemId(page, FirstOffsetNumber);

    if (opaque->flags & MYAM_LEAF_PAGE)
    {
        MyamIndexTuple tuple = (MyamIndexTuple) PageGetItem(page, itemid);
        myam_deform_tuple(meta, tuple, key);
    }
    else
    {
        MyamInternalTuple tuple = (MyamInternalTuple) PageGetItem(page, itemid);
        myam_deform_tupledata(meta, tuple->data, key);
    }
}

/* ── helper: find insert position in internal page ──────────────────────── */
static OffsetNumber
myam_internal_find_insert_pos(MyamMeta *meta, MyamPage page, MyamKey *key)
{
    OffsetNumber max_off = PageGetMaxOffsetNumber(page);
    OffsetNumber lo = FirstOffsetNumber, hi = max_off, pos = FirstOffsetNumber;

    while (lo <= hi)
    {
        OffsetNumber mid = lo + ((hi - lo) >> 1);
        ItemId iid = PageGetItemId(page, mid);
        MyamInternalTuple t = (MyamInternalTuple) PageGetItem(page, iid);
        int cmp = myam_compare_internal_tuple_key(meta, t, key);
        if (cmp < 0) { pos = mid + 1; lo = mid + 1; }
        else         { pos = mid;     hi = mid - 1;  }
    }
    return pos;
}

/* ── helper: find insert position in leaf page ───────────────────────────── */
static OffsetNumber
myam_page_find_insert_pos(MyamMeta *meta, MyamPage page, MyamKey *key)
{
    OffsetNumber max_off = PageGetMaxOffsetNumber(page);
    OffsetNumber lo = FirstOffsetNumber, hi = max_off, pos = FirstOffsetNumber;

    while (lo <= hi)
    {
        OffsetNumber mid = lo + ((hi - lo) >> 1);
        ItemId iid = PageGetItemId(page, mid);
        MyamIndexTuple t = (MyamIndexTuple) PageGetItem(page, iid);
        int cmp = myam_compare_tuple_key(meta, t, key);
        if (cmp < 0) { pos = mid + 1; lo = mid + 1; }
        else         { pos = mid;     hi = mid - 1;  }
    }
    return pos;
}

/* ── helper: deform tuple data (internal tuples) ────────────────────────── */
static void
myam_deform_tupledata(MyamMeta *meta, const char *tuple_data, MyamKey *key)
{
    const char *ptr = tuple_data;
    int         null_bitmap_len = (meta->nkeys + 7) / 8;
    int         i;

    key->nkeys = meta->nkeys;
    for (i = 0; i < meta->nkeys; i++)
        key->isnull[i] = (ptr[i/8] & (1 << (i%8))) != 0;
    ptr += null_bitmap_len;

    for (i = 0; i < meta->nkeys; i++)
    {
        if (!key->isnull[i])
        {
            if (meta->key_typlen[i] == -1)
            {
                struct varlena *v = (struct varlena *) ptr;
                key->keys[i] = PointerGetDatum(v);
                ptr += VARSIZE_ANY(v);
            }
            else
            {
                Size len = meta->key_typlen[i];
                if (meta->key_byval[i]) { Datum d; memcpy(&d, ptr, len); key->keys[i] = d; }
                else                     key->keys[i] = PointerGetDatum(ptr);
                ptr += len;
            }
        }
        else
            key->keys[i] = (Datum) 0;
    }
}

/* ────────────────────────────────────────────────────────────────────────────
 * myam_btree_search
 *
 * Traverse B+tree from root to leaf using buffer-manager page reads.
 * Caller must hold meta->lock LW_SHARED (or LW_EXCLUSIVE).
 * ────────────────────────────────────────────────────────────────────────────
 */
bool
myam_btree_search(Relation index, MyamMeta *meta, MyamKey *key,
                  ItemPointer result_tid)
{
    BlockNumber blkno;
    Buffer      buf;
    Page        page;

    /* 1. PDC fast path */
    if (myam_cache_search(meta, key, result_tid))
        return true;

    if (meta->root_blkno == InvalidBlockNumber)
        return false;

    blkno = meta->root_blkno;

    for (;;)
    {
        MyamPageOpaque opaque;

        buf   = ReadBuffer(index, blkno);
        LockBuffer(buf, BUFFER_LOCK_SHARE);
        page  = BufferGetPage(buf);
        opaque = MyamPageGetOpaque(page);

        if (opaque->flags & MYAM_LEAF_PAGE)
        {
            /* Binary search on leaf */
            OffsetNumber lo = FirstOffsetNumber;
            OffsetNumber hi = PageGetMaxOffsetNumber(page);

            while (lo <= hi)
            {
                OffsetNumber  mid   = lo + ((hi - lo) >> 1);
                ItemId        iid   = PageGetItemId(page, mid);
                MyamIndexTuple tup  = (MyamIndexTuple) PageGetItem(page, iid);
                int           cmp   = myam_compare_tuple_key(meta, tup, key);

                if (cmp == 0)
                {
                    *result_tid = tup->t_tid;
                    myam_cache_insert(meta, key, blkno, mid, &tup->t_tid, true);
                    UnlockReleaseBuffer(buf);
                    return true;
                }
                else if (cmp < 0) lo = mid + 1;
                else              hi = mid - 1;
            }

            UnlockReleaseBuffer(buf);
            return false;
        }
        else
        {
            /* Internal page: find child, release current, descend */
            OffsetNumber off = myam_internal_find_child_off(meta, page, key);
            if (!OffsetNumberIsValid(off))
            {
                UnlockReleaseBuffer(buf);
                return false;
            }
            blkno = myam_internal_get_child_blkno(page, off);
            UnlockReleaseBuffer(buf);
        }
    }
}

bool
myam_btree_check_unique(Relation index, MyamMeta *meta, MyamKey *key)
{
    ItemPointerData tid;
    return !myam_btree_search(index, meta, key, &tid);
}

/* ────────────────────────────────────────────────────────────────────────────
 * Leaf split: copy upper half to a new page, update prev/next links.
 * Returns the new page's buffer (locked exclusive + dirty).
 * The caller's leaf buffer is also dirtied.
 * ────────────────────────────────────────────────────────────────────────────
 */
static Buffer
myam_page_split_leaf(Relation index, MyamMeta *meta,
                     BlockNumber leaf_blkno, Buffer leaf_buf)
{
    Page            leaf_page  = BufferGetPage(leaf_buf);
    MyamPageOpaque  leaf_op    = MyamPageGetOpaque(leaf_page);
    uint16          flags      = leaf_op->flags & ~MYAM_ROOT_PAGE;

    /* Allocate new right sibling */
    Buffer          new_buf    = myam_page_new(index, meta, flags);
    BlockNumber     new_blkno  = BufferGetBlockNumber(new_buf);
    Page            new_page   = BufferGetPage(new_buf);
    MyamPageOpaque  new_op     = MyamPageGetOpaque(new_page);

    OffsetNumber    max_off    = PageGetMaxOffsetNumber(leaf_page);
    OffsetNumber    split_off  = max_off / 2;
    int             nitems;
    OffsetNumber   *itemnos;
    int             i;

    /* Copy upper half to new page */
    for (OffsetNumber off = split_off + 1; off <= max_off; off++)
    {
        ItemId iid  = PageGetItemId(leaf_page, off);
        Item   item = PageGetItem(leaf_page, iid);
        Size   len  = ItemIdGetLength(iid);
        if (PageAddItem(new_page, item, len, InvalidOffsetNumber, false, false)
                == InvalidOffsetNumber)
            elog(ERROR, "MyAM: leaf split new page full");
    }

    /* Remove upper half from left page */
    nitems  = (int)(max_off - split_off);
    itemnos = (OffsetNumber *) palloc(sizeof(OffsetNumber) * nitems);
    for (i = 0; i < nitems; i++)
        itemnos[i] = split_off + 1 + i;
    PageIndexMultiDelete(leaf_page, itemnos, nitems);
    pfree(itemnos);

    leaf_op->max_off = PageGetMaxOffsetNumber(leaf_page);
    new_op->max_off  = PageGetMaxOffsetNumber(new_page);

    /* Fix sibling links: left <-> new <-> old-right */
    BlockNumber old_next = leaf_op->next;
    new_op->prev  = leaf_blkno;
    new_op->next  = old_next;
    leaf_op->next = new_blkno;

    /* ── Atomically WAL-log all modified pages ── */
    {
        GenericXLogState *xlog = GenericXLogStart(index);
        GenericXLogRegisterBuffer(xlog, leaf_buf, GENERIC_XLOG_FULL_IMAGE);
        GenericXLogRegisterBuffer(xlog, new_buf,  GENERIC_XLOG_FULL_IMAGE);

        if (old_next != InvalidBlockNumber)
        {
            Buffer nb = ReadBuffer(index, old_next);
            LockBuffer(nb, BUFFER_LOCK_EXCLUSIVE);
            MyamPageGetOpaque(BufferGetPage(nb))->prev = new_blkno;
            GenericXLogRegisterBuffer(xlog, nb, GENERIC_XLOG_FULL_IMAGE);
            GenericXLogFinish(xlog);
            UnlockReleaseBuffer(nb);
        }
        else
            GenericXLogFinish(xlog);
    }

    return new_buf;
}

/* ────────────────────────────────────────────────────────────────────────────
 * Internal page split.  Similar to leaf split but no sibling-link update.
 * Returns new right sibling's buffer (locked exclusive + dirty).
 * ────────────────────────────────────────────────────────────────────────────
 */
static Buffer
myam_page_split_internal(Relation index, MyamMeta *meta, Buffer left_buf)
{
    Page            left_page = BufferGetPage(left_buf);
    MyamPageOpaque  left_op   = MyamPageGetOpaque(left_page);
    uint16          flags     = left_op->flags & ~MYAM_ROOT_PAGE;

    Buffer          new_buf   = myam_page_new(index, meta, flags);
    Page            new_page  = BufferGetPage(new_buf);
    MyamPageOpaque  new_op    = MyamPageGetOpaque(new_page);
    OffsetNumber    max_off   = PageGetMaxOffsetNumber(left_page);
    OffsetNumber    split_off = max_off / 2;
    int             nitems;
    OffsetNumber   *itemnos;
    int             i;

    new_op->level = left_op->level;

    for (OffsetNumber off = split_off + 1; off <= max_off; off++)
    {
        ItemId iid  = PageGetItemId(left_page, off);
        Item   item = PageGetItem(left_page, iid);
        Size   len  = ItemIdGetLength(iid);
        if (PageAddItem(new_page, item, len, InvalidOffsetNumber, false, false)
                == InvalidOffsetNumber)
            elog(ERROR, "MyAM: internal split new page full");
    }

    nitems  = (int)(max_off - split_off);
    itemnos = (OffsetNumber *) palloc(sizeof(OffsetNumber) * nitems);
    for (i = 0; i < nitems; i++)
        itemnos[i] = split_off + 1 + i;
    PageIndexMultiDelete(left_page, itemnos, nitems);
    pfree(itemnos);

    left_op->max_off = PageGetMaxOffsetNumber(left_page);
    new_op->max_off  = PageGetMaxOffsetNumber(new_page);

    {
        GenericXLogState *xlog = GenericXLogStart(index);
        GenericXLogRegisterBuffer(xlog, left_buf, GENERIC_XLOG_FULL_IMAGE);
        GenericXLogRegisterBuffer(xlog, new_buf,  GENERIC_XLOG_FULL_IMAGE);
        GenericXLogFinish(xlog);
    }

    return new_buf;
}

/* ────────────────────────────────────────────────────────────────────────────
 * myam_insert_into_parent
 *
 * After a page split, insert separator key + right-child block number into
 * the parent.  Uses the recorded descent path to navigate back up.
 * path[0] = root level, path[path_depth-1] = parent of the split page.
 * ────────────────────────────────────────────────────────────────────────────
 */
static void
myam_insert_into_parent(Relation index, MyamMeta *meta,
                        BlockNumber left_blkno, BlockNumber right_blkno,
                        MyamKey *right_key,
                        MyamPathItem *path, int path_depth)
{
    if (path_depth == 0)
    {
        /*
         * The split page was the root.  Create a new root that points to
         * both halves.
         */
        Buffer          root_buf;
        Page            root_page;
        MyamPageOpaque  root_op;
        BlockNumber     new_root_blkno;
        MyamKey         left_key;
        Buffer          left_buf;
        MyamInternalTuple ltup, rtup;

        root_buf        = myam_page_new(index, meta,
                                        MYAM_INTERNAL_PAGE | MYAM_ROOT_PAGE);
        new_root_blkno  = BufferGetBlockNumber(root_buf);
        root_page       = BufferGetPage(root_buf);
        root_op         = MyamPageGetOpaque(root_page);

        /* Determine left child's level to set root level */
        left_buf = ReadBuffer(index, left_blkno);
        LockBuffer(left_buf, BUFFER_LOCK_SHARE);
        root_op->level = MyamPageGetOpaque(BufferGetPage(left_buf))->level + 1;
        myam_page_get_first_key(meta, BufferGetPage(left_buf), &left_key);
        UnlockReleaseBuffer(left_buf);

        ltup = myam_form_internal_tuple(meta, &left_key, left_blkno);
        rtup = myam_form_internal_tuple(meta, right_key, right_blkno);

        if (!myam_page_insert_item(meta, root_page, (Item) ltup, ltup->t_info,
                                   InvalidOffsetNumber, NULL))
            elog(ERROR, "MyAM: new-root left insert failed");
        if (!myam_page_insert_item(meta, root_page, (Item) rtup, rtup->t_info,
                                   InvalidOffsetNumber, NULL))
            elog(ERROR, "MyAM: new-root right insert failed");

        pfree(ltup);
        pfree(rtup);

        /* Read meta buf and rewrite it in-memory */
        Buffer meta_buf = ReadBuffer(index, MYAM_META_BLKNO);
        LockBuffer(meta_buf, BUFFER_LOCK_EXCLUSIVE);
        {
            Page mp = BufferGetPage(meta_buf);
            MyamDiskMetaData dm;
            dm.magic        = MYAM_PAGE_MAGIC;
            dm.version      = MYAM_DISK_META_VERSION;
            dm.root_blkno   = new_root_blkno;
            dm.tree_height  = meta->tree_height + 1;
            dm.total_pages  = meta->total_pages;
            dm.total_tuples = meta->total_tuples;
            PageInit(mp, BLCKSZ, sizeof(MyamPageOpaqueData));
            MyamPageGetOpaque(mp)->flags = MYAM_META_PAGE;
            if (PageAddItem(mp, (Item) &dm, sizeof(dm),
                            FirstOffsetNumber, false, false) == InvalidOffsetNumber)
                elog(ERROR, "MyAM: meta page rewrite failed");
        }
        /* One WAL record covers both root_buf and meta_buf */
        {
            GenericXLogState *xlog = GenericXLogStart(index);
            GenericXLogRegisterBuffer(xlog, root_buf,  GENERIC_XLOG_FULL_IMAGE);
            GenericXLogRegisterBuffer(xlog, meta_buf,  GENERIC_XLOG_FULL_IMAGE);
            GenericXLogFinish(xlog);
        }
        UnlockReleaseBuffer(meta_buf);
        UnlockReleaseBuffer(root_buf);
        meta->root_blkno  = new_root_blkno;
        meta->tree_height++;
        return;   /* skip the myam_write_meta_page call below */
    }

    /* Normal case: insert into parent page */
    BlockNumber      parent_blkno = path[path_depth - 1].blkno;
    Buffer           parent_buf   = ReadBuffer(index, parent_blkno);
    Page             parent_page;
    MyamInternalTuple itup;
    OffsetNumber     pos;

    LockBuffer(parent_buf, BUFFER_LOCK_EXCLUSIVE);
    parent_page = BufferGetPage(parent_buf);

    itup = myam_form_internal_tuple(meta, right_key, right_blkno);
    pos  = myam_internal_find_insert_pos(meta, parent_page, right_key);

    if (myam_page_insert_item(meta, parent_page, (Item) itup,
                              itup->t_info, pos, NULL))
    {
        pfree(itup);
        {
            GenericXLogState *xlog = GenericXLogStart(index);
            GenericXLogRegisterBuffer(xlog, parent_buf, GENERIC_XLOG_FULL_IMAGE);
            GenericXLogFinish(xlog);
        }
        UnlockReleaseBuffer(parent_buf);
        return;
    }

    /* Parent is also full: split it */
    Buffer      new_parent_buf   = myam_page_split_internal(index, meta, parent_buf);
    BlockNumber new_parent_blkno = BufferGetBlockNumber(new_parent_buf);
    Page        new_parent_page  = BufferGetPage(new_parent_buf);
    MyamKey     sep_key;
    Page        dest_page;
    Buffer      dest_buf;
    BlockNumber dest_blkno;

    myam_page_get_first_key(meta, new_parent_page, &sep_key);

    if (myam_key_compare(meta, &sep_key, right_key) <= 0)
    {
        dest_buf   = new_parent_buf;
        dest_page  = new_parent_page;
        dest_blkno = new_parent_blkno;
    }
    else
    {
        dest_buf   = parent_buf;
        dest_page  = parent_page;
        dest_blkno = parent_blkno;
    }

    pos = myam_internal_find_insert_pos(meta, dest_page, right_key);
    if (!myam_page_insert_item(meta, dest_page, (Item) itup,
                               itup->t_info, pos, NULL))
        elog(ERROR, "MyAM: parent insert failed after split");

    pfree(itup);
    {
        GenericXLogState *xlog = GenericXLogStart(index);
        GenericXLogRegisterBuffer(xlog, parent_buf,     GENERIC_XLOG_FULL_IMAGE);
        GenericXLogRegisterBuffer(xlog, new_parent_buf, GENERIC_XLOG_FULL_IMAGE);
        GenericXLogFinish(xlog);
    }
    UnlockReleaseBuffer(parent_buf);
    UnlockReleaseBuffer(new_parent_buf);

    /* Propagate split upward */
    myam_insert_into_parent(index, meta,
                            parent_blkno, new_parent_blkno,
                            &sep_key,
                            path, path_depth - 1);
}

/* ────────────────────────────────────────────────────────────────────────────
 * myam_btree_insert
 *
 * Descend to the correct leaf, insert the tuple, split if necessary.
 * Caller must hold meta->lock LW_EXCLUSIVE.
 * ────────────────────────────────────────────────────────────────────────────
 */
void
myam_btree_insert(Relation index, MyamMeta *meta, MyamKey *key,
                  ItemPointer tid, BlockNumber *out_blkno, OffsetNumber *out_off)
{
    BlockNumber     blkno;
    Buffer          buf;
    Page            page;
    MyamPathItem    path[MYAM_MAX_TREE_HEIGHT];
    int             path_depth = 0;
    MyamIndexTuple  tuple;
    OffsetNumber    pos;
    OffsetNumber    inserted_off = InvalidOffsetNumber;

    /* Initialise root if index is empty */
    if (meta->root_blkno == InvalidBlockNumber)
    {
        Buffer root_buf = myam_page_new(index, meta,
                                        MYAM_LEAF_PAGE | MYAM_ROOT_PAGE);
        meta->root_blkno  = BufferGetBlockNumber(root_buf);
        meta->tree_height = 1;
        {
            GenericXLogState *xlog = GenericXLogStart(index);
            GenericXLogRegisterBuffer(xlog, root_buf, GENERIC_XLOG_FULL_IMAGE);
            GenericXLogFinish(xlog);
        }
        UnlockReleaseBuffer(root_buf);
        myam_write_meta_page(index, meta);
    }

    /* ── Descent: from root to leaf, record path ── */
    blkno = meta->root_blkno;
    for (;;)
    {
        MyamPageOpaque opaque;

        buf   = ReadBuffer(index, blkno);
        LockBuffer(buf, BUFFER_LOCK_SHARE);
        page  = BufferGetPage(buf);
        opaque = MyamPageGetOpaque(page);

        if (opaque->flags & MYAM_LEAF_PAGE)
        {
            UnlockReleaseBuffer(buf);
            break;  /* blkno now = target leaf */
        }

        /* Record this level in the descent path */
        OffsetNumber off = myam_internal_find_child_off(meta, page, key);
        if (!OffsetNumberIsValid(off))
            elog(ERROR, "MyAM: corrupt internal page blkno=%u", blkno);

        path[path_depth].blkno = blkno;
        path[path_depth].off   = off;
        path_depth++;

        blkno = myam_internal_get_child_blkno(page, off);
        UnlockReleaseBuffer(buf);
    }

    /* ── Lock leaf exclusively and insert ── */
    buf  = ReadBuffer(index, blkno);
    LockBuffer(buf, BUFFER_LOCK_EXCLUSIVE);
    page = BufferGetPage(buf);

    tuple = myam_form_tuple(meta, key, tid);
    pos   = myam_page_find_insert_pos(meta, page, key);

    if (myam_page_insert_tuple(meta, page, tuple, pos, &inserted_off))
    {
        {
            GenericXLogState *xlog = GenericXLogStart(index);
            GenericXLogRegisterBuffer(xlog, buf, GENERIC_XLOG_FULL_IMAGE);
            GenericXLogFinish(xlog);
        }
        UnlockReleaseBuffer(buf);
    }
    else
    {
        /* ── Leaf is full: split ── */
        BlockNumber     leaf_blkno_orig = blkno;   /* original left half — save before blkno may change */
        Buffer          new_buf   = myam_page_split_leaf(index, meta, blkno, buf);
        BlockNumber     new_blkno = BufferGetBlockNumber(new_buf);
        Page            new_page  = BufferGetPage(new_buf);
        MyamKey         sep_key;

        ItemId         iid0 = PageGetItemId(new_page, FirstOffsetNumber);
        MyamIndexTuple t0   = (MyamIndexTuple) PageGetItem(new_page, iid0);
        myam_deform_tuple(meta, t0, &sep_key);

        /* Insert into whichever half the key belongs to */
        if (myam_compare_tuple_key(meta, t0, key) <= 0)
        {
            UnlockReleaseBuffer(buf);
            buf   = new_buf;
            page  = new_page;
            blkno = new_blkno;
        }
        else
        {
            UnlockReleaseBuffer(new_buf);
        }

        pos = myam_page_find_insert_pos(meta, page, key);
        if (!myam_page_insert_tuple(meta, page, tuple, pos, &inserted_off))
            elog(ERROR, "MyAM: leaf full after split");

        {
            GenericXLogState *xlog = GenericXLogStart(index);
            GenericXLogRegisterBuffer(xlog, buf, GENERIC_XLOG_FULL_IMAGE);
            GenericXLogFinish(xlog);
        }
        UnlockReleaseBuffer(buf);

        /* Propagate split upward */
        myam_insert_into_parent(index, meta,
                                leaf_blkno_orig,
                                new_blkno,
                                &sep_key,
                                path, path_depth);
    }

    pfree(tuple);
    meta->total_tuples++;

    if (out_blkno) *out_blkno = blkno;
    if (out_off)   *out_off   = inserted_off;
}

void
myam_btree_delete_tid(Relation index, MyamMeta *meta, ItemPointer tid)
{
    /* Deferred: not implemented in Phase 2 */
    (void) index; (void) meta; (void) tid;
}


/* ==================== 缓存管理 ==================== */

/*
 * 初始化缓存
 */
void
myam_cache_init(MyamMeta *meta)
{
    myam_pdc_init(meta);
}

/*
 * 缓存搜索
 */
bool
myam_cache_search(MyamMeta *meta, MyamKey *key, ItemPointer result_tid)
{
    /* 先查直连页缓存（PDC） */
    if (myam_pdc_lookup(meta, key, result_tid))
    {
        /* 统计命中 */
        myam_stats_update(meta, key, true);
        return true;
    }
    
    /* 统计未命中 */
    myam_stats_update(meta, key, false);
    return false;
}

/*
 * 缓存插入
 */
void
myam_cache_insert(MyamMeta *meta, MyamKey *key, BlockNumber blkno,
                  OffsetNumber off, ItemPointer tid, bool is_hot)
{
    /* Load factor 70% - balance between performance and memory usage */
    if (meta->cache_size >= meta->cache_capacity * 7 / 10)
        myam_cache_evict_low_value(meta);
    if (myam_predictor_should_cache(meta, key))
        myam_pdc_insert(meta, key, blkno, off, tid, is_hot);
}

/*
 * 缓存移除
 */
void
myam_cache_remove(MyamMeta *meta, MyamKey *key)
{
    myam_pdc_remove(meta, key);
}

/*
 * 按 TID 清理缓存项（用于 VACUUM 同步删除）
 */
void
myam_cache_remove_tid(MyamMeta *meta, ItemPointer tid)
{
    myam_pdc_remove_tid(meta, tid);
}

/*
 * 插入后同步：将新 TID 与查询统计及缓存对齐
 */
void
myam_cache_sync_on_insert(MyamMeta *meta, MyamKey *key, ItemPointer tid)
{
    /* 更新统计的 TID 映射 */
    myam_stats_update_tid(meta, key, tid);
}

/*
 * Clock 淘汰 (open-addressing 版本)：O(1) 摊销。
 *
 * 从 clock_hand 位置开始顺序扫描 PDC_VALID 条目：
 *   - access_count > 0: 递减（给"第二次机会"），继续；
 *   - access_count == 0: 设为 PDC_DELETED（tombstone），立即返回。
 *
 * 最坏情况 O(capacity)，负载因子 ≤ 0.75 时平均 ~1-2 步。
 */
void
myam_cache_evict_low_value(MyamMeta *meta)
{
    PageDirectCache *pdc = meta->pdc;

    if (!pdc || pdc->capacity == 0)
        return;

    uint32 cap     = pdc->capacity;
    uint32 scanned = 0;

    while (scanned < cap * 2)
    {
        uint32     idx = pdc->clock_hand & pdc->mask;
        pdc->clock_hand = (pdc->clock_hand + 1) & pdc->mask;
        scanned++;

        PDCEntry *e = &pdc->entries[idx];

        if (e->state != PDC_VALID)
            continue;

        if (e->access_count > 0)
        {
            e->access_count--;
            continue;
        }

        /* Evict: mark as tombstone so probe chains remain intact */
        e->state = PDC_DELETED;
        meta->cache_size = Max(0, meta->cache_size - 1);
        return;
    }
}

/* ==================== 查询统计 ==================== */

/*
 * 更新查询统计
 */
void
myam_stats_update(MyamMeta *meta, MyamKey *key, bool is_hot)
{
    /* 暂时禁用统计，以避免复杂key处理 */
}

/*
 * 为查询统计节点设置对应的 TID（若节点不存在则创建占位）
 */
void
myam_stats_update_tid(MyamMeta *meta, MyamKey *key, ItemPointer tid)
{
    /* Skipped */
}

QueryStatsNode *
myam_stats_get_node(MyamMeta *meta, MyamKey *key)
{
    /* Skipped */
    return NULL;
}

/*
 * 清理统计数据
 */
void
myam_stats_cleanup(MyamMeta *meta)
{
    /* Skipped */
}

/* ==================== 0-1背包优化 ==================== */

/*
 * 解决0-1背包问题
 */
double
myam_knapsack_solve(QueryStatsNode **candidates, int candidate_count, 
                   int capacity, bool **selected)
{
    /* Skipped for now */
    return 0.0;
}

/*
 * 应用0-1背包解决方案
 */
void
myam_knapsack_apply_solution(MyamMeta *meta, QueryStatsNode **candidates,
                            bool **selected, int candidate_count, int capacity)
{
    /* Skipped */
}

/*
 * 执行0-1背包优化
 */
void
myam_knapsack_optimize(MyamMeta *meta)
{
    /* Skipped */
}

/* ==================== 元数据管理 ==================== */

/*
 * 创建元数据
 */
MyamMeta *
myam_meta_create(Relation index)
{
    MemoryContext index_cxt = AllocSetContextCreate(TopMemoryContext,
                                                   "MyAM Index Context",
                                                   ALLOCSET_DEFAULT_SIZES);

    MyamMeta *meta = (MyamMeta *) MemoryContextAllocZero(index_cxt, sizeof(MyamMeta));
    meta->index_cxt = index_cxt;

    meta->index_oid = RelationGetRelid(index);
    meta->nkeys = index->rd_index->indnkeyatts;
    for (int i = 0; i < meta->nkeys; i++)
    {
        meta->key_type_oids[i] = TupleDescAttr(index->rd_att, i)->atttypid;
        meta->collation_oids[i] = TupleDescAttr(index->rd_att, i)->attcollation;

        get_typlenbyval(meta->key_type_oids[i], &meta->key_typlen[i], &meta->key_byval[i]);

        Oid cmp_proc = index_getprocid(index, i + 1, BTORDER_PROC);
        fmgr_info_cxt(cmp_proc, &meta->key_proc_infos[i], meta->index_cxt);

        switch (meta->key_type_oids[i])
        {
            case INT4OID:   meta->fast_cmp_type[i] = MYAM_CMP_INT4;   break;
            case INT8OID:   meta->fast_cmp_type[i] = MYAM_CMP_INT8;   break;
            case FLOAT4OID: meta->fast_cmp_type[i] = MYAM_CMP_FLOAT4; break;
            case FLOAT8OID: meta->fast_cmp_type[i] = MYAM_CMP_FLOAT8; break;
            default:        meta->fast_cmp_type[i] = MYAM_CMP_GENERIC; break;
        }
    }
    meta->is_unique = index->rd_index->indisunique;

    meta->root_blkno = InvalidBlockNumber;
    meta->tree_height = 0;
    meta->total_pages = 0;
    meta->total_tuples = 0;

    meta->cache_capacity = MYAM_DEFAULT_CACHE_CAPACITY;
    myam_cache_init(meta);

    meta->query_stats = NULL;
    meta->stats_list = NULL;
    meta->total_queries = 0;
    meta->hot_data_queries = 0;

    meta->knapsack_runs = 0;
    meta->optimization_ratio = 0.0;

    myam_background_optimize_init(meta);

    meta->avg_search_time = 0.0;
    meta->cache_hit_ratio = 0.0;

    /* 初始化自适应范围扫描 */
    myam_adaptive_init(meta);

    /* If the index already has pages on disk, read the meta page */
    if (RelationGetNumberOfBlocks(index) > 0)
    {
        PG_TRY();
        {
            myam_read_meta_page(index, meta);
        }
        PG_CATCH();
        {
            /* Non-fatal: meta page may be corrupt or from a different format */
            FlushErrorState();
            meta->root_blkno = InvalidBlockNumber;
        }
        PG_END_TRY();
    }

    return meta;
}

/*
 * 获取元数据
 */
MyamMeta *
myam_meta_get(Relation index)
{
    Oid index_oid = RelationGetRelid(index);
    MyamMetaCacheEntry *entry;
    bool found;
    
    /* 初始化全局元数据哈希（如果需要） */
    if (!myam_meta_hash)
    {
        HASHCTL hash_ctl;
        memset(&hash_ctl, 0, sizeof(hash_ctl));
        hash_ctl.keysize = sizeof(Oid);
        /* 修复：哈希项需包含键和值结构 */
        hash_ctl.entrysize = sizeof(MyamMetaCacheEntry);
        hash_ctl.hcxt = TopMemoryContext;
        
        myam_meta_hash = hash_create("MyAM Meta Hash",
                                     128,
                                     &hash_ctl,
                                     HASH_ELEM | HASH_BLOBS | HASH_CONTEXT);
    }
    
    entry = (MyamMetaCacheEntry *) hash_search(myam_meta_hash, &index_oid, HASH_FIND, &found);
    
    if (!found)
    {
        entry = (MyamMetaCacheEntry *) hash_search(myam_meta_hash, &index_oid, HASH_ENTER, &found);
        entry->key = index_oid;
        entry->meta = myam_meta_create(index);
    }
    
    return entry->meta;
}

/* ==================== 访问方法接口 ==================== */

/*
 * MyAM 处理器
 */
/*
 * Vacuum cleanup
 */
IndexBulkDeleteResult *
myam_vacuumcleanup(IndexVacuumInfo *info,
                   IndexBulkDeleteResult *stats)
{
    /* No-op for now, just return stats */
    if (stats == NULL)
        stats = (IndexBulkDeleteResult *) palloc0(sizeof(IndexBulkDeleteResult));
    return stats;
}

PG_FUNCTION_INFO_V1(myam_handler);
Datum
myam_handler(PG_FUNCTION_ARGS)
{
    IndexAmRoutine *amroutine = makeNode(IndexAmRoutine);
    
    amroutine->amstrategies = 5;
    amroutine->amsupport = 1;
    amroutine->amcanorder = true;
    amroutine->amcanorderbyop = false;
    amroutine->amcanbackward = false; /* Only forward for now */
    amroutine->amcanunique = true;
    amroutine->amcanmulticol = true;
    amroutine->amoptionalkey = true;
    amroutine->amsearcharray = false;
    amroutine->amsearchnulls = true; /* We handle nulls */
    amroutine->amstorage = false;
    amroutine->amclusterable = false;
    amroutine->ampredlocks = false;
    amroutine->amcanparallel = false;
    amroutine->amcaninclude = false;
    amroutine->amusemaintenanceworkmem = false;
    amroutine->amparallelvacuumoptions = 0;
    amroutine->amkeytype = InvalidOid;
    
    amroutine->ambuild = myam_build;
    amroutine->ambuildempty = myam_buildempty;
    amroutine->aminsert = myam_insert;
    amroutine->ambulkdelete = myam_bulkdelete;
    amroutine->amvacuumcleanup = myam_vacuumcleanup;
    amroutine->amcanreturn = NULL;
    amroutine->amcostestimate = myam_costestimate;
    amroutine->amoptions = NULL;
    amroutine->amproperty = NULL;
    amroutine->ambuildphasename = NULL;
    amroutine->amvalidate = NULL;
    amroutine->amadjustmembers = NULL;
    amroutine->ambeginscan = myam_beginscan;
    amroutine->amrescan = myam_rescan;
    amroutine->amgettuple = myam_gettuple;
    amroutine->amgetbitmap = myam_getbitmap;
    amroutine->amendscan = myam_endscan;
    amroutine->ammarkpos = NULL;
    amroutine->amrestrpos = NULL;
    amroutine->amestimateparallelscan = NULL;
    amroutine->aminitparallelscan = NULL;
    amroutine->amparallelrescan = NULL;
    
    PG_RETURN_POINTER(amroutine);
}

/*
 * Build empty init fork for UNLOGGED indexes.
 * Called by PostgreSQL when creating an UNLOGGED index; writes a minimal
 * meta page to INIT_FORKNUM so the index can be reset after WAL recovery.
 */
static void
myam_buildempty(Relation index)
{
    BulkWriteState *bulkstate;
    BulkWriteBuffer metabuf;
    Page             page;
    MyamDiskMetaData dm;

    bulkstate = smgr_bulk_start_rel(index, INIT_FORKNUM);
    metabuf   = smgr_bulk_get_buf(bulkstate);
    page      = (Page) metabuf;

    PageInit(page, BLCKSZ, sizeof(MyamPageOpaqueData));
    MyamPageGetOpaque(page)->flags = MYAM_META_PAGE;

    dm.magic        = MYAM_PAGE_MAGIC;
    dm.version      = MYAM_DISK_META_VERSION;
    dm.root_blkno   = InvalidBlockNumber;
    dm.tree_height  = 0;
    dm.total_pages  = 0;
    dm.total_tuples = 0;

    if (PageAddItem(page, (Item) &dm, sizeof(dm),
                    FirstOffsetNumber, false, false) == InvalidOffsetNumber)
        elog(ERROR, "MyAM: could not write empty meta page to init fork");

    smgr_bulk_write(bulkstate, MYAM_META_BLKNO, metabuf, true);
    smgr_bulk_finish(bulkstate);
}

/*
 * 构建索引
 */
IndexBuildResult *
myam_build(Relation heap, Relation index, struct IndexInfo *indexInfo)
{
    IndexBuildResult *result;
    MyamMeta *meta;

    /* Create a fresh meta entry for this index */
    meta = myam_meta_get(index);

    /* Ensure type info is up to date */
    meta->nkeys = indexInfo->ii_NumIndexKeyAttrs;
    meta->is_unique = indexInfo->ii_Unique;
    for (int i = 0; i < meta->nkeys; i++)
    {
        meta->key_type_oids[i] = TupleDescAttr(index->rd_att, i)->atttypid;
        meta->collation_oids[i] = TupleDescAttr(index->rd_att, i)->attcollation;
        get_typlenbyval(meta->key_type_oids[i], &meta->key_typlen[i], &meta->key_byval[i]);
        Oid cmp_proc = index_getprocid(index, i + 1, BTORDER_PROC);
        fmgr_info_cxt(cmp_proc, &meta->key_proc_infos[i], meta->index_cxt);
        switch (meta->key_type_oids[i])
        {
            case INT4OID:   meta->fast_cmp_type[i] = MYAM_CMP_INT4;   break;
            case INT8OID:   meta->fast_cmp_type[i] = MYAM_CMP_INT8;   break;
            case FLOAT4OID: meta->fast_cmp_type[i] = MYAM_CMP_FLOAT4; break;
            case FLOAT8OID: meta->fast_cmp_type[i] = MYAM_CMP_FLOAT8; break;
            default:        meta->fast_cmp_type[i] = MYAM_CMP_GENERIC; break;
        }
    }

    /* Write the meta page (block 0) first */
    myam_init_meta_page(index, meta);

    result = (IndexBuildResult *) palloc0(sizeof(IndexBuildResult));
    myam_bulk_build(meta, heap, index, indexInfo, result);
    return result;
}

/*
 * 插入索引项
 */
bool
myam_insert(Relation index, Datum *values, bool *isnull, ItemPointer heap_tid,
           Relation heapRel, IndexUniqueCheck checkUnique, bool indexUnchanged,
           struct IndexInfo *indexInfo)
{
    MyamMeta *meta = myam_meta_get(index);
    LWLock   *lock = myam_get_lwlock(RelationGetRelid(index));
    MyamKey key;
    BlockNumber ins_blkno = InvalidBlockNumber;
    OffsetNumber ins_off  = InvalidOffsetNumber;

    LWLockAcquire(lock, LW_EXCLUSIVE);
    myam_read_meta_page(index, meta);  /* always refresh on write path */

    key.nkeys = meta->nkeys;
    for (int i = 0; i < meta->nkeys; i++)
    {
        key.keys[i] = values[i];
        key.isnull[i] = isnull[i];
    }

    if (checkUnique == UNIQUE_CHECK_YES || checkUnique == UNIQUE_CHECK_PARTIAL)
    {
        if (meta->is_unique)
        {
            if (!myam_btree_check_unique(index, meta, &key))
            {
                LWLockRelease(lock);
                ereport(ERROR,
                        (errcode(ERRCODE_UNIQUE_VIOLATION),
                         errmsg("duplicate key value violates unique constraint \"%s\"",
                                RelationGetRelationName(index))));
            }
        }
    }

    myam_btree_insert(index, meta, &key, heap_tid, &ins_blkno, &ins_off);

    LWLockRelease(lock);
    return true;
}

/*
 * 开始扫描
 */
IndexScanDesc
myam_beginscan(Relation indexRel, int nkeys, int norderbys)
{
    IndexScanDesc scan;
    MyamScanOpaque so;

    scan = RelationGetIndexScan(indexRel, nkeys, norderbys);

    so = (MyamScanOpaque) palloc0(sizeof(MyamScanOpaqueData));
    so->meta         = myam_meta_get(indexRel);
    so->index        = indexRel;
    so->current_blkno = InvalidBlockNumber;
    so->current_buf  = InvalidBuffer;
    so->current_off  = InvalidOffsetNumber;
    so->started      = false;

    scan->opaque = so;
    return scan;
}

/*
 * 重新扫描
 */
void
myam_rescan(IndexScanDesc scan, ScanKey scankey, int nscankeys,
           ScanKey orderbys, int norderbys)
{
    MyamScanOpaque so = (MyamScanOpaque) scan->opaque;

    if (scankey && scan->numberOfKeys > 0)
    {
        memmove(scan->keyData, scankey,
                scan->numberOfKeys * sizeof(ScanKeyData));
    }

    if (BufferIsValid(so->current_buf))
    {
        UnlockReleaseBuffer(so->current_buf);
        so->current_buf = InvalidBuffer;
    }
    so->current_blkno = InvalidBlockNumber;
    so->current_off   = InvalidOffsetNumber;
    so->started       = false;
}

/*
 * Descend B+tree to find the leaf page for a given key.
 * Returns the BlockNumber of the leaf, or InvalidBlockNumber on error.
 */
static BlockNumber
myam_find_leaf_blkno(Relation index, MyamMeta *meta, MyamKey *key)
{
    BlockNumber blkno;

    if (meta->root_blkno == InvalidBlockNumber)
        return InvalidBlockNumber;

    blkno = meta->root_blkno;
    for (;;)
    {
        Buffer          buf;
        Page            page;
        MyamPageOpaque  opaque;

        buf    = ReadBuffer(index, blkno);
        LockBuffer(buf, BUFFER_LOCK_SHARE);
        page   = BufferGetPage(buf);
        opaque = MyamPageGetOpaque(page);

        if (opaque->flags & MYAM_LEAF_PAGE)
        {
            UnlockReleaseBuffer(buf);
            return blkno;
        }

        OffsetNumber off = myam_internal_find_child_off(meta, page, key);
        if (!OffsetNumberIsValid(off))
            off = FirstOffsetNumber;
        blkno = myam_internal_get_child_blkno(page, off);
        UnlockReleaseBuffer(buf);
    }
}

/*
 * Find the leftmost leaf block in the B+tree.
 */
static BlockNumber
myam_find_leftmost_leaf_blkno(Relation index, MyamMeta *meta)
{
    BlockNumber blkno;

    if (meta->root_blkno == InvalidBlockNumber)
        return InvalidBlockNumber;

    blkno = meta->root_blkno;
    for (;;)
    {
        Buffer          buf;
        Page            page;
        MyamPageOpaque  opaque;

        buf    = ReadBuffer(index, blkno);
        LockBuffer(buf, BUFFER_LOCK_SHARE);
        page   = BufferGetPage(buf);
        opaque = MyamPageGetOpaque(page);

        if (opaque->flags & MYAM_LEAF_PAGE)
        {
            UnlockReleaseBuffer(buf);
            return blkno;
        }

        if (PageGetMaxOffsetNumber(page) == 0)
        {
            UnlockReleaseBuffer(buf);
            return InvalidBlockNumber;
        }
        blkno = myam_internal_get_child_blkno(page, FirstOffsetNumber);
        UnlockReleaseBuffer(buf);
    }
}

/*
 * 获取下一个元组 — buffer-manager based scan
 */
bool
myam_gettuple(IndexScanDesc scan, ScanDirection dir)
{
    MyamScanOpaque  so    = (MyamScanOpaque) scan->opaque;
    MyamMeta       *meta  = so->meta;
    Relation        index = so->index;
    BlockNumber     blkno;
    OffsetNumber    off;

    if (dir != ForwardScanDirection)
        return false;

    if (!so->started)
    {
        LWLock *lock = myam_get_lwlock(RelationGetRelid(index));

        /* Only hold lock during metadata refresh */
        LWLockAcquire(lock, LW_SHARED);
        so->started = true;
        myam_meta_refresh(index, meta);
        LWLockRelease(lock);

        if (meta->root_blkno == InvalidBlockNumber)
            return false;

        /* ── Point query fast path (all columns = equality) ── */
        if (scan->numberOfKeys > 0)
        {
            bool     all_eq = true;
            MyamKey  key;
            ItemPointerData tid;

            key.nkeys = meta->nkeys;
            for (int i = 0; i < meta->nkeys; i++)
            {
                key.isnull[i] = true;
                key.keys[i]   = (Datum) 0;
            }

            for (int i = 0; i < scan->numberOfKeys; i++)
            {
                ScanKey skey    = &scan->keyData[i];
                int     col_idx = skey->sk_attno - 1;

                if (col_idx < 0 || col_idx >= meta->nkeys ||
                    (skey->sk_flags & SK_ISNULL) ||
                    skey->sk_strategy != BTEqualStrategyNumber)
                {
                    all_eq = false;
                    break;
                }
                key.isnull[col_idx] = false;
                key.keys[col_idx]   = skey->sk_argument;
            }

            if (all_eq)
            {
                for (int i = 0; i < meta->nkeys; i++)
                    if (key.isnull[i]) { all_eq = false; break; }
            }

            if (all_eq)
            {
                /* Point query - mark query type and use PDC cache */
                so->is_point_query = true;
                so->is_range_query = false;

                if (myam_btree_search(index, meta, &key, &tid))
                {
                    scan->xs_heaptid  = tid;
                    so->current_blkno = InvalidBlockNumber;
                    return true;
                }
                return false;
            }
            else
            {
                /* Range query - mark query type, skip PDC cache */
                so->is_point_query = false;
                so->is_range_query = true;
            }
        }

        /* ── INT4 Range Query Fast Path ── */
        {
            int32 lower_bound, upper_bound;

            /* 检查是否是简单的INT4范围查询 */
            if (myam_is_simple_int4_range(meta, scan, &lower_bound, &upper_bound))
            {
                /* 标记使用INT4快速路径 */
                so->range_scan_strategy = MYAM_RANGE_SCAN_PURE_BTREE;

                /* INT4快速路径：直接读取INT4值，避免tuple解析 */
                MyamKey seek_key;
                seek_key.nkeys = 1;
                seek_key.keys[0] = Int32GetDatum(lower_bound);
                seek_key.isnull[0] = false;

                blkno = myam_find_leaf_blkno(index, meta, &seek_key);
                if (blkno == InvalidBlockNumber)
                    return false;

                Buffer tbuf = ReadBuffer(index, blkno);
                LockBuffer(tbuf, BUFFER_LOCK_SHARE);
                off = myam_page_find_insert_pos(meta, BufferGetPage(tbuf), &seek_key);
                UnlockReleaseBuffer(tbuf);

                so->current_blkno = blkno;
                so->current_off = off;

                /* 使用INT4快速扫描循环 */
                goto int4_fast_scan;
            }
        }

        /* ── Range scan: seek to lower-bound leaf ── */
        {
            bool    can_seek = (scan->numberOfKeys > 0);
            MyamKey seek_key;

            seek_key.nkeys = meta->nkeys;
            for (int i = 0; i < meta->nkeys; i++)
            {
                seek_key.isnull[i] = true;
                seek_key.keys[i]   = (Datum) 0;
            }

            if (can_seek)
            {
                for (int i = 0; i < scan->numberOfKeys; i++)
                {
                    ScanKey skey    = &scan->keyData[i];
                    int     col_idx = skey->sk_attno - 1;

                    if (col_idx < 0 || col_idx >= meta->nkeys ||
                        (skey->sk_flags & SK_ISNULL))
                    {
                        can_seek = false;
                        break;
                    }

                    if ((skey->sk_strategy == BTEqualStrategyNumber ||
                         skey->sk_strategy == BTGreaterEqualStrategyNumber ||
                         skey->sk_strategy == BTGreaterStrategyNumber) &&
                        seek_key.isnull[col_idx])
                    {
                        seek_key.isnull[col_idx] = false;
                        seek_key.keys[col_idx]   = skey->sk_argument;
                    }
                }
            }

            if (can_seek)
            {
                blkno = myam_find_leaf_blkno(index, meta, &seek_key);
                if (blkno == InvalidBlockNumber)
                    return false;

                Buffer tbuf = ReadBuffer(index, blkno);
                LockBuffer(tbuf, BUFFER_LOCK_SHARE);
                off = myam_page_find_insert_pos(meta, BufferGetPage(tbuf), &seek_key);
                UnlockReleaseBuffer(tbuf);
            }
            else
            {
                blkno = myam_find_leftmost_leaf_blkno(index, meta);
                off   = FirstOffsetNumber;
            }

            so->current_blkno = blkno;
            so->current_off   = off;
        }
    }

    blkno = so->current_blkno;
    off   = so->current_off;

    if (blkno == InvalidBlockNumber)
        return false;

int4_fast_scan:
    /* ── INT4 Fast Scan Loop (for simple INT4 range queries) ── */
    if (so->range_scan_strategy == MYAM_RANGE_SCAN_PURE_BTREE &&
        meta->nkeys == 1 && meta->key_type_oids[0] == INT4OID)
    {
        int32 lower_bound, upper_bound;

        if (myam_is_simple_int4_range(meta, scan, &lower_bound, &upper_bound))
        {
            /* INT4快速路径：直接读取INT4值，无需完整的tuple解析 */
            for (;;)
            {
                Buffer          buf;
                Page            page;
                BlockNumber     next_blkno;
                OffsetNumber    max_off;

                buf        = ReadBuffer(index, blkno);
                LockBuffer(buf, BUFFER_LOCK_SHARE);
                page       = BufferGetPage(buf);
                max_off    = PageGetMaxOffsetNumber(page);
                next_blkno = MyamPageGetOpaque(page)->next;

                while (off <= max_off)
                {
                    ItemId         iid   = PageGetItemId(page, off);
                    MyamIndexTuple tuple = (MyamIndexTuple) PageGetItem(page, iid);

                    /* 直接读取INT4值（假设单列INT4索引的tuple格式） */
                    int null_bitmap_len = 1;  /* (1 + 7) / 8 = 1 */
                    const char *bitmap = tuple->data;
                    const char *data_ptr = tuple->data + null_bitmap_len;

                    /* 检查null */
                    if (bitmap[0] & 0x01)
                    {
                        /* NULL值，跳过 */
                        off++;
                        continue;
                    }

                    /* 直接读取INT4值 */
                    int32 value;
                    memcpy(&value, data_ptr, sizeof(int32));

                    /* 检查范围 */
                    if (value < lower_bound)
                    {
                        off++;
                        continue;
                    }

                    if (value >= upper_bound)
                    {
                        /* 超出范围，结束扫描 */
                        UnlockReleaseBuffer(buf);
                        so->current_blkno = InvalidBlockNumber;
                        return false;
                    }

                    /* 匹配，返回结果 */
                    scan->xs_heaptid  = tuple->t_tid;
                    so->current_blkno = blkno;
                    so->current_off   = off + 1;
                    UnlockReleaseBuffer(buf);
                    return true;
                }

                UnlockReleaseBuffer(buf);

                if (next_blkno == InvalidBlockNumber)
                {
                    so->current_blkno = InvalidBlockNumber;
                    return false;
                }

                blkno = next_blkno;
                off   = FirstOffsetNumber;
            }
        }
    }

    /* ── Scan loop - no global lock needed ── */
    for (;;)
    {
        Buffer          buf;
        Page            page;
        BlockNumber     next_blkno;
        OffsetNumber    max_off;

        buf        = ReadBuffer(index, blkno);
        LockBuffer(buf, BUFFER_LOCK_SHARE);
        page       = BufferGetPage(buf);
        max_off    = PageGetMaxOffsetNumber(page);
        next_blkno = MyamPageGetOpaque(page)->next;

        while (off <= max_off)
        {
            ItemId         iid   = PageGetItemId(page, off);
            MyamIndexTuple tuple = (MyamIndexTuple) PageGetItem(page, iid);

            /* Early upper-bound termination */
            bool past_upper = false;
            for (int ki = 0; ki < scan->numberOfKeys; ki++)
            {
                ScanKey skey = &scan->keyData[ki];
                if (skey->sk_strategy == BTLessStrategyNumber ||
                    skey->sk_strategy == BTLessEqualStrategyNumber)
                {
                    int   col_idx = skey->sk_attno - 1;
                    Datum datum;
                    bool  is_null;

                    if (myam_tuple_get_col(meta, tuple, col_idx, &datum, &is_null) &&
                        !is_null)
                    {
                        int32 cmp = myam_cmp_col(meta, col_idx, datum,
                                                  skey->sk_argument);
                        if ((skey->sk_strategy == BTLessStrategyNumber)
                                ? (cmp >= 0) : (cmp > 0))
                        {
                            past_upper = true;
                            break;
                        }
                    }
                }
            }

            if (past_upper)
            {
                UnlockReleaseBuffer(buf);
                so->current_blkno = InvalidBlockNumber;
                return false;
            }

            if (myam_tuple_matches_scan_keys(scan, tuple))
            {
                scan->xs_heaptid  = tuple->t_tid;
                so->current_blkno = blkno;
                so->current_off   = off + 1;
                UnlockReleaseBuffer(buf);
                return true;
            }

            off++;
        }

        UnlockReleaseBuffer(buf);

        if (next_blkno == InvalidBlockNumber)
        {
            so->current_blkno = InvalidBlockNumber;
            return false;
        }

        blkno = next_blkno;
        off   = FirstOffsetNumber;
    }
}

static bool
myam_tuple_get_col(MyamMeta *meta, MyamIndexTuple tuple, int col_idx, Datum *datum, bool *is_null)
{
    int null_bitmap_len = (meta->nkeys + 7) / 8;
    const char *bitmap = tuple->data;
    const char *ptr = tuple->data + null_bitmap_len;

    if (col_idx < 0 || col_idx >= meta->nkeys)
        return false;

    *is_null = (bitmap[col_idx/8] & (1 << (col_idx%8))) != 0;
    if (*is_null)
    {
        *datum = (Datum) 0;
        return true;
    }

    for (int i = 0; i < meta->nkeys; i++)
    {
        bool col_null = (bitmap[i/8] & (1 << (i%8))) != 0;
        if (!col_null)
        {
            Size len;
            if (meta->key_typlen[i] == -1)
            {
                struct varlena *v = (struct varlena *) ptr;
                len = VARSIZE_ANY(v);
                if (i == col_idx)
                    *datum = PointerGetDatum(v);
            }
            else
            {
                len = meta->key_typlen[i];
                if (i == col_idx)
                {
                    if (meta->key_byval[i])
                    {
                        Datum d = (Datum) 0;
                        memcpy(&d, ptr, len);
                        *datum = d;
                    }
                    else
                        *datum = PointerGetDatum(ptr);
                }
            }

            ptr += len;
        }

        if (i == col_idx)
            return true;
    }

    return false;
}

static bool
myam_tuple_matches_scan_keys(IndexScanDesc scan, MyamIndexTuple tuple)
{
    MyamScanOpaque so = (MyamScanOpaque) scan->opaque;
    MyamMeta *meta = so->meta;

    for (int i = 0; i < scan->numberOfKeys; i++)
    {
        ScanKey skey = &scan->keyData[i];
        int col_idx = skey->sk_attno - 1;

        Datum datum = (Datum) 0;
        bool is_null = false;

        if (!myam_tuple_get_col(meta, tuple, col_idx, &datum, &is_null))
            continue;

        if (skey->sk_flags & SK_ISNULL)
        {
            if (!is_null)
                return false;
            continue;
        }

        if (is_null)
            return false;

        Datum test_value = skey->sk_argument;
        bool matches = false;

        if (skey->sk_strategy >= BTLessStrategyNumber &&
            skey->sk_strategy <= BTGreaterStrategyNumber)
        {
            int32 cmp = myam_cmp_col(meta, col_idx, datum, test_value);
            switch (skey->sk_strategy)
            {
                case BTLessStrategyNumber:
                    matches = (cmp < 0);
                    break;
                case BTLessEqualStrategyNumber:
                    matches = (cmp <= 0);
                    break;
                case BTEqualStrategyNumber:
                    matches = (cmp == 0);
                    break;
                case BTGreaterEqualStrategyNumber:
                    matches = (cmp >= 0);
                    break;
                case BTGreaterStrategyNumber:
                    matches = (cmp > 0);
                    break;
            }
        }
        else
        {
            Datum result = FunctionCall2Coll(&skey->sk_func,
                                             skey->sk_collation,
                                             datum,
                                             test_value);
            matches = DatumGetBool(result);
        }

        if (!matches)
            return false;
    }

    return true;
}

/*
 * 获取位图
 */
int64
myam_getbitmap(IndexScanDesc scan, TIDBitmap *bitmap)
{
    int64 ntids = 0;

    while (myam_gettuple(scan, ForwardScanDirection))
    {
        tbm_add_tuples(bitmap, &scan->xs_heaptid, 1, false);
        ntids++;
    }

    return ntids;
}

/*
 * 结束扫描
 */
void
myam_endscan(IndexScanDesc scan)
{
    MyamScanOpaque so = (MyamScanOpaque) scan->opaque;

    if (so)
    {
        if (BufferIsValid(so->current_buf))
            UnlockReleaseBuffer(so->current_buf);
        pfree(so);
    }
}

IndexBulkDeleteResult *
myam_bulkdelete(IndexVacuumInfo *info,
                IndexBulkDeleteResult *stats,
                IndexBulkDeleteCallback callback,
                void *callback_state)
{
    Relation  index = info->index;
    MyamMeta *meta  = myam_meta_get(index);
    LWLock   *lock  = myam_get_lwlock(RelationGetRelid(index));

    if (stats == NULL)
        stats = (IndexBulkDeleteResult *) palloc0(sizeof(IndexBulkDeleteResult));
    stats->tuples_removed = 0;

    LWLockAcquire(lock, LW_EXCLUSIVE);
    myam_read_meta_page(index, meta);  /* always refresh on write path */

    if (!meta || meta->root_blkno == InvalidBlockNumber)
    {
        stats->num_pages = 0;
        stats->num_index_tuples = 0;
        LWLockRelease(lock);
        return stats;
    }

    /* Walk all leaf pages via the linked list starting from the leftmost leaf */
    BlockNumber blkno = myam_find_leftmost_leaf_blkno(index, meta);

    while (blkno != InvalidBlockNumber)
    {
        Buffer          buf    = ReadBuffer(index, blkno);
        LockBuffer(buf, BUFFER_LOCK_EXCLUSIVE);
        Page            page   = BufferGetPage(buf);
        OffsetNumber    max_off = PageGetMaxOffsetNumber(page);
        BlockNumber     next   = MyamPageGetOpaque(page)->next;

        OffsetNumber *to_delete = NULL;
        int          ndelete    = 0;

        for (OffsetNumber off = FirstOffsetNumber; off <= max_off; off++)
        {
            ItemId         iid   = PageGetItemId(page, off);
            MyamIndexTuple tuple = (MyamIndexTuple) PageGetItem(page, iid);

            if (callback(&tuple->t_tid, callback_state))
            {
                if (to_delete == NULL)
                    to_delete = (OffsetNumber *) palloc(sizeof(OffsetNumber) * max_off);
                to_delete[ndelete++] = off;
                stats->tuples_removed++;

                /* Invalidate PDC entries for this TID */
                myam_cache_remove_tid(meta, &tuple->t_tid);
            }
        }

        if (ndelete > 0)
        {
            PageIndexMultiDelete(page, to_delete, ndelete);
            MyamPageGetOpaque(page)->max_off = PageGetMaxOffsetNumber(page);
            {
                GenericXLogState *xlog = GenericXLogStart(index);
                GenericXLogRegisterBuffer(xlog, buf, GENERIC_XLOG_FULL_IMAGE);
                GenericXLogFinish(xlog);
            }
            pfree(to_delete);
        }

        UnlockReleaseBuffer(buf);
        blkno = next;
    }

    stats->num_pages = meta->total_pages;
    stats->num_index_tuples = meta->total_tuples;
    LWLockRelease(lock);
    return stats;
}

/*
 * 成本估算
 *
 * MyAM 是纯内存 B+树；无磁盘 I/O 开销。
 * 调用 genericcostestimate() 获得真实的选择率，然后用
 * O(log₂ N) 树遍历开销替换磁盘 I/O 部分，使规划器能正确
 * 区分点查与顺序扫描。
 */
void
myam_costestimate(PlannerInfo *root, IndexPath *path, double loop_count,
                 Cost *indexStartupCost, Cost *indexTotalCost,
                 Selectivity *indexSelectivity, double *indexCorrelation,
                 double *indexPages)
{
    IndexOptInfo       *index = path->indexinfo;
    GenericCosts        costs;

    MemSet(&costs, 0, sizeof(costs));

    /* Let genericcostestimate compute a proper selectivity. */
    genericcostestimate(root, path, loop_count, &costs);

    *indexSelectivity = costs.indexSelectivity;
    *indexCorrelation = 1.0;    /* B+tree maintains sort order */
    *indexPages       = 0.0;    /* in-memory: no disk pages */

    /*
     * Startup: O(log₂ N) comparisons to reach the first qualifying leaf.
     * Run: cpu_index_tuple_cost per returned index entry.
     */
    double ntuples = Max(index->rel->tuples, 1.0);
    double log_n   = (ntuples > 1.0) ? log(ntuples) / M_LN2 : 1.0;

    *indexStartupCost = cpu_operator_cost * log_n;
    *indexTotalCost   = *indexStartupCost
                        + cpu_index_tuple_cost * costs.numIndexTuples
                        + cpu_tuple_cost        * costs.numIndexTuples;

    /*
     * MyAM data lives in process-local memory (TopMemoryContext of the
     * creating backend).  Parallel workers fork with an empty myam_meta_hash
     * and would silently return zero rows.  Inflate the cost whenever the
     * session allows parallel workers so the planner strongly prefers a
     * serial plan.
     *
     * 修改：不再使用1000倍惩罚，改为根据查询类型动态调整
     * - 点查询（返回少量行）：10倍惩罚
     * - 范围查询（返回大量行）：根据返回行数比例调整
     */
    if (max_parallel_workers_per_gather > 0)
    {
        double selectivity = costs.numIndexTuples / Max(ntuples, 1.0);

        if (selectivity < 0.001)  /* 点查询或极小范围 */
        {
            *indexStartupCost *= 10.0;
            *indexTotalCost   *= 10.0;
        }
        else if (selectivity < 0.01)  /* 小范围查询 */
        {
            *indexStartupCost *= 20.0;
            *indexTotalCost   *= 20.0;
        }
        else  /* 大范围查询 */
        {
            /* 根据选择率动态调整：选择率越高，惩罚越大 */
            double penalty = 10.0 + selectivity * 990.0;
            *indexStartupCost *= penalty;
            *indexTotalCost   *= penalty;
        }
    }
}

/*
 * 初始化后台优化字段（保留用于兼容性）
 */
void
myam_background_optimize_init(MyamMeta *meta)
{
    meta->background_optimize_status = MYAM_BACKGROUND_OPTIMIZE_IDLE;
    meta->queries_since_last_optimize = 0;
    meta->optimization_needed = false;
}

/*
 * 检查是否需要触发后台优化（空实现，由BackgroundWorker处理）
 */
void
myam_background_optimize_check(MyamMeta *meta)
{
    /* 空实现 - 优化现在由BackgroundWorker处理 */
}

/*
 * 触发后台优化（空实现，由BackgroundWorker处理）
 */
void
myam_background_optimize_trigger(MyamMeta *meta)
{
    /* 空实现 - 优化现在由BackgroundWorker处理 */
}

/*
 * 尝试运行后台优化（空实现，由BackgroundWorker处理）
 */
bool
myam_background_optimize_try_run(MyamMeta *meta)
{
    /* 空实现 - 优化现在由BackgroundWorker处理 */
    return false;
}

/*
 * 贪心优化
 */
void
myam_greedy_optimize(MyamMeta *meta)
{
    /* Skipped for now */
}

/* ==================== PDC 与哈希 ==================== */

/* Macro for fast integer hashing and comparison */
#define MYAM_FAST_HASH_INT4(val) ((uint32)(val))
#define MYAM_FAST_EQ_INT4(a, b) ((a) == (b))

/*
 * Serialize a MyamKey into a flat byte buffer (null bitmap + column data).
 * Returns the number of bytes written, or 0 if the key exceeds maxbuf.
 * Layout mirrors myam_form_tuple: nbm-byte null bitmap followed by column data.
 */
static int
myam_pdc_serialize_key(MyamMeta *meta, MyamKey *key,
                       char *buf, int maxbuf)
{
    int   nbm  = (meta->nkeys + 7) / 8;
    int   pos  = nbm;
    int   i;

    if (pos > maxbuf)
        return 0;

    /* null bitmap */
    memset(buf, 0, nbm);
    for (i = 0; i < meta->nkeys; i++)
        if (key->isnull[i])
            buf[i / 8] |= (uint8)(1 << (i % 8));

    /* column data */
    for (i = 0; i < meta->nkeys; i++)
    {
        int16 typlen;
        int   colsz;

        if (key->isnull[i])
            continue;

        typlen = meta->key_typlen[i];

        if (typlen == -1)
        {
            struct varlena *orig = (struct varlena *) DatumGetPointer(key->keys[i]);
            struct varlena *v    = PG_DETOAST_DATUM_PACKED(key->keys[i]);
            colsz = (int) VARSIZE_ANY(v);
            if (pos + colsz > maxbuf)
            {
                if ((Pointer) v != (Pointer) orig) pfree(v);
                return 0;
            }
            memcpy(buf + pos, v, colsz);
            if ((Pointer) v != (Pointer) orig) pfree(v);
        }
        else
        {
            colsz = (int) typlen;
            if (pos + colsz > maxbuf)
                return 0;
            if (meta->key_byval[i])
                memcpy(buf + pos, &key->keys[i], colsz);
            else
                memcpy(buf + pos, DatumGetPointer(key->keys[i]), colsz);
        }
        pos += colsz;
    }
    return pos;
}

static inline uint32
myam_hash_key(MyamMeta *meta, MyamKey *key)
{
    if (!meta || !key)
        return 0;

    uint32 h = 0x9e3779b9;  /* Golden ratio - better initial seed */
    for (int i = 0; i < key->nkeys; i++)
    {
        if (key->isnull[i])
            continue;

        Oid type_oid = meta->key_type_oids[i];
        Datum d = key->keys[i];
        uint32 col_h = 0;

        /* Fast path for INT4 - use better mixing */
        if (type_oid == INT4OID)
        {
            uint32 v = (uint32) DatumGetInt32(d);
            /* MurmurHash3 finalizer for better distribution */
            v ^= v >> 16;
            v *= 0x85ebca6b;
            v ^= v >> 13;
            v *= 0xc2b2ae35;
            v ^= v >> 16;
            col_h = v;
        }
        else if (type_oid == INT8OID)
        {
            int64 v = DatumGetInt64(d);
            col_h = (uint32) (v ^ (v >> 32));
        }
        else if (type_oid == TEXTOID || type_oid == VARCHAROID)
        {
            /*
             * Use PG_DETOAST_DATUM_PACKED to ensure we have accessible data
             * without unnecessary decompression if possible, or fetch from toast if needed.
             */
            struct varlena *v = (struct varlena *) DatumGetPointer(d);
            struct varlena *detoasted = PG_DETOAST_DATUM_PACKED(d);

            char *data = VARDATA_ANY(detoasted);
            int len = VARSIZE_ANY_EXHDR(detoasted);

            col_h = DatumGetUInt32(hash_any((const unsigned char *) data, len));

            /* Free if we allocated a new copy */
            if ((Pointer) detoasted != (Pointer) v)
                pfree(detoasted);
        }
        else
        {
            /* Fallback */
            col_h = tag_hash(&d, sizeof(Datum));
        }

        /* Better hash combining - rotate and XOR */
        h ^= col_h + 0x9e3779b9 + (h << 6) + (h >> 2);
    }
    return h;
}


static inline uint32
myam_pdc_index(PageDirectCache *pdc, uint32 h)
{
    if (!pdc || pdc->capacity == 0)
        return 0;
    if (pdc->mask)
        return h & pdc->mask;
    return h % pdc->capacity;
}

/*
 * 初始化 PDC（开放定址线性探测哈希）。
 *
 * 初始容量 4096，负载因子上限 0.75 → 最多 3072 个活跃条目。
 * 所有条目通过 MemoryContextAllocZero 清零，因此 state == PDC_EMPTY (0)。
 */
void
myam_pdc_init(MyamMeta *meta)
{
    if (!meta || meta->pdc)
        return;

    MemoryContext cxt = meta->index_cxt ? meta->index_cxt : TopMemoryContext;

    meta->pdc = (PageDirectCache *) MemoryContextAllocZero(cxt, sizeof(PageDirectCache));

    uint32 cap = 524288;  /* 512 K slots × ~56 bytes ≈ 28.7 MB */
    meta->pdc->capacity   = cap;
    meta->pdc->mask       = cap - 1;
    meta->pdc->clock_hand = 0;
    meta->pdc->entries    = (PDCEntry *) MemoryContextAllocZero(cxt, sizeof(PDCEntry) * cap);

    meta->cache_capacity = (int32) cap;
}

/*
 * PDC 查找（线性探测）。
 *
 * 探测终止条件：
 *   PDC_EMPTY  → key 不在表中（probe chain 在此中断）；
 *   PDC_DELETED → 跳过（tombstone，chain 仍连续）；
 *   PDC_VALID  → 比较 hash 后比较 key；命中则返回 true。
 */
bool
myam_pdc_lookup(MyamMeta *meta, MyamKey *key, ItemPointer result_tid)
{
    PageDirectCache *pdc = meta->pdc;

    if (!pdc)
        return false;

    /* Fast path for single-column INT4 (most common case) */
    if (meta->nkeys == 1 && meta->key_type_oids[0] == INT4OID && !key->isnull[0])
    {
        uint32 h = myam_hash_key(meta, key);
        uint32 cap = pdc->capacity;
        uint32 mask = pdc->mask;
        uint32 idx = h & mask;
        uint32 probe = 0;
        uint32 max_probe = 512;
        int32 search_val = DatumGetInt32(key->keys[0]);

        while (probe < max_probe && probe < cap)
        {
            PDCEntry *e = &pdc->entries[idx];

            if (e->state == PDC_EMPTY)
                break;

            if (e->state == PDC_VALID &&
                e->hash == h &&
                e->key_size == sizeof(int32) &&
                *(int32*)e->key_data == search_val)
            {
                *result_tid = e->tid;
                e->access_count++;
                meta->cache_hits++;
                return true;
            }

            idx = (idx + 1) & mask;
            probe++;
        }

        meta->cache_misses++;
        return false;
    }

    /* General path for multi-column or non-INT4 keys */
    char   kbuf[MYAM_PDC_KEY_INLINE_BYTES];
    int    ksize = myam_pdc_serialize_key(meta, key, kbuf, MYAM_PDC_KEY_INLINE_BYTES);

    if (ksize == 0)
    {
        meta->cache_misses++;
        return false;  /* key too large for inline storage */
    }

    uint32 h     = myam_hash_key(meta, key);
    uint32 cap   = pdc->capacity;
    uint32 mask  = pdc->mask;
    uint32 idx   = h & mask;
    uint32 probe = 0;
    uint32 max_probe = 512;  /* Limit max probes to prevent excessive scanning */

    while (probe < max_probe && probe < cap)
    {
        PDCEntry *e = &pdc->entries[idx];

        if (e->state == PDC_EMPTY)
            break;  /* end of probe chain; key not present */

        if (e->state == PDC_VALID &&
            e->hash == h &&
            e->key_size == (uint8) ksize &&
            memcmp(e->key_data, kbuf, ksize) == 0)
        {
            *result_tid = e->tid;
            e->access_count++;
            meta->cache_hits++;
            return true;
        }

        idx = (idx + 1) & mask;
        probe++;
    }

    meta->cache_misses++;
    return false;
}

/*
 * PDC 插入（线性探测 + 复用 tombstone）。
 *
 * 如果 key 已存在，更新现有条目（不改变 cache_size）。
 * 否则写入第一个 PDC_EMPTY 或 PDC_DELETED 槽，并增加 cache_size。
 */
void
myam_pdc_insert(MyamMeta *meta, MyamKey *key, BlockNumber blkno,
                OffsetNumber off, ItemPointer tid, bool is_hot)
{
    PageDirectCache *pdc = meta->pdc;

    if (!pdc)
        return;

    /* Fast path for single-column INT4 */
    if (meta->nkeys == 1 && meta->key_type_oids[0] == INT4OID && !key->isnull[0])
    {
        uint32 h = myam_hash_key(meta, key);
        uint32 cap = pdc->capacity;
        uint32 mask = pdc->mask;
        uint32 idx = h & mask;
        int32 insert_idx = -1;
        uint32 probe = 0;
        uint32 max_probe = 512;
        int32 search_val = DatumGetInt32(key->keys[0]);

        while (probe < max_probe && probe < cap)
        {
            PDCEntry *e = &pdc->entries[idx];

            if (e->state == PDC_EMPTY)
            {
                if (insert_idx < 0)
                    insert_idx = (int32) idx;
                break;
            }

            if (e->state == PDC_DELETED)
            {
                if (insert_idx < 0)
                    insert_idx = (int32) idx;
            }
            else if (e->hash == h &&
                     e->key_size == sizeof(int32) &&
                     *(int32*)e->key_data == search_val)
            {
                /* Update existing entry */
                e->blkno = blkno;
                e->off = off;
                e->tid = *tid;
                e->is_hot = is_hot;
                return;
            }

            idx = (idx + 1) & mask;
            probe++;
        }

        if (insert_idx < 0)
            return;

        PDCEntry *slot = &pdc->entries[(uint32) insert_idx];
        slot->state = PDC_VALID;
        slot->hash = h;
        *(int32*)slot->key_data = search_val;
        slot->key_size = sizeof(int32);
        slot->blkno = blkno;
        slot->off = off;
        slot->tid = *tid;
        slot->access_count = 1;
        slot->is_hot = is_hot;
        meta->cache_size++;
        return;
    }

    /* General path */
    char   kbuf[MYAM_PDC_KEY_INLINE_BYTES];
    int    ksize = myam_pdc_serialize_key(meta, key, kbuf, MYAM_PDC_KEY_INLINE_BYTES);

    if (ksize == 0)
        return;  /* key too large for inline storage; skip caching */

    uint32 h    = myam_hash_key(meta, key);
    uint32 cap  = pdc->capacity;
    uint32 mask = pdc->mask;
    uint32 idx  = h & mask;
    int32  insert_idx = -1;   /* best available slot (EMPTY or first DELETED) */
    uint32 probe = 0;
    uint32 max_probe = 512;  /* Limit max probes to prevent excessive scanning */

    while (probe < max_probe && probe < cap)
    {
        PDCEntry *e = &pdc->entries[idx];

        if (e->state == PDC_EMPTY)
        {
            /* Probe chain ends here; key absent. Use this slot if no tombstone found. */
            if (insert_idx < 0)
                insert_idx = (int32) idx;
            break;
        }

        if (e->state == PDC_DELETED)
        {
            /* Tombstone: remember first one, keep probing for existing key. */
            if (insert_idx < 0)
                insert_idx = (int32) idx;
        }
        else if (e->hash == h &&
                 e->key_size == (uint8) ksize &&
                 memcmp(e->key_data, kbuf, ksize) == 0)
        {
            /* Key already cached; update in place. cache_size unchanged. */
            e->blkno   = blkno;
            e->off     = off;
            e->tid     = *tid;
            e->is_hot  = is_hot;
            return;
        }

        idx = (idx + 1) & mask;
        probe++;
    }

    if (insert_idx < 0)
        return;  /* table completely full — should never happen given eviction */

    PDCEntry *slot = &pdc->entries[(uint32) insert_idx];
    slot->state        = PDC_VALID;
    slot->hash         = h;
    memcpy(slot->key_data, kbuf, ksize);
    slot->key_size     = (uint8) ksize;
    slot->blkno        = blkno;
    slot->off          = off;
    slot->tid          = *tid;
    slot->access_count = 1;
    slot->is_hot       = is_hot;
    meta->cache_size++;
}

/*
 * PDC 按 key 删除：设置 tombstone (PDC_DELETED) 保持探测链连续。
 */
void
myam_pdc_remove(MyamMeta *meta, MyamKey *key)
{
    PageDirectCache *pdc = meta->pdc;

    if (!pdc)
        return;

    char   kbuf[MYAM_PDC_KEY_INLINE_BYTES];
    int    ksize = myam_pdc_serialize_key(meta, key, kbuf, MYAM_PDC_KEY_INLINE_BYTES);

    if (ksize == 0)
        return;  /* key too large; can't be in cache */

    uint32 h     = myam_hash_key(meta, key);
    uint32 cap   = pdc->capacity;
    uint32 mask  = pdc->mask;
    uint32 idx   = h & mask;
    uint32 probe = 0;
    uint32 max_probe = 512;  /* Limit max probes to prevent excessive scanning */

    while (probe < max_probe && probe < cap)
    {
        PDCEntry *e = &pdc->entries[idx];

        if (e->state == PDC_EMPTY)
            return;  /* not found */

        if (e->state == PDC_VALID &&
            e->hash == h &&
            e->key_size == (uint8) ksize &&
            memcmp(e->key_data, kbuf, ksize) == 0)
        {
            e->state = PDC_DELETED;   /* tombstone */
            meta->cache_size = Max(0, meta->cache_size - 1);
            return;
        }

        idx = (idx + 1) & mask;
        probe++;
    }
}

/*
 * PDC 按 TID 删除：线性扫描全表（仅 VACUUM 调用，不在热路径上）。
 */
void
myam_pdc_remove_tid(MyamMeta *meta, ItemPointer tid)
{
    PageDirectCache *pdc = meta->pdc;

    if (!pdc)
        return;

    for (uint32 i = 0; i < pdc->capacity; i++)
    {
        PDCEntry *e = &pdc->entries[i];
        if (e->state == PDC_VALID && ItemPointerCompare(&e->tid, tid) == 0)
        {
            e->state = PDC_DELETED;
            meta->cache_size = Max(0, meta->cache_size - 1);
            /* TIDs should be unique in the cache; stop after first match. */
            return;
        }
    }
}

/* ==================== Predictor 层 ==================== */

bool
myam_predictor_should_cache(MyamMeta *meta, MyamKey *key)
{
    return true; /* Always cache */
}

void
myam_predictor_on_access(MyamMeta *meta, MyamKey *key, bool hit)
{
    meta->queries_since_last_optimize++;
    if (meta->cache_size >= meta->cache_capacity)
         meta->optimization_needed = true;
}

PG_FUNCTION_INFO_V1(myam_btree_lookup);
Datum
myam_btree_lookup(PG_FUNCTION_ARGS)
{
    Oid idx_oid = PG_GETARG_OID(0);
    int32 k = PG_GETARG_INT32(1);
    Relation indexRel = RelationIdGetRelation(idx_oid);
    if (!RelationIsValid(indexRel))
        PG_RETURN_BOOL(false);
    MyamMeta *meta = myam_meta_get(indexRel);
    
    MyamKey key;
    memset(&key, 0, sizeof(MyamKey));
    key.nkeys = 1;
    key.keys[0] = Int32GetDatum(k);
    key.isnull[0] = false;
    
    ItemPointerData tid;
    bool ok = myam_btree_search(indexRel, meta, &key, &tid);
    RelationClose(indexRel);
    PG_RETURN_BOOL(ok);
}

/*
 * Get tuple count from meta
 */
PG_FUNCTION_INFO_V1(myam_meta_tuple_count);
Datum
myam_meta_tuple_count(PG_FUNCTION_ARGS)
{
    Oid indexOid = PG_GETARG_OID(0);
    Relation indexRel = index_open(indexOid, AccessShareLock);
    MyamMeta *meta = myam_meta_get(indexRel);
    int32 count = meta ? meta->total_tuples : 0;
    index_close(indexRel, AccessShareLock);
    PG_RETURN_INT32(count);
}

#include "executor/spi.h"

/*
 * General Benchmark function returning stats
 */
PG_FUNCTION_INFO_V1(index_bench_stats);
Datum
index_bench_stats(PG_FUNCTION_ARGS)
{
    Oid         indexOid = PG_GETARG_OID(0);
    int32       nqueries = PG_GETARG_INT32(1);
    int32       nkeys = PG_GETARG_INT32(2);
    int32       skew = PG_GETARG_INT32(3);
    
    int32       i;
    int32       hot_range = nkeys * 30 / 100;
    
    double      total_time = 0;
    double      min_time = DBL_MAX;
    double      max_time = 0;
    double      sum_sq_time = 0;
    
    /* Output tuple */
    TupleDesc   tupdesc;
    Datum       values[6];
    bool        nulls[6] = {false};
    HeapTuple   tuple;
    
    char        *am_name;
    Relation    indexRel;
    
    /* Get AM name for reporting */
    indexRel = index_open(indexOid, AccessShareLock);
    am_name = get_am_name(indexRel->rd_rel->relam);
    index_close(indexRel, AccessShareLock);
    
    /* Connect to SPI */
    if (SPI_connect() != SPI_OK_CONNECT)
        elog(ERROR, "SPI_connect failed");
    
    /* Prepare plan */
    char *sql = "SELECT 1 FROM t WHERE id = $1";
    Oid argtypes[1] = {INT4OID};
    SPIPlanPtr plan = SPI_prepare(sql, 1, argtypes);
    if (plan == NULL)
        elog(ERROR, "SPI_prepare failed: %s", SPI_result_code_string(SPI_result));
        
    /* Save plan */
    if (SPI_keepplan(plan))
        elog(ERROR, "SPI_keepplan failed");
        
    srand(time(NULL));
    
    Datum args[1];
    char nulls_args[1] = {' '};
    
    for (i = 0; i < nqueries; i++)
    {
        /* Progress report */
        if (i > 0 && i % 100000 == 0)
            elog(NOTICE, "Bench: query %d", i);

        int32 key;
        int r = rand() % 100;
        if (r < skew)
            key = rand() % hot_range;
        else
            key = hot_range + (rand() % (nkeys - hot_range));
        
        args[0] = Int32GetDatum(key);
        
        /* Timer start */
        struct timespec start, end;
        clock_gettime(CLOCK_MONOTONIC, &start);
        
        int ret = SPI_execute_plan(plan, args, nulls_args, true, 1);
        if (ret < 0)
            elog(ERROR, "SPI_execute_plan failed: %s", SPI_result_code_string(ret));
            
        clock_gettime(CLOCK_MONOTONIC, &end);
        /* Timer end */
        
        double elapsed_ms = (end.tv_sec - start.tv_sec) * 1000.0 + (end.tv_nsec - start.tv_nsec) / 1000000.0;
        
        total_time += elapsed_ms;
        if (elapsed_ms < min_time) min_time = elapsed_ms;
        if (elapsed_ms > max_time) max_time = elapsed_ms;
        sum_sq_time += elapsed_ms * elapsed_ms;
    }
    
    SPI_finish();
    
    /* Build result tuple */
    if (get_call_result_type(fcinfo, NULL, &tupdesc) != TYPEFUNC_COMPOSITE)
        ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                        errmsg("function returning record called in context that cannot accept type record")));
    BlessTupleDesc(tupdesc);
    
    double avg_time = total_time / nqueries;
    double variance = (sum_sq_time / nqueries) - (avg_time * avg_time);
    double stddev = sqrt(variance > 0 ? variance : 0);
    
    values[0] = CStringGetTextDatum(am_name);
    values[1] = Int32GetDatum(nqueries);
    values[2] = Float8GetDatum(avg_time);
    values[3] = Float8GetDatum(min_time);
    values[4] = Float8GetDatum(max_time);
    values[5] = Float8GetDatum(stddev);
    
    tuple = heap_form_tuple(tupdesc, values, nulls);
    PG_RETURN_DATUM(HeapTupleGetDatum(tuple));
}

PG_FUNCTION_INFO_V1(myam_cache_stats);
Datum
myam_cache_stats(PG_FUNCTION_ARGS)
{
    Oid indexOid = PG_GETARG_OID(0);
    Relation indexRel = index_open(indexOid, AccessShareLock);
    MyamMeta *meta = myam_meta_get(indexRel);
    
    ReturnSetInfo *rsinfo = (ReturnSetInfo *) fcinfo->resultinfo;
    TupleDesc tupdesc;
    Tuplestorestate *tupstore;
    MemoryContext oldcontext;
    
    /* check to see if caller supports us returning a tuplestore */
    if (rsinfo == NULL || !IsA(rsinfo, ReturnSetInfo))
        ereport(ERROR,
                (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                 errmsg("set-valued function called in context that cannot accept a set")));
    if (!(rsinfo->allowedModes & SFRM_Materialize))
        ereport(ERROR,
                (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                 errmsg("materialize mode required, but it is not " \
                        "allowed in this context")));
                        
    /* Build a tuple descriptor for our result type */
    if (get_call_result_type(fcinfo, NULL, &tupdesc) != TYPEFUNC_COMPOSITE)
        ereport(ERROR,
                (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                 errmsg("return type must be a row type")));
                 
    tupstore = tuplestore_begin_heap(true, false, work_mem);
    rsinfo->returnMode = SFRM_Materialize;
    rsinfo->setResult = tupstore;
    rsinfo->setDesc = tupdesc;
    
    oldcontext = MemoryContextSwitchTo(rsinfo->econtext->ecxt_per_query_memory);
    
    if (meta)
    {
        Datum values[4];
        bool nulls[4];
        memset(nulls, 0, sizeof(nulls));
        
        values[0] = Int64GetDatum(meta->cache_hits);
        values[1] = Int64GetDatum(meta->cache_misses);
        values[2] = Int32GetDatum(meta->cache_size);
        values[3] = Int32GetDatum(meta->cache_capacity);
        
        tuplestore_putvalues(tupstore, tupdesc, values, nulls);
    }
    
    /* tuplestore_donestoring(tupstore); removed as it is not available */
    MemoryContextSwitchTo(oldcontext);
    
    index_close(indexRel, AccessShareLock);
    return (Datum) 0;
}

PG_FUNCTION_INFO_V1(myam_bench);
Datum
myam_bench(PG_FUNCTION_ARGS)
{
    PG_RETURN_INT32(0); /* Dummy implementation */
}

/* ==================== 自适应范围扫描实现 ==================== */

/*
 * 初始化自适应阈值
 */
void
myam_adaptive_init(MyamMeta *meta)
{
    AdaptiveThresholds *thresholds;

    thresholds = (AdaptiveThresholds *) MemoryContextAllocZero(
        meta->index_cxt,
        sizeof(AdaptiveThresholds));

    /* 初始阈值 */
    thresholds->pdc_range_threshold = 100;
    thresholds->pdc_hotness_threshold = 0.7;
    thresholds->hybrid_range_threshold = 500;
    thresholds->hybrid_hotness_threshold = 0.3;

    /* 统计信息 */
    thresholds->total_range_queries = 0;
    thresholds->pdc_strategy_wins = 0;
    thresholds->hybrid_strategy_wins = 0;
    thresholds->btree_strategy_wins = 0;
    thresholds->stats_index = 0;

    meta->adaptive_thresholds = thresholds;
    meta->enable_adaptive_range = true;
}

/*
 * 估算范围的热度
 * 通过采样检查PDC中的命中率来估算
 */
float
myam_estimate_hotness(MyamMeta *meta, MyamKey *start_key, MyamKey *end_key, int32 range_size)
{
    int32 sample_size;
    int32 hits = 0;
    int32 total = 0;

    /* 如果范围太大，使用固定采样大小 */
    if (range_size > MYAM_HOTNESS_SAMPLE_SIZE)
        sample_size = MYAM_HOTNESS_SAMPLE_SIZE;
    else
        sample_size = range_size;

    /* 对于INT4单列索引的快速路径 */
    if (meta->nkeys == 1 && meta->key_type_oids[0] == INT4OID &&
        !start_key->isnull[0] && !end_key->isnull[0])
    {
        int32 start_val = DatumGetInt32(start_key->keys[0]);
        int32 end_val = DatumGetInt32(end_key->keys[0]);
        int32 range = end_val - start_val;

        if (range <= 0)
            return 0.0;

        /* 均匀采样 */
        for (int i = 0; i < sample_size; i++)
        {
            int32 sample_val = start_val + (range * i) / sample_size;
            MyamKey sample_key;
            ItemPointerData dummy_tid;

            sample_key.nkeys = 1;
            sample_key.keys[0] = Int32GetDatum(sample_val);
            sample_key.isnull[0] = false;

            total++;
            if (myam_cache_search(meta, &sample_key, &dummy_tid))
                hits++;
        }
    }
    else
    {
        /* 对于复杂键，暂时返回保守估计 */
        return 0.0;
    }

    if (total == 0)
        return 0.0;

    return (float) hits / total;
}

/*
 * 选择范围扫描策略
 */
int32
myam_choose_range_strategy(MyamMeta *meta, int32 range_size, float hotness)
{
    AdaptiveThresholds *thresholds = meta->adaptive_thresholds;

    if (!meta->enable_adaptive_range || thresholds == NULL)
        return MYAM_RANGE_SCAN_PURE_BTREE;

    /* 策略1: 纯PDC扫描 */
    if (range_size < thresholds->pdc_range_threshold &&
        hotness > thresholds->pdc_hotness_threshold)
    {
        return MYAM_RANGE_SCAN_PURE_PDC;
    }

    /* 策略2: 混合扫描 */
    if (range_size < thresholds->hybrid_range_threshold &&
        hotness > thresholds->hybrid_hotness_threshold)
    {
        return MYAM_RANGE_SCAN_HYBRID;
    }

    /* 策略3: 纯B+树扫描 */
    return MYAM_RANGE_SCAN_PURE_BTREE;
}

/*
 * 更新范围扫描统计
 */
void
myam_adaptive_update_stats(MyamMeta *meta, RangeScanStats *stats)
{
    AdaptiveThresholds *thresholds = meta->adaptive_thresholds;

    if (thresholds == NULL)
        return;

    /* 保存到环形缓冲区 */
    int32 idx = thresholds->stats_index % MYAM_RANGE_STATS_WINDOW;
    thresholds->recent_stats[idx] = *stats;
    thresholds->stats_index++;
    thresholds->total_range_queries++;

    /* 每100次查询调整一次阈值 */
    if (thresholds->total_range_queries % 100 == 0)
    {
        myam_adaptive_adjust_thresholds(meta);
    }
}

/*
 * 自适应调整阈值
 * 基于最近的查询性能统计
 */
void
myam_adaptive_adjust_thresholds(MyamMeta *meta)
{
    AdaptiveThresholds *thresholds = meta->adaptive_thresholds;
    int32 window_size;
    double pdc_avg_time = 0.0;
    double hybrid_avg_time = 0.0;
    double btree_avg_time = 0.0;
    int32 pdc_count = 0;
    int32 hybrid_count = 0;
    int32 btree_count = 0;

    if (thresholds == NULL)
        return;

    /* 计算窗口大小 */
    window_size = (thresholds->total_range_queries < MYAM_RANGE_STATS_WINDOW) ?
                  thresholds->total_range_queries : MYAM_RANGE_STATS_WINDOW;

    /* 统计各策略的平均性能 */
    for (int i = 0; i < window_size; i++)
    {
        RangeScanStats *stat = &thresholds->recent_stats[i];

        switch (stat->strategy_used)
        {
            case MYAM_RANGE_SCAN_PURE_PDC:
                pdc_avg_time += stat->execution_time_us;
                pdc_count++;
                break;
            case MYAM_RANGE_SCAN_HYBRID:
                hybrid_avg_time += stat->execution_time_us;
                hybrid_count++;
                break;
            case MYAM_RANGE_SCAN_PURE_BTREE:
                btree_avg_time += stat->execution_time_us;
                btree_count++;
                break;
        }
    }

    if (pdc_count > 0)
        pdc_avg_time /= pdc_count;
    if (hybrid_count > 0)
        hybrid_avg_time /= hybrid_count;
    if (btree_count > 0)
        btree_avg_time /= btree_count;

    /* 调整阈值：如果某策略表现好，扩大其适用范围 */
    if (pdc_count > 10 && pdc_avg_time < btree_avg_time * 0.9)
    {
        /* PDC策略表现好，增加范围阈值 */
        thresholds->pdc_range_threshold = (int32)(thresholds->pdc_range_threshold * 1.1);
        if (thresholds->pdc_range_threshold > 200)
            thresholds->pdc_range_threshold = 200;
    }
    else if (pdc_count > 10 && pdc_avg_time > btree_avg_time * 1.1)
    {
        /* PDC策略表现差，减少范围阈值 */
        thresholds->pdc_range_threshold = (int32)(thresholds->pdc_range_threshold * 0.9);
        if (thresholds->pdc_range_threshold < 20)
            thresholds->pdc_range_threshold = 20;
    }

    /* 类似地调整混合策略阈值 */
    if (hybrid_count > 10 && hybrid_avg_time < btree_avg_time * 0.9)
    {
        thresholds->hybrid_range_threshold = (int32)(thresholds->hybrid_range_threshold * 1.1);
        if (thresholds->hybrid_range_threshold > 1000)
            thresholds->hybrid_range_threshold = 1000;
    }
    else if (hybrid_count > 10 && hybrid_avg_time > btree_avg_time * 1.1)
    {
        thresholds->hybrid_range_threshold = (int32)(thresholds->hybrid_range_threshold * 0.9);
        if (thresholds->hybrid_range_threshold < 100)
            thresholds->hybrid_range_threshold = 100;
    }
}

/*
 * 估算范围大小
 */
int32
myam_estimate_range_size(IndexScanDesc scan)
{
    MyamScanOpaque so = (MyamScanOpaque) scan->opaque;
    MyamMeta *meta = so->meta;

    /* 对于INT4单列索引的快速估算 */
    if (meta->nkeys == 1 && meta->key_type_oids[0] == INT4OID)
    {
        int32 lower = INT32_MIN;
        int32 upper = INT32_MAX;
        bool has_lower = false;
        bool has_upper = false;

        for (int i = 0; i < scan->numberOfKeys; i++)
        {
            ScanKey skey = &scan->keyData[i];
            int32 value;

            if (skey->sk_attno != 1 || (skey->sk_flags & SK_ISNULL))
                continue;

            value = DatumGetInt32(skey->sk_argument);

            if (skey->sk_strategy == BTGreaterEqualStrategyNumber ||
                skey->sk_strategy == BTGreaterStrategyNumber)
            {
                lower = value;
                has_lower = true;
            }
            else if (skey->sk_strategy == BTLessStrategyNumber ||
                     skey->sk_strategy == BTLessEqualStrategyNumber)
            {
                upper = value;
                has_upper = true;
            }
        }

        if (has_lower && has_upper)
            return upper - lower;
        else if (has_lower || has_upper)
            return 1000;  /* 单边界，保守估计 */
    }

    /* 其他情况，返回保守估计 */
    return 10000;
}

/*
 * 检查是否是简单的INT4范围查询
 */
bool
myam_is_simple_int4_range(MyamMeta *meta, IndexScanDesc scan,
                          int32 *lower, int32 *upper)
{
    if (meta->nkeys != 1 || meta->key_type_oids[0] != INT4OID)
        return false;

    bool has_lower = false;
    bool has_upper = false;
    *lower = INT32_MIN;
    *upper = INT32_MAX;

    for (int i = 0; i < scan->numberOfKeys; i++)
    {
        ScanKey skey = &scan->keyData[i];

        if (skey->sk_attno != 1 || (skey->sk_flags & SK_ISNULL))
            return false;

        int32 value = DatumGetInt32(skey->sk_argument);

        if (skey->sk_strategy == BTGreaterEqualStrategyNumber)
        {
            *lower = value;
            has_lower = true;
        }
        else if (skey->sk_strategy == BTGreaterStrategyNumber)
        {
            *lower = value + 1;
            has_lower = true;
        }
        else if (skey->sk_strategy == BTLessStrategyNumber)
        {
            *upper = value;
            has_upper = true;
        }
        else if (skey->sk_strategy == BTLessEqualStrategyNumber)
        {
            *upper = value + 1;
            has_upper = true;
        }
        else if (skey->sk_strategy == BTEqualStrategyNumber)
        {
            *lower = value;
            *upper = value + 1;
            has_lower = true;
            has_upper = true;
        }
        else
        {
            return false;  /* 不支持的策略 */
        }
    }

    return has_lower && has_upper && *lower < *upper;
}
