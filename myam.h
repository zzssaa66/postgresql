#ifndef ACCESS_MYAM_H
#define ACCESS_MYAM_H

#include "postgres.h"
#include "access/genam.h"
#include "access/amapi.h"
#include "fmgr.h"
#include "utils/rel.h"
#include "utils/hsearch.h"
#include "storage/block.h"
#include "storage/buf.h"
#include "storage/bufmgr.h"
#include "storage/itemptr.h"
#include "access/itup.h"
#include "nodes/pathnodes.h"
#include "optimizer/cost.h"
#include "postmaster/bgworker.h"
#include "storage/latch.h"
#include "storage/lwlock.h"

/* KP-Index (Knapsack-based Performance Index) 完整实现 */

/* Fast comparison type codes stored in MyamMeta.fast_cmp_type[].
 * MYAM_CMP_GENERIC (0) means fall back to FunctionCall2Coll.
 * Other values enable inline comparisons that bypass fmgr overhead. */
#define MYAM_CMP_GENERIC  0
#define MYAM_CMP_INT4     1
#define MYAM_CMP_INT8     2
#define MYAM_CMP_FLOAT4   3
#define MYAM_CMP_FLOAT8   4

/* 页面类型定义 */
#define MYAM_PAGE_MAGIC     0x4D59414D  /* "MYAM" */
#define MYAM_META_BLKNO     0           /* block 0 = on-disk meta page */
#define MYAM_LEAF_PAGE      0x01
#define MYAM_INTERNAL_PAGE  0x02
#define MYAM_ROOT_PAGE      0x04
#define MYAM_META_PAGE      0x10        /* flags value for meta block */

/* 页面大小和容量 */
#define MYAM_PAGE_SIZE          BLCKSZ
#define MYAM_MAX_ITEMS_PER_PAGE 200
#define MYAM_INTERNAL_FANOUT    128
#define MYAM_DEFAULT_CACHE_CAPACITY 100000
#define MYAM_PDC_KEY_INLINE_BYTES 32   /* 32 bytes covers 7× int4, 3× int8, etc. */
#define MYAM_KNAPSACK_THRESHOLD 100

/* 优化器模式与阈值（支持混合策略） */
#define MYAM_OPTIMIZER_GREEDY    1
#define MYAM_OPTIMIZER_KNAPSACK  2
#define MYAM_OPTIMIZER_HYBRID    3

#ifndef MYAM_OPTIMIZER_MODE
#define MYAM_OPTIMIZER_MODE MYAM_OPTIMIZER_HYBRID
#endif

#ifndef MYAM_GREEDY_RUN_THRESHOLD
#define MYAM_GREEDY_RUN_THRESHOLD 1000
#endif

#ifndef MYAM_KNAPSACK_SLOW_INTERVAL
#define MYAM_KNAPSACK_SLOW_INTERVAL 10000
#endif

#ifndef MYAM_GREEDY_CANDIDATE_LIMIT
#define MYAM_GREEDY_CANDIDATE_LIMIT 2048
#endif

#ifndef MYAM_GREEDY_MIN_QUERIES
#define MYAM_GREEDY_MIN_QUERIES 5
#endif

#ifndef MYAM_GREEDY_BATCH_SIZE
#define MYAM_GREEDY_BATCH_SIZE 128
#endif

/* 查询频率时间衰减配置 */
#ifndef MYAM_DECAY_BASE
#define MYAM_DECAY_BASE 0.98  /* 每个查询间隔的衰减基数 */
#endif

/* 公开元数据哈希以供后台线程访问（同库内共享符号） */
extern HTAB *myam_meta_hash;

/* 后台优化相关常量 */
#define MYAM_BACKGROUND_OPTIMIZE_INTERVAL 10000  /* 每10000次查询触发一次后台优化 */
#define MYAM_BACKGROUND_OPTIMIZE_PENDING 1       /* 后台优化待执行状态 */
#define MYAM_BACKGROUND_OPTIMIZE_RUNNING 2       /* 后台优化执行中状态 */
#define MYAM_BACKGROUND_OPTIMIZE_IDLE    0       /* 后台优化空闲状态 */

/* 自适应范围扫描相关常量 */
#define MYAM_RANGE_SCAN_PURE_PDC    1    /* 纯PDC扫描策略 */
#define MYAM_RANGE_SCAN_HYBRID      2    /* PDC + B+树混合策略 */
#define MYAM_RANGE_SCAN_PURE_BTREE  3    /* 纯B+树扫描策略 */

#define MYAM_HOTNESS_SAMPLE_SIZE    20   /* 热度估算采样大小 */
#define MYAM_RANGE_STATS_WINDOW     100  /* 范围查询统计窗口大小 */

/* 多列键结构 */
#define MYAM_MAX_INDEX_COLS 32
typedef struct MyamKey
{
    int         nkeys;
    Datum       keys[MYAM_MAX_INDEX_COLS];
    bool        isnull[MYAM_MAX_INDEX_COLS];
} MyamKey;

/* MyAM Index Tuple (Serialized, Flat Bytes) */
typedef struct MyamIndexTupleData
{
    ItemPointerData t_tid;        /* Heap TID or BlockNumber (internal) */
    uint16          t_info;       /* Flags, Length? */
    /* 
     * Data follows:
     * - Null Bitmap (if any nullable columns)
     * - Key Data (flat bytes, no alignment padding between columns for compactness?)
     *   Actually, usually we want alignment.
     *   Let's assume simply concatenated datums.
     */
     char data[FLEXIBLE_ARRAY_MEMBER];
} MyamIndexTupleData;
typedef MyamIndexTupleData *MyamIndexTuple;

/* Page Opaque Data (Footer of Page) — stored on every disk page */
typedef struct MyamPageOpaqueData
{
    BlockNumber prev;       /* previous leaf block (leaf level only) */
    BlockNumber next;       /* next leaf block (leaf level only) */
    BlockNumber parent;     /* parent block (InvalidBlockNumber for root) */
    uint16      level;      /* 0 = leaf */
    uint16      flags;      /* MYAM_LEAF_PAGE / MYAM_INTERNAL_PAGE / etc. */
    uint16      max_off;    /* current number of items on page */
} MyamPageOpaqueData;
typedef MyamPageOpaqueData *MyamPageOpaque;

/* MyamPage is just the standard Page type (char * to 8 KB buffer) */
typedef Page MyamPage;

/*
 * Internal tuple: separator key + child block number.
 * child_blkno replaces the old in-memory child_page_ptr pointer.
 */
typedef struct MyamInternalTupleData
{
    BlockNumber      child_blkno;   /* block number of child page */
    uint16           t_info;        /* tuple size */
    char             data[FLEXIBLE_ARRAY_MEMBER];
} MyamInternalTupleData;
typedef MyamInternalTupleData *MyamInternalTuple;

/* In-memory page management (mock buffer manager) */
/* We need to allocate pages and return a "BlockNumber" or pointer. */
/* If we use pointers, we can cast them to BlockNumber if they fit (not on 64-bit). */
/* So we likely need to keep using pointers for in-memory, OR implement a simple array-based mapping. */
/* Let's use pointers for now, but typed as MyamPage* */

#define MyamPageGetOpaque(page) ((MyamPageOpaque) PageGetSpecialPointer(page))
#define MyamPageGetMaxOffset(page) (MyamPageGetOpaque(page)->max_off)
#define MyamPageIsLeaf(page) (MyamPageGetOpaque(page)->flags & MYAM_LEAF_PAGE)

/* 查询频率统计节点 */
typedef struct QueryStatsNode
{
    MyamKey     key;            /* 查询键值 (Multi-column) */
    int32       query_count;    /* 查询次数 */
    int64       last_query_seq; /* 上次查询序号（用于时间衰减） */
    int32       memory_cost;    /* 内存开销 */
    double      value_ratio;    /* 价值比率 = query_count / memory_cost */
    bool        in_cache;       /* 是否在哈希缓存中 */
    ItemPointerData tid;        /* 对应的TID */
    struct QueryStatsNode *next; /* 链表指针 */
} QueryStatsNode;

/* 元数据缓存条目（全局哈希：索引OID -> MyamMeta*） */
typedef struct MyamMetaCacheEntry
{
    Oid         key;    /* 索引OID作为哈希键 */
    struct MyamMeta *meta; /* 指向MyamMeta的指针 */
} MyamMetaCacheEntry;

/* 哈希缓存项 */
typedef struct HashCacheItem
{
    MyamKey     key;            /* 缓存键值 */
    ItemPointerData tid;        /* 对应的元组指针 */
    int32       access_count;   /* 访问计数 */
    int32       memory_cost;    /* 内存开销 */
    bool        is_hot;         /* 是否为热点数据 */
} HashCacheItem;

/* PDC entry state for open-addressing (linear-probe) hash table.
 * PDC_EMPTY (0) must be zero so MemoryContextAllocZero initialises correctly. */
typedef enum PDCEntryState
{
    PDC_EMPTY   = 0,    /* slot was never used — terminates probe chain */
    PDC_VALID   = 1,    /* slot holds a live entry */
    PDC_DELETED = 2,    /* tombstone: entry evicted, probe chain continues */
} PDCEntryState;

/* 直连页缓存（PDC）条目 — compact layout (~56 bytes vs. old ~328 bytes) */
typedef struct PDCEntry
{
    uint32          hash;           /* pre-computed key hash */
    BlockNumber     blkno;          /* 目标页块号（磁盘版） */
    ItemPointerData tid;            /* 目标 TID */
    OffsetNumber    off;            /* 页内偏移 */
    int32           access_count;   /* clock 淘汰使用计数 + 热度估算 */
    uint8           state;          /* PDCEntryState */
    uint8           key_size;       /* serialized key length in key_data */
    bool            is_hot;         /* 热点标记 */
    char            key_data[MYAM_PDC_KEY_INLINE_BYTES]; /* flat serialized key */
} PDCEntry;
/* ~56 bytes vs. old ~328 bytes (5.8x reduction) */

typedef struct PageDirectCache
{
    uint32      capacity;       /* 固定容量（2 的幂） */
    uint32      mask;           /* capacity - 1 */
    PDCEntry   *entries;        /* 对齐的缓存条目数组 */
    uint32      clock_hand;     /* clock 淘汰指针，O(1) 摊销淘汰 */
} PageDirectCache;

/* B+树节点结构 (Replaced by MyamPage) */
/*
typedef struct BTreeNode
{
    BlockNumber     block_num;
    bool            is_leaf;
    int32           item_count;
    MyamIndexTuple  items[MYAM_MAX_ITEMS_PER_PAGE];
    struct BTreeNode *children[MYAM_MAX_ITEMS_PER_PAGE + 1];
    struct BTreeNode *parent;
    struct BTreeNode *next;
    struct BTreeNode *prev;
} BTreeNode;
*/

/*
 * On-disk meta page (block 0).
 * Written once at index creation; updated on every structural change.
 * Fits inside the "special area" is too small, so we store it as the
 * sole item on block 0 using PageAddItem.
 */
typedef struct MyamDiskMetaData
{
    uint32      magic;          /* MYAM_PAGE_MAGIC */
    uint32      version;        /* format version, currently 1 */
    BlockNumber root_blkno;     /* current root block */
    int32       tree_height;    /* height (leaf = 1) */
    int32       total_pages;    /* total allocated pages */
    int32       total_tuples;   /* total live tuples */
} MyamDiskMetaData;
typedef MyamDiskMetaData *MyamDiskMeta;

#define MYAM_DISK_META_VERSION  1

/* 自适应范围扫描统计 */
typedef struct RangeScanStats
{
    int32       range_size;         /* 范围大小 */
    float       hotness;            /* 热度（0.0-1.0） */
    int32       strategy_used;      /* 使用的策略 */
    double      execution_time_us;  /* 执行时间（微秒） */
    int32       pdc_hits;           /* PDC命中次数 */
    int32       btree_accesses;     /* B+树访问次数 */
} RangeScanStats;

/* 自适应阈值 */
typedef struct AdaptiveThresholds
{
    int32       pdc_range_threshold;      /* PDC范围扫描阈值（初始100） */
    float       pdc_hotness_threshold;    /* PDC热度阈值（初始0.7） */
    int32       hybrid_range_threshold;   /* 混合策略范围阈值（初始500） */
    float       hybrid_hotness_threshold; /* 混合策略热度阈值（初始0.3） */

    /* 统计信息（用于自适应调整） */
    int32       total_range_queries;      /* 总范围查询数 */
    int32       pdc_strategy_wins;        /* PDC策略胜利次数 */
    int32       hybrid_strategy_wins;     /* 混合策略胜利次数 */
    int32       btree_strategy_wins;      /* B+树策略胜利次数 */

    /* 最近N次查询的统计（环形缓冲区） */
    RangeScanStats recent_stats[MYAM_RANGE_STATS_WINDOW];
    int32       stats_index;              /* 当前统计索引 */
} AdaptiveThresholds;

/* MyAM索引元数据（进程内缓存，不写磁盘） */
typedef struct MyamMeta
{
    /* 基本信息 */
    Oid             index_oid;      /* 索引OID */
    int             nkeys;          /* 键列数量 */
    Oid             key_type_oids[MYAM_MAX_INDEX_COLS];
    Oid             collation_oids[MYAM_MAX_INDEX_COLS];

    /* Type information */
    bool            key_byval[MYAM_MAX_INDEX_COLS];
    int16           key_typlen[MYAM_MAX_INDEX_COLS];
    int8            fast_cmp_type[MYAM_MAX_INDEX_COLS];

    bool            is_unique;

    /* Cached comparison functions */
    FmgrInfo        key_proc_infos[MYAM_MAX_INDEX_COLS];

    /* B+树磁盘位置（从 block 0 读取，缓存在此） */
    BlockNumber     root_blkno;     /* 根页块号 */
    int32           tree_height;
    int32           total_pages;
    int32           total_tuples;

    /* 直连页缓存（PDC） */
    PageDirectCache *pdc;
    int32           cache_capacity;
    int32           cache_size;
    int32           cache_hits;
    int32           cache_misses;

    /* 内存上下文（仅用于 PDC 和统计，不存 B+树页面） */
    MemoryContext   index_cxt;

    /* 查询统计 */
    HTAB           *query_stats;
    QueryStatsNode *stats_list;
    int32           total_queries;
    int32           hot_data_queries;

    /* 优化控制 */
    int32           knapsack_runs;
    double          optimization_ratio;
    int32           background_optimize_status;
    int32           queries_since_last_optimize;
    bool            optimization_needed;

    /* 性能统计 */
    double          avg_search_time;
    double          cache_hit_ratio;

    /* 自适应范围扫描 */
    AdaptiveThresholds *adaptive_thresholds;  /* 自适应阈值 */
    bool            enable_adaptive_range;     /* 是否启用自适应范围扫描 */
} MyamMeta;

/* 扫描状态结构 */
typedef struct MyamScanOpaqueData
{
    MyamMeta       *meta;           /* 索引元数据 */
    Relation        index;          /* 索引 relation（用于 ReadBuffer） */
    BlockNumber     current_blkno;  /* 当前扫描页块号 */
    Buffer          current_buf;    /* 当前持有的 buffer pin */
    OffsetNumber    current_off;    /* 当前页内偏移 */
    bool            cache_hit;      /* 是否缓存命中 */
    ScanKey         scan_keys;      /* 扫描键 */
    int32           num_keys;       /* 键数量 */
    bool            started;        /* 是否已开始扫描 */
    MyamKey         current_key;    /* 当前扫描到的键（范围扫描校验用） */

    /* 查询类型识别 */
    bool            is_point_query;   /* 是否是点查询（所有列等值） */
    bool            is_range_query;   /* 是否是范围查询 */

    /* 范围扫描统计 */
    int32           range_scan_strategy;  /* 当前使用的策略 */
    double          range_scan_start_time; /* 范围扫描开始时间 */
    int32           range_pdc_hits;       /* 本次范围扫描PDC命中数 */
    int32           range_btree_accesses; /* 本次范围扫描B+树访问数 */
} MyamScanOpaqueData;

typedef MyamScanOpaqueData *MyamScanOpaque;

/* 函数声明 */

/* 访问方法接口 */
extern Datum myam_handler(PG_FUNCTION_ARGS);
extern IndexBuildResult *myam_build(Relation heap, Relation index, struct IndexInfo *indexInfo);
extern bool myam_insert(Relation index, Datum *values, bool *isnull, ItemPointer heap_tid,
                        Relation heapRel, IndexUniqueCheck checkUnique, bool indexUnchanged,
                        struct IndexInfo *indexInfo);
extern IndexScanDesc myam_beginscan(Relation indexRel, int nkeys, int norderbys);
extern void myam_rescan(IndexScanDesc scan, ScanKey scankey, int nscankeys,
                       ScanKey orderbys, int norderbys);
extern bool myam_gettuple(IndexScanDesc scan, ScanDirection dir);
extern int64 myam_getbitmap(IndexScanDesc scan, TIDBitmap *bitmap);
extern void myam_endscan(IndexScanDesc scan);
extern void myam_costestimate(PlannerInfo *root, IndexPath *path, double loop_count,
                             Cost *indexStartupCost, Cost *indexTotalCost,
                             Selectivity *indexSelectivity, double *indexCorrelation,
                             double *indexPages);

/* B+树操作（磁盘版，返回 BlockNumber） */
extern Buffer myam_page_new(Relation index, MyamMeta *meta, uint16 flags);
extern bool myam_page_insert_tuple(MyamMeta *meta, MyamPage page, MyamIndexTuple tuple,
                                   OffsetNumber offset, OffsetNumber *inserted_off);
extern MyamIndexTuple myam_form_tuple(MyamMeta *meta, MyamKey *key, ItemPointer tid);
extern void myam_deform_tuple(MyamMeta *meta, MyamIndexTuple tuple, MyamKey *key);
extern int myam_compare_tuple_key(MyamMeta *meta, MyamIndexTuple tuple, MyamKey *key);
extern int myam_compare_tuple_key_direct(MyamMeta *meta, MyamIndexTuple tuple, MyamKey *key);
extern void myam_bulk_build(MyamMeta *meta, Relation heap, Relation index,
                            struct IndexInfo *indexInfo, IndexBuildResult *result);

/* 磁盘 Meta 页操作 */
extern void myam_init_meta_page(Relation index, MyamMeta *meta);
extern void myam_write_meta_page(Relation index, MyamMeta *meta);
extern void myam_read_meta_page(Relation index, MyamMeta *meta);

/* Old functions removed/replaced */
/* extern BTreeNode *myam_btree_create_node(MyamMeta *meta, bool is_leaf); */
/* extern void myam_btree_insert(MyamMeta *meta, MyamKey *key, ItemPointer tid); */
/* extern void myam_btree_split_node(MyamMeta *meta, BTreeNode *node); */
/* extern BTreeNode *myam_btree_find_leaf(MyamMeta *meta, BTreeNode *root, MyamKey *key); */

/* 缓存管理（page 参数改为 BlockNumber） */
extern void myam_cache_init(MyamMeta *meta);
extern bool myam_cache_search(MyamMeta *meta, MyamKey *key, ItemPointer result_tid);
extern void myam_cache_insert(MyamMeta *meta, MyamKey *key, BlockNumber blkno,
                              OffsetNumber off, ItemPointer tid, bool is_hot);
extern void myam_cache_remove(MyamMeta *meta, MyamKey *key);
extern void myam_cache_remove_tid(MyamMeta *meta, ItemPointer tid);
extern void myam_cache_sync_on_insert(MyamMeta *meta, MyamKey *key, ItemPointer tid);
extern void myam_cache_evict_low_value(MyamMeta *meta);
extern void myam_cache_update_stats(MyamMeta *meta, MyamKey *key, bool hit);

/* 查询统计 */
extern void myam_stats_update(MyamMeta *meta, MyamKey *key, bool is_hot);
extern void myam_stats_update_tid(MyamMeta *meta, MyamKey *key, ItemPointer tid);
extern QueryStatsNode *myam_stats_get_node(MyamMeta *meta, MyamKey *key);
extern void myam_stats_cleanup(MyamMeta *meta);

/* 自适应范围扫描 */
extern void myam_adaptive_init(MyamMeta *meta);
extern float myam_estimate_hotness(MyamMeta *meta, MyamKey *start_key, MyamKey *end_key, int32 range_size);
extern int32 myam_choose_range_strategy(MyamMeta *meta, int32 range_size, float hotness);
extern void myam_adaptive_update_stats(MyamMeta *meta, RangeScanStats *stats);
extern void myam_adaptive_adjust_thresholds(MyamMeta *meta);
extern int32 myam_estimate_range_size(IndexScanDesc scan);
extern bool myam_is_simple_int4_range(MyamMeta *meta, IndexScanDesc scan,
                                      int32 *lower, int32 *upper);


/* 0-1背包优化函数 */
extern void myam_knapsack_optimize(MyamMeta *meta);
extern double myam_knapsack_solve(QueryStatsNode **candidates, int candidate_count, 
                                 int capacity, bool **selected);
extern void myam_knapsack_apply_solution(MyamMeta *meta, QueryStatsNode **candidates,
                                        bool **selected, int candidate_count, int capacity);

/* 贪心优化（Top-K） */
extern void myam_greedy_optimize(MyamMeta *meta);

/* Predictor 层：决策缓存与策略 */
extern bool myam_predictor_should_cache(MyamMeta *meta, MyamKey *key);
extern void myam_predictor_on_access(MyamMeta *meta, MyamKey *key, bool hit);

/* PDC 原语（page 字段改为 BlockNumber） */
extern void myam_pdc_init(MyamMeta *meta);
extern bool myam_pdc_lookup(MyamMeta *meta, MyamKey *key, ItemPointer result_tid);
extern void myam_pdc_insert(MyamMeta *meta, MyamKey *key, BlockNumber blkno,
                            OffsetNumber off, ItemPointer tid, bool is_hot);
extern void myam_pdc_remove(MyamMeta *meta, MyamKey *key);
extern void myam_pdc_remove_tid(MyamMeta *meta, ItemPointer tid);

/* 后台优化相关函数 */
extern void myam_background_optimize_init(MyamMeta *meta);
extern void myam_background_optimize_check(MyamMeta *meta);
extern void myam_background_optimize_trigger(MyamMeta *meta);
extern bool myam_background_optimize_try_run(MyamMeta *meta);

/* BackgroundWorker相关函数 */
extern void myam_bgworker_main(Datum main_arg);
extern void myam_register_bgworker(void);
extern void _PG_init(void);

/* VACUUM 删除入口（同步清理缓存） */
extern IndexBulkDeleteResult *myam_bulkdelete(IndexVacuumInfo *info,
                                             IndexBulkDeleteResult *stats,
                                             IndexBulkDeleteCallback callback,
                                             void *callback_state);

/* 元数据管理 */
extern MyamMeta *myam_meta_create(Relation index);
extern MyamMeta *myam_meta_get(Relation index);
extern void myam_meta_update(MyamMeta *meta);
extern void myam_meta_destroy(MyamMeta *meta);

/* 工具函数 */
extern int myam_compare_datums(Datum a, Datum b, Oid type_oid);
extern bool myam_datum_equal(Datum a, Datum b, Oid type_oid);
extern Size myam_datum_size(Datum datum, Oid type_oid);
extern int myam_key_compare(MyamMeta *meta, MyamKey *a, MyamKey *b);
extern void myam_key_copy(MyamMeta *meta, MyamKey *dest, MyamKey *src);
extern void myam_key_free(MyamMeta *meta, MyamKey *key);

#endif /* ACCESS_MYAM_H */
