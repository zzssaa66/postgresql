/*
 * MyAM Background Worker Implementation
 * 
 * 这个文件实现了MyAM的BackgroundWorker功能，作为独立的共享库加载
 */

#include "postgres.h"
#include "fmgr.h"
#include "miscadmin.h"
#include "postmaster/bgworker.h"
#include "postmaster/postmaster.h"
#include "storage/latch.h"
#include "storage/lwlock.h"
#include "storage/proc.h"
#include "storage/ipc.h"
#include "storage/procsignal.h"
#include "tcop/tcopprot.h"
#include "utils/ps_status.h"
#include "utils/hsearch.h"
#include "access/hash.h"
#include "pgstat.h"
#include "myam.h"
#include <time.h>

/* 全局变量 */
static volatile sig_atomic_t got_sigterm = false;


/* 外部声明：在 myam_am.c 中创建的全局索引元数据哈希 */
extern HTAB *myam_meta_hash;

/* 函数声明 */
void myam_register_bgworker(void);
void PGDLLEXPORT myam_bgworker_main(Datum main_arg);
static void myam_sigterm_handler(SIGNAL_ARGS);

/*
 * 注册MyAM后台优化Worker
 */
void
myam_register_bgworker(void)
{
    BackgroundWorker worker;
    
    elog(LOG, "MyAM: Starting BackgroundWorker registration");
    
    /* 初始化worker结构 */
    memset(&worker, 0, sizeof(BackgroundWorker));
    
    /* 设置worker名称和类型 */
    snprintf(worker.bgw_name, BGW_MAXLEN, "MyAM Background Optimizer");
    snprintf(worker.bgw_type, BGW_MAXLEN, "myam_optimizer");
    
    /* 设置worker标志 */
    worker.bgw_flags = BGWORKER_SHMEM_ACCESS | BGWORKER_BACKEND_DATABASE_CONNECTION;
    
    /* 设置启动时间 - 在数据库一致性状态后启动 */
    worker.bgw_start_time = BgWorkerStart_ConsistentState;
    
    /* 设置重启间隔 - 60秒后重启 */
    worker.bgw_restart_time = 60;
    
    /* 设置库名和函数名 */
    snprintf(worker.bgw_library_name, MAXPGPATH, "myam");
    snprintf(worker.bgw_function_name, BGW_MAXLEN, "myam_bgworker_main");
    
    /* 设置主参数 */
    worker.bgw_main_arg = (Datum) 0;
    
    elog(LOG, "MyAM: Calling RegisterBackgroundWorker with name='%s', library='%s', function='%s'",
         worker.bgw_name, worker.bgw_library_name, worker.bgw_function_name);
    
    /* 注册worker */
    RegisterBackgroundWorker(&worker);
    
    elog(LOG, "MyAM: RegisterBackgroundWorker call completed");
}

/*
 * 信号处理函数
 */
static void
myam_sigterm_handler(SIGNAL_ARGS)
{
    int save_errno = errno;
    got_sigterm = true;
    SetLatch(MyLatch);
    errno = save_errno;
}

/*
 * BackgroundWorker主循环函数
 */
PGDLLEXPORT void
myam_bgworker_main(Datum main_arg)
{
    HASH_SEQ_STATUS seq_status;
    MyamMetaCacheEntry *entry;
    
    elog(LOG, "MyAM BackgroundWorker: Starting main function");
    
    /* 设置信号处理 */
    pqsignal(SIGTERM, myam_sigterm_handler);
    BackgroundWorkerUnblockSignals();
    
    /* 设置进程标题 */
    set_ps_display("MyAM Background Optimizer");
    
    elog(LOG, "MyAM BackgroundWorker: Initializing database connection");
    
    /* 初始化数据库连接 */
    BackgroundWorkerInitializeConnection("postgres", NULL, 0);
    
    elog(LOG, "MyAM BackgroundWorker: Database connection established, entering main loop");
    
    /* 主循环 */
    while (!got_sigterm)
    {
        int rc;
        
        /* 等待30秒或直到收到信号 */
        rc = WaitLatch(MyLatch,
                      WL_LATCH_SET | WL_TIMEOUT | WL_EXIT_ON_PM_DEATH,
                      30000L,
                      0);
        
        ResetLatch(MyLatch);
        
        /* 检查postmaster是否死亡 */
        if (rc & WL_EXIT_ON_PM_DEATH)
            proc_exit(1);
        
        /* 检查是否收到终止信号 */
        if (got_sigterm)
            break;
        
        elog(LOG, "MyAM BackgroundWorker: Performing optimization check");
        
        /* 如果myam_meta_hash不存在，跳过这次循环 */
        if (!myam_meta_hash)
        {
            elog(DEBUG1, "MyAM BackgroundWorker: myam_meta_hash not initialized, skipping");
            continue;
        }
        
        /* 遍历所有MyAM索引的元数据（键为索引OID，值为 MyamMeta*） */
        hash_seq_init(&seq_status, myam_meta_hash);
        while ((entry = (MyamMetaCacheEntry *) hash_seq_search(&seq_status)) != NULL)
        {
            MyamMeta *meta = entry->meta;
            if (meta == NULL)
                continue;

            /* 快速优化：在达到查询阈值且标记需要优化时进行贪心释放 */
            if (meta->optimization_needed &&
                meta->queries_since_last_optimize >= MYAM_GREEDY_RUN_THRESHOLD)
            {
                elog(LOG, "MyAM BackgroundWorker: Greedy optimize index %u (queries_since=%d)",
                     entry->key, meta->queries_since_last_optimize);
                myam_greedy_optimize(meta);
                meta->queries_since_last_optimize = 0;
                meta->optimization_needed = false;
            }

            /* 慢周期背包重构：按配置的慢周期进行重构，提升整体命中 */
            if (meta->total_queries > 0 &&
                (meta->total_queries % MYAM_KNAPSACK_SLOW_INTERVAL) == 0)
            {
                elog(LOG, "MyAM BackgroundWorker: Knapsack optimize index %u (total_queries=%d)",
                     entry->key, meta->total_queries);
                myam_knapsack_optimize(meta);
            }
        }
    }
    
    elog(LOG, "MyAM BackgroundWorker: Exiting main loop");
    proc_exit(0);
}
