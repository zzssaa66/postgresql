
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
    
    tuplestore_donestoring(tupstore);
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
