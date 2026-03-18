
/*
 * Inline fast column comparison.
 *
 * For the four common scalar types (int4/int8/float4/float8) we compare
 * directly, bypassing the fmgr call overhead (~40–100 ns per call on a modern
 * CPU).  All other types fall back to FunctionCall2Coll as before.
 *
 * Returns negative, zero, or positive like a standard comparator.
 */
static inline int32
myam_cmp_col(MyamMeta *meta, int col, Datum a, Datum b)
{
    switch (meta->fast_cmp_type[col])
    {
        case MYAM_CMP_INT4:
        {
            int32 av = DatumGetInt32(a);
            int32 bv = DatumGetInt32(b);
            return (av < bv) ? -1 : (av > bv) ? 1 : 0;
        }
        case MYAM_CMP_INT8:
        {
            int64 av = DatumGetInt64(a);
            int64 bv = DatumGetInt64(b);
            return (av < bv) ? -1 : (av > bv) ? 1 : 0;
        }
        case MYAM_CMP_FLOAT4:
        {
            float4 av = DatumGetFloat4(a);
            float4 bv = DatumGetFloat4(b);
            return (av < bv) ? -1 : (av > bv) ? 1 : 0;
        }
        case MYAM_CMP_FLOAT8:
        {
            float8 av = DatumGetFloat8(a);
            float8 bv = DatumGetFloat8(b);
            return (av < bv) ? -1 : (av > bv) ? 1 : 0;
        }
        default:
            return DatumGetInt32(FunctionCall2Coll(&meta->key_proc_infos[col],
                                                   meta->collation_oids[col],
                                                   a, b));
    }
}

/*
 * Form a MyamIndexTuple from MyamKey
 */
MyamIndexTuple
myam_form_tuple(MyamMeta *meta, MyamKey *key, ItemPointer tid)
{
    /* Calculate size */
    Size total_size = offsetof(MyamIndexTupleData, data);
    Size data_size = 0;
    int i;
    
    /* Null bitmap size? 
       For simplicity, let's store nulls as a byte array or just bitmap.
       If nkeys <= 8, 1 byte.
       Let's use (nkeys + 7) / 8 bytes.
    */
    int null_bitmap_len = (meta->nkeys + 7) / 8;
    total_size += null_bitmap_len;
    
    for (i = 0; i < meta->nkeys; i++)
    {
        if (!key->isnull[i])
        {
            if (meta->key_typlen[i] == -1) /* varlena */
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
                data_size += strlen(DatumGetCString(key->keys[i])) + 1; /* CString fallback? */
        }
    }
    
    total_size += data_size;

    /*
     * 分配在当前内存上下文（而非永久的 index_cxt）。
     * PageAddItem 会把数据拷贝进 page，所以 tuple 仅在
     * 调用方持有期间有效，调用方负责在 PageAddItem 后 pfree。
     */
    MyamIndexTuple tuple = (MyamIndexTuple) palloc(total_size);
    tuple->t_tid = *tid;
    tuple->t_info = total_size; /* Store size in info for now */
    
    char *ptr = tuple->data;
    
    /* Write null bitmap */
    memset(ptr, 0, null_bitmap_len);
    for (i = 0; i < meta->nkeys; i++)
    {
        if (key->isnull[i])
            ptr[i/8] |= (1 << (i%8));
    }
    ptr += null_bitmap_len;
    
    /* Write data */
    for (i = 0; i < meta->nkeys; i++)
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
                    data_ptr = (char *) &key->keys[i]; /* Valid for int/float/etc */
                else
                    data_ptr = DatumGetPointer(key->keys[i]);
                
                memcpy(ptr, data_ptr, len);
                ptr += len;
            }
        }
    }
    
    return tuple;
}

/*
 * Deform tuple to key
 */
void
myam_deform_tuple(MyamMeta *meta, MyamIndexTuple tuple, MyamKey *key)
{
    char *ptr = tuple->data;
    int null_bitmap_len = (meta->nkeys + 7) / 8;
    int i;
    
    key->nkeys = meta->nkeys;
    
    /* Read null bitmap */
    for (i = 0; i < meta->nkeys; i++)
    {
        if (ptr[i/8] & (1 << (i%8)))
            key->isnull[i] = true;
        else
            key->isnull[i] = false;
    }
    ptr += null_bitmap_len;
    
    /* Read data */
    for (i = 0; i < meta->nkeys; i++)
    {
        if (!key->isnull[i])
        {
            if (meta->key_typlen[i] == -1)
            {
                /* Varlena: need to read length from data */
                /* Assuming it's stored as varlena (with header) */
                struct varlena *v = (struct varlena *) ptr;
                Size len = VARSIZE_ANY(v);
                key->keys[i] = PointerGetDatum(v);
                ptr += len;
            }
            else
            {
                Size len = meta->key_typlen[i];
                if (meta->key_byval[i])
                {
                    Datum d;
                    memcpy(&d, ptr, len);
                    key->keys[i] = d;
                }
                else
                {
                    key->keys[i] = PointerGetDatum(ptr);
                }
                ptr += len;
            }
        }
        else
        {
            key->keys[i] = (Datum) 0;
        }
    }
}

/*
 * Compare Tuple with Key
 */
int
myam_compare_tuple_key(MyamMeta *meta, MyamIndexTuple tuple, MyamKey *key)
{
    return myam_compare_tuple_key_direct(meta, tuple, key);
}

int
myam_compare_tuple_key_direct(MyamMeta *meta, MyamIndexTuple tuple, MyamKey *key)
{
    const char *ptr;
    int null_bitmap_len = (meta->nkeys + 7) / 8;
    int i;

    ptr = tuple->data + null_bitmap_len;

    for (i = 0; i < meta->nkeys; i++)
    {
        bool tuple_null = (tuple->data[i/8] & (1 << (i%8))) != 0;
        bool key_null = key->isnull[i];

        if (tuple_null && key_null)
            continue;
        if (tuple_null)
            return 1;
        if (key_null)
            return -1;

        Datum tuple_val = (Datum) 0;
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

/*
 * 比较两个多列键
 */
int
myam_key_compare(MyamMeta *meta, MyamKey *a, MyamKey *b)
{
    int i;
    for (i = 0; i < meta->nkeys; i++)
    {
        /* 处理 NULL 值：NULLs last */
        if (a->isnull[i] && b->isnull[i])
            continue;
        if (a->isnull[i])
            return 1;
        if (b->isnull[i])
            return -1;
            
        Datum da = a->keys[i];
        Datum db = b->keys[i];

        int32 cmp = myam_cmp_col(meta, i, da, db);
        if (cmp != 0)
            return cmp;
    }
    return 0;
}

/*
 * 复制多列键 (Deep Copy)
 */
void
myam_key_copy(MyamMeta *meta, MyamKey *dest, MyamKey *src)
{
    int i;
    dest->nkeys = src->nkeys;
    for (i = 0; i < src->nkeys; i++)
    {
        dest->isnull[i] = src->isnull[i];

        if (!dest->isnull[i])
        {
            if (meta->key_byval[i] && meta->key_typlen[i] != -1)
            {
                /*
                 * Pass-by-value（int4/int8 等）：直接赋值，
                 * 无需 MemoryContextSwitchTo 和 datumCopy 的额外开销。
                 */
                dest->keys[i] = src->keys[i];
            }
            else
            {
                /* pass-by-reference 类型需要深拷贝 */
                MemoryContext oldcxt = MemoryContextSwitchTo(
                    meta->index_cxt ? meta->index_cxt : CurrentMemoryContext);

                if (meta->key_typlen[i] == -1) /* varlena */
                    dest->keys[i] = PointerGetDatum(PG_DETOAST_DATUM_COPY(src->keys[i]));
                else
                    dest->keys[i] = datumCopy(src->keys[i],
                                              meta->key_byval[i],
                                              meta->key_typlen[i]);

                MemoryContextSwitchTo(oldcxt);
            }
        }
        else
        {
            dest->keys[i] = (Datum) 0;
        }
    }
}

/*
 * 释放多列键 (Deep Free for pass-by-ref types)
 */
void
myam_key_free(MyamMeta *meta, MyamKey *key)
{
    int i;
    for (i = 0; i < key->nkeys; i++)
    {
        if (!key->isnull[i])
        {
            if (!meta->key_byval[i])
            {
                /* pass-by-ref Datum is a pointer */
                /* datumCopy allocates memory for it. We should free it if we are destroying the key. */
                /* However, standard pfree checks might be tricky if it's not a simple pointer */
                /* Usually DatumGetPointer(d) is palloc'ed chunk */
                /* Be careful not to double free if we copied shallowly before. Now we deep copy. */
                pfree(DatumGetPointer(key->keys[i]));
            }
        }
    }
}
