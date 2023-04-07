create table lineage as (
SELECT CB.*, p_rid1, ps_rid1,s_rid1, n_rid1, r_rid1, s_nationkey, n_nationkey, s_comment
FROM (
    SELECT  part.rowid as p_rid1, 
            supplier.rowid as s_rid1,
            partsupp.rowid as ps_rid1,
            nation.rowid as n_rid1,
            region.rowid as r_rid1,
            ps_partkey, s_suppkey, ps_suppkey,
            ps_supplycost as qplus_ps_supplycost,
            p_size, p_type, s_nationkey, n_nationkey, n_regionkey, r_regionkey,
            r_name, 
            p_partkey, s_comment
    FROM part, supplier, partsupp, nation, region
    WHERE p_partkey = ps_partkey AND s_suppkey = ps_suppkey AND p_size = 15
    AND p_type LIKE '%BRASS' AND s_nationkey = n_nationkey
    AND n_regionkey = r_regionkey AND r_name = 'EUROPE'
    AND ps_supplycost = (
        SELECT min(ps_supplycost)
        FROM partsupp,supplier, nation, region
        WHERE  p_partkey = ps_partkey AND s_suppkey = ps_suppkey
            AND s_nationkey = n_nationkey AND n_regionkey = r_regionkey AND r_name = 'EUROPE'
          )
    ORDER BY
        s_acctbal DESC,
        n_name,
        s_name,
        p_partkey
    LIMIT 100
) AS qplus,
(
    SELECT partsupp.rowid as ps_rid2, 
           supplier.rowid as s_rid2,
           nation.rowid as n_rid2, 
          region.rowid as r_rid2, 
          n_nationkey as CB_n_nationkey, 
          s_nationkey as CB_s_nationkey,
          ps_partkey as CB_ps_partkey,
          s_suppkey as CB_s_suppkey,
          ps_suppkey as CB_ps_suppkey
    FROM partsupp, supplier, nation, region
    WHERE  r_name = 'EUROPE'
) AS CB
WHERE
    EXISTS (
      SELECT *
      FROM ( select t1.*, t2.*
        from
        (SELECT min(ps_supplycost)  AS min_ps_supplycost 
          FROM partsupp, supplier, nation, region
          WHERE qplus.p_partkey = ps_partkey AND s_suppkey = ps_suppkey AND s_nationkey = n_nationkey
              AND n_regionkey = r_regionkey AND r_name = 'EUROPE') as t1,
        (SELECT ps_partkey as X_ps_partkey, s_suppkey as X_s_suppkey, s_nationkey as X_s_nationkey,
          n_nationkey as X_n_nationkey, ps_suppkey as X_ps_suppkey
          FROM partsupp, supplier, nation, region
          WHERE qplus.p_partkey = ps_partkey AND s_suppkey = ps_suppkey AND s_nationkey = n_nationkey
              AND n_regionkey = r_regionkey AND r_name = 'EUROPE' ) as t2
          ) as Qsub_plus
      WHERE  
      Qsub_plus.X_ps_partkey=CB.CB_ps_partkey
      AND Qsub_plus.X_s_suppkey=CB.CB_s_suppkey
      AND Qsub_plus.X_ps_suppkey=CB.CB_ps_suppkey
      AND Qsub_plus.X_s_nationkey=CB.CB_s_nationkey
      AND Qsub_plus.X_n_nationkey=CB.CB_n_nationkey
      AND (qplus.qplus_ps_supplycost = (
        SELECT min(ps_supplycost)
        FROM partsupp,supplier, nation, region
        WHERE  qplus.p_partkey = ps_partkey AND s_suppkey = ps_suppkey
            AND s_nationkey = n_nationkey AND n_regionkey = r_regionkey AND r_name = 'EUROPE'
          )
      OR NOT qplus.qplus_ps_supplycost=min_ps_supplycost)
    )
)
