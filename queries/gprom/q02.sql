create table lineage as (
  select joins2.*, ps_rid2, s_rid2, n_rid2, r_rid2
  from (SELECT 
    partsupp.rowid as ps_rid2,
    supplier.rowid as s_rid2,
    nation.rowid as n_rid2,
    region.rowid as r_rid2,
    min(ps_supplycost) over (partition by ps_partkey) as min_ps_supplycost
    FROM partsupp, supplier, nation, region
    WHERE s_suppkey = ps_suppkey AND s_nationkey = n_nationkey AND n_regionkey = r_regionkey AND r_name = 'EUROPE'
  ) as group1 join (
    SELECT part.rowid as p_rid,
          supplier.rowid as s_rid,
          partsupp.rowid as ps_rid,
          nation.rowid as n_rid,
          region.rowid as r_rid,
        ps_supplycost,s_acctbal, s_name, n_name, p_partkey, p_mfgr, s_address, s_phone, s_comment
    FROM part, supplier, partsupp, nation, region
    WHERE p_partkey = partsupp.ps_partkey
        AND s_suppkey = partsupp.ps_suppkey AND p_size = 15
        AND p_type LIKE '%BRASS' AND s_nationkey = n_nationkey
        AND n_regionkey = r_regionkey AND r_name = 'EUROPE'
      AND ps_supplycost = (
          SELECT min(ps_supplycost)
          FROM partsupp, supplier, nation, region
          WHERE p_partkey = ps_partkey AND s_suppkey = ps_suppkey AND s_nationkey = n_nationkey
              AND n_regionkey = r_regionkey AND r_name = 'EUROPE')
    ORDER BY s_acctbal DESC, n_name, s_name, p_partkey
  ) as joins2 on (group1.min_ps_supplycost=joins2.ps_supplycost)
)
