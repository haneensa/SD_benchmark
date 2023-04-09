create table lineage as (
  select main.*, subq.* from (
      select groups.*,
      j1.partsupp_rowid as ps_rid, j1.supplier_rowid as s_rid, j1.nation_rowid as n_rid
      from (
        SELECT ps_partkey, sum(ps_supplycost * ps_availqty) AS value
        FROM (
          SELECT ps_partkey, ps_supplycost, ps_availqty
          FROM partsupp, supplier, nation
          WHERE ps_suppkey = s_suppkey AND s_nationkey = n_nationkey AND n_name = 'GERMANY'
        )
        GROUP BY ps_partkey
          HAVING
              sum(ps_supplycost * ps_availqty) > (
                  SELECT
                      sum(ps_supplycost * ps_availqty) * 0.000100000
                  FROM
                      partsupp,
                      supplier,
                      nation
                  WHERE
                      ps_suppkey = s_suppkey
                      AND s_nationkey = n_nationkey
                      AND n_name = 'GERMANY')
        ORDER BY value DESC
      ) as groups join (
        SELECT partsupp.rowid as partsupp_rowid, supplier.rowid as supplier_rowid, nation.rowid as nation_rowid,
               ps_partkey, ps_supplycost, ps_availqty
        FROM partsupp, supplier, nation
        WHERE ps_suppkey = s_suppkey AND s_nationkey = n_nationkey AND n_name = 'GERMANY'
      ) as j1 using (ps_partkey)
    ) as main, (select * from
        (SELECT sum(ps_supplycost * ps_availqty) * 0.000100000 as value_inner
          FROM partsupp, supplier, nation WHERE ps_suppkey = s_suppkey AND s_nationkey = n_nationkey AND n_name = 'GERMANY'),
        (SELECT partsupp.rowid as ps_rid2, supplier.rowid as s_rid2, nation.rowid as n_rid2
        FROM partsupp, supplier, nation
        WHERE ps_suppkey = s_suppkey AND s_nationkey = n_nationkey AND n_name = 'GERMANY')
    ) as subq
)
