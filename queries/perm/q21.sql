CREATE TABLE lineage as (
  select *
  from (
      SELECT s_name, count(*) AS numwait
      FROM (
          SELECT s_name, s_suppkey, o_orderkey
          FROM supplier, lineitem l1, orders, nation
          WHERE s_suppkey = l1.l_suppkey AND o_orderkey = l1.l_orderkey
              AND o_orderstatus = 'F' AND l1.l_receiptdate > l1.l_commitdate
              AND EXISTS (SELECT * FROM lineitem l2 WHERE l2.l_orderkey = l1.l_orderkey AND l2.l_suppkey <> l1.l_suppkey)
              AND NOT EXISTS (SELECT * FROM lineitem l3  WHERE   l3.l_orderkey = l1.l_orderkey AND l3.l_suppkey <> l1.l_suppkey AND l3.l_receiptdate > l3.l_commitdate)
              AND s_nationkey = n_nationkey
              AND n_name = 'SAUDI ARABIA'
      )
      GROUP BY s_name
      ORDER BY numwait DESC, s_name
      LIMIT 100
    ) as qbase join (
    select * from (
    SELECT *, supplier.rowid as s_rid, l1.rowid as l_rid,
           orders.rowid as o_rid, nation.rowid as n_rid,
          s_name, l_orderkey, l_suppkey, l_commitdate
    FROM supplier, lineitem l1, orders, nation
    WHERE     s_suppkey = l1.l_suppkey AND o_orderkey = l1.l_orderkey
        AND o_orderstatus = 'F' AND l1.l_receiptdate > l1.l_commitdate
        AND EXISTS (SELECT * FROM lineitem l2 WHERE l2.l_orderkey = l1.l_orderkey AND l2.l_suppkey <> l1.l_suppkey)
        AND NOT EXISTS (SELECT * FROM lineitem l3  WHERE   l3.l_orderkey = l1.l_orderkey AND l3.l_suppkey <> l1.l_suppkey AND l3.l_receiptdate > l3.l_commitdate)
        AND s_nationkey = n_nationkey
        AND n_name = 'SAUDI ARABIA'
      ) as in_plus,
       lineitem  as cb_sub1 
      where in_plus.s_suppkey = in_plus.l_suppkey AND in_plus.o_orderkey = in_plus.l_orderkey
        AND in_plus.o_orderstatus = 'F' AND in_plus.l_receiptdate > in_plus.l_commitdate
        AND EXISTS (SELECT * FROM lineitem l2 WHERE l2.l_orderkey = in_plus.l_orderkey AND l2.l_suppkey <> in_plus.l_suppkey)
        AND NOT EXISTS (SELECT * FROM lineitem l3  WHERE   l3.l_orderkey = in_plus.l_orderkey AND l3.l_suppkey <> in_plus.l_suppkey AND l3.l_receiptdate > l3.l_commitdate)
        AND in_plus.s_nationkey = in_plus.n_nationkey
        AND in_plus.n_name = 'SAUDI ARABIA'
        AND EXISTS ( select * from lineitem as l2
          where
          l2.l_orderkey=in_plus.l_orderkey and
          l2.l_suppkey <> in_plus.l_suppkey 
          and cb_sub1.rowid=l2.rowid
        )
  )  using (s_name)
)
