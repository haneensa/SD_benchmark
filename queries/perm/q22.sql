CREATE TABLE lineage AS (
  select Qbase.*, Qplus.*
  from (
      SELECT cntrycode, count(*) AS numcust, sum(c_acctbal) AS totacctbal
      FROM (
          SELECT substring(c_phone FROM 1 FOR 2) AS cntrycode,  c_acctbal
          FROM customer
          WHERE
              substring(c_phone FROM 1 FOR 2) IN ('13', '31', '23', '29', '30', '18', '17')
              AND c_acctbal > (
                  SELECT avg(c_acctbal)
                  FROM customer
                  WHERE c_acctbal > 0.00
                      AND substring(c_phone FROM 1 FOR 2) IN ('13', '31', '23', '29', '30', '18', '17'
                  ))
              AND NOT EXISTS (
                      SELECT *
                      FROM orders
                      WHERE o_custkey = c_custkey)
              ) AS custsale
      GROUP BY cntrycode
      ORDER BY cntrycode
  ) as Qbase join (
    SELECT * 
    FROM  (
      SELECT *, substring(c_phone FROM 1 FOR 2) AS cntrycode,  c_acctbal,
      customer.rowid as c_rid1
      FROM customer
    ) as in_plus,
    ( select customer.rowid as c_rid2
      from customer
    ) as cb_sub1
    WHERE substring(in_plus.c_phone FROM 1 FOR 2) IN ('13', '31', '23', '29', '30', '18', '17')
              AND in_plus.c_acctbal > (
                  SELECT avg(c_acctbal)
                  FROM customer
                  WHERE c_acctbal > 0.00
                      AND substring(c_phone FROM 1 FOR 2) IN ('13', '31', '23', '29', '30', '18', '17'
                  ))
              AND NOT EXISTS (
                      SELECT *
                      FROM orders
                      WHERE o_custkey = in_plus.c_custkey)
          AND EXISTS (select * from 
            ( select * from
                 ( SELECT avg(c_acctbal) as avg_c_acctbal FROM customer
                  WHERE c_acctbal > 0.00 AND substring(c_phone FROM 1 FOR 2) IN ('13', '31', '23', '29', '30', '18', '17')
                ), (select *, customer.rowid as c_rid3 from customer 
                  WHERE c_acctbal > 0.00 AND substring(c_phone FROM 1 FOR 2) IN ('13', '31', '23', '29', '30', '18', '17')
                )
              ) as Qsub_plus1
              where 
              Qsub_plus1.c_rid3=cb_sub1.c_rid2
              and in_plus.c_acctbal > Qsub_plus1.avg_c_acctbal
            )
  ) as Qplus using (cntrycode)
)
          AND EXISTS (select * from 
            (select *, orders.rowid as o_rid2 from orders where o_custkey=in_plus.c_custkey) as Qsub_plus2
            where 
            Qsub_plus2.o_custkey = cb_sub2.o_custkey
            and Qsub_plus2.o_rid2 = cb_sub2.o_rid
          )
    ,(
      select *, orders.rowid as o_rid
      from orders
    ) as cb_sub2
