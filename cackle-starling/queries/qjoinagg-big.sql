select
        l_shipmode,
        count(*) as cnt,
        sum(l_orderkey),
        sum(l_partkey),
        sum(l_suppkey),
        sum(l_linenumber),
        sum(l_quantity),
        sum(l_extendedprice),
        sum(l_discount),
        sum(l_tax),
        min(l_shipdate),
        min(l_commitdate),
        min(l_receiptdate)
from
        lineitem,
        orders
where
        l_receiptdate < date '1992-06-01'
        and o_orderpriority = '2-HIGH         '
        and o_orderkey = l_orderkey
group by
        l_shipmode
order by
        cnt
