select
        l_returnflag,
        l_linestatus,
        count(*),
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
        lineitem
where
        l_shipdate <= date '1998-08-06'
group by
        l_returnflag,
        l_linestatus
order by
        l_returnflag,
        l_linestatus;

