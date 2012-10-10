-- TODO: check if using non-meterialized VIEW instead would speed up the datacube creation, 
-- as database could decide whether to materialize this join or not

-- product name (part, s*200K) we probably don't want to externalize as joining would be quite costly
-- though the storage cost in the cube is 1,6M vs 20K, for s=0.1 ==> 16M*s, vs 200K*s [assuming linearity]

-- using only customers location. that shall be enough
-- P_NAME,


create table fulljoin
select R_regionkey as REGIONKEY, N_nationkey AS NATIONKEY, P_PARTKEY AS PRODUCT_ID, O_ORDERDATE, EXTRACT(YEAR FROM O_ORDERDATE) AS YEAR, 
EXTRACT(YEAR_MONTH FROM O_ORDERDATE) AS YEARMONTH, O_ORDERDATE AS DAY_OF_YEAR, L_EXTENDEDPRICE * (1.0 -	L_DISCOUNT) AS VOLUME 
FROM
part join lineitem on part.P_PARTKEY = lineitem.L_partkey
join orders on lineitem.L_orderkey = orders.O_orderkey
join customer on orders.O_CUSTKEY = customer.C_custkey
join nation on nation.N_nationkey = customer.C_nationkey
join region on nation.N_regionkey = region.R_regionkey;


-- not used: EXTRACT(QUARTER FROM O_ORDERDATE), EXTRACT(WEEK FROM O_ORDERDATE)



-- what if I materialized full join [by customer] locally.
