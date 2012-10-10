
-- fix schema: not a good solution if we create tables bit-by-bit
-- following uses LINENUMBER and ORDERKEY instead

-- version 1 based on idea of using environment variables from http://www.astorm.ch/blog/index.php?post/2011/06/26/BloomJoin-avec-MySQL

--------------------------------
-- GENERATING BIT_VECTOR -------
--------------------------------
set @hashlen=99881;
set session group_concat_max_len=@hashlen + 1; 


-- q8:
-- INSERT INTO hashes SELECT DISTINCT (((L_PARTKEY)*107+1009) MOD @hashlen) AS hash, 1 AS active FROM lineitem limit 10000 ;

-- q7:


-- version 2: takes 3s on sf=0.1 0.2s; with hashlen 99881 0.76 s
select LENGTH(compress(bit_vector)), LENGTH(bit_vector), @bit_vect := compress(bit_vector) FROM (
SELECT group_concat(active order by hash asc SEPARATOR '') as bit_vector FROM (
        -- get list of hash states
        select MAX(active) AS active, hash FROM   (
                     select hash, active from (SELECT DISTINCT (((L_SUPPKEY)*107+1009) MOD @hashlen) AS hash, 1 AS active FROM lineitem) as hashes
                     Union 
                     select number as hash, 0 as active from all_numbers where number < @hashlen
        ) as hashitems
        group by hash
) as b
) as c;

--------------------
-- SELECTION -------
--------------------



-- uncompressing and unserializing (useful)
drop temporary TABLE if exists active_hashes;
CREATE temporary TABLE active_hashes AS
select hash from
(
  SELECT
    SUBSTRING(s, number+1, 1) AS bit, number as hash
  FROM all_numbers, (SELECT uncompress(@bit_vect) AS s) sel1
  WHERE number < char_length(s)
) as bit_vector
where bit='1';

-- now to select only bloom-join filtered cols!!!
-- q7
SELECT * FROM supplier 
JOIN nation on (S_NATIONKEY = N_NATIONKEY) 
WHERE N_NAME = 'FRANCE' OR N_NAME = 'GERMANY' AND (((S_SUPPKEY)*107+1009) MOD @hashlen)  in (select hash from active_hashes);



-- uncompressing and running selection at the same time
-- pass: INT @hashlen and BINARY @bit_vect
SELECT * FROM supplier 
JOIN nation on (S_NATIONKEY = N_NATIONKEY) 
WHERE N_NAME = 'FRANCE' OR N_NAME = 'GERMANY' AND (((S_SUPPKEY)*107+1009) MOD @hashlen)  in (
        -- uncompress bit vector
        select hash from
        (
          SELECT
            SUBSTRING(s, number+1, 1) AS bit, number as hash
          FROM all_numbers, (SELECT uncompress(@bit_vect) AS s) sel1
          WHERE number < char_length(s)
        ) as bit_vector
        where bit='1');



-- q8
select * from part where P_TYPE = 'ECONOMY ANODIZED STEEL';


-- TODO: compare performance of STR_CONCAT and env variales
--- TODO: choosing better hash function may minimize the false positives and collisions
-- send bit_vector + hash_f (a, b)
--- TODO: see the lecture notes
-- I can try improve mem usage it by using bits: SET @v1 = b'1000001';  http://dev.mysql.com/doc/refman/5.1/en/user-variables.html

