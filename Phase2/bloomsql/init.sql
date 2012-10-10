create TABLE all_numbers(number INT);

-- generate table with numbers up to 100K. support hashes of size up to 100K bits

insert into all_numbers select
  @i := @i + 1 as number
from
  (select 0 union all select 1 union all select 2 union all 
   select 3 union all select 4 union all select 5 union all 
   select 6 union all select 7 union all select 8 union all select 9) as t0,
  (select 0 union all select 1 union all select 2 union all 
   select 3 union all select 4 union all select 5 union all 
   select 6 union all select 7 union all select 8 union all select 9) as t1,
  (select 0 union all select 1 union all select 2 union all 
   select 3 union all select 4 union all select 5 union all 
   select 6 union all select 7 union all select 8 union all select 9) as t2,
  (select 0 union all select 1 union all select 2 union all 
   select 3 union all select 4 union all select 5 union all 
   select 6 union all select 7 union all select 8 union all select 9) as t3,
  (select 0 union all select 1 union all select 2 union all 
   select 3 union all select 4 union all select 5 union all 
   select 6 union all select 7 union all select 8 union all select 9) as t4,
  (select @i:=0) as t_init;

select count(*) from all_numbers;


