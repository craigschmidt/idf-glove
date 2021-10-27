-- do this on AWS Athena

CREATE EXTERNAL TABLE IF NOT EXISTS `craig`.`cleanwords100k` (
  `id` int,
  `words` string
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe' 
WITH SERDEPROPERTIES (
  'serialization.format' = '    ',
  'field.delim' = ' '
) LOCATION 's3://delomore-infogain/inputdata/'
TBLPROPERTIES ('has_encrypted_data'='false');

select count(*) from cleanwords100k;
-- 100000


CREATE EXTERNAL TABLE IF NOT EXISTS `craig`.`cleanwords` (
  `id` int,
  `words` string
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe' 
WITH SERDEPROPERTIES (
  'serialization.format' = '  ',
  'field.delim' = ' '
) LOCATION 's3://delomore-infogain/inputdatabig/'
TBLPROPERTIES ('has_encrypted_data'='false');

select count(*) from cleanwords;
-- 62551463

-- note: only say with on first one, comma after each
with overall as 
(
  select cast(count(*) as double) as N from cleanwords100k
),
-- get all id*word pairs
pairs as 
(
  select id, word
  from 
  ( select id, 
          split(words, ' ') as wordarr
    -- from cleanwords100k 
    -- where id < 10000
    from cleanwords
  ) a 
  cross join unnest(wordarr) as t (word)
),
-- count pairs of words in the same document, C_{xy}
pairwisecnt as
(
    select x, y, cast(count(*) as double) as Cxy
    from 
    (
      select l.word as x, r.word as y
      from 
      (select id, word from pairs) as l 
      join 
      (select id, word from pairs) as r on ((l.id = r.id) and (l.word != r.word))
    ) b
    group by x, y
),
-- C(w) for w = x or y
singlecnt as 
(   select word, cast(count(*) as double) as Cw
    from pairs 
    group by word
),
-- get the single probability P(w)
prob as 
(   select word, Cw/N as Pw
    from singlecnt 
    cross join overall 
),
-- now compute P(x|y) terms
conditional as 
(
  select x, y, (Cxy/sy.Cw)*ln((Cxy/sy.Cw)/px.Pw) as term
  from pairwisecnt p
  join singlecnt sy on p.y = sy.word
  join prob px on p.x = px.word
),
informationgain as 
(
  select y, sum(term) as ig from conditional
  group by y 
)
select y, ig, s.Cw as df
from informationgain i 
join  singlecnt s on i.y = s.word 
;