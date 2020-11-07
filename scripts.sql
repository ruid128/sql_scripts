-- Пример использования подзапросов
select distinct
    trim(f01_001)
from sch_res.unit001 t1
where research_type_id = 4 
      and not exists(select 
                            *
                     from esse.esse_region 
                     where region_name = trim(t1.f01_001));

-- Пример 'многие-ко-многим'
select 
      array_agg(xx) 
from 
    (select 
          unnest(regexp_split_to_array('Example', E'\\s+')) || ' text' as xx) t
;


select distinct
    replace(ВФ,'ВФ:','') f
  from (
           select id
                , case when n like 'ВР:%' then n end ВР
                , case when n like 'ВФ:%' then n end ВФ
           from (
                    select id,
                           unnest(string_to_array(
                                   replace(replace(replace(f01_015_004, 'ВР ', 'ВР:'), 'ВФ ', 'ВФ:'), ' ', ''), ';')) n
                    from sch_res.unit001
                ) t
           where COALESCE(n, '') <> ''
       ) q
 where ВФ is not null
 order by f
;

select distinct
    replace(ВР,'ВР:','') f
  from (
           select id
                , case when n like 'ВР:%' then n end ВР
                , case when n like 'ВФ:%' then n end ВФ
           from (
                    select id,
                           unnest(string_to_array(
                                   replace(replace(replace(f01_015_004, 'ВР ', 'ВР:'), 'ВФ ', 'ВФ:'), ' ', ''), ';')) n
                    from sch_res.unit001
                ) t
           where COALESCE(n, '') <> ''
       ) q
 where ВР is not null
 order by f
;

select id
    , case when n like 'ВР:%' then n end ВР
    , case when n like 'ВФ:%' then n end ВФ
from (
        select id,
               unnest(string_to_array(
                       replace(replace(replace(f01_015_004, 'ВР ', 'ВР:'), 'ВФ ', 'ВФ:'), ' ', ''), ';')) n
        from sch_res.unit001
    ) t
where COALESCE(n, '') <> ''

          
--Для решения задачи по замене пустых строк на null без сдвига
update esse_tmp.temp_2
set col2 = NULLIF(col2, 'l');

--Решение задачи по поиску и удалению дубликатов строк без свдига по строкам
update esse_tmp.temp_1
set col1 = (
update esse_tmp.temp_1
set col1 = (select case
                       when r.t = 1
                           then r.col1
                       else null
                       end as col1
            from (
                     select col1, row_number() over (partition by col1) as t
                     from temp_1) as r
            );

