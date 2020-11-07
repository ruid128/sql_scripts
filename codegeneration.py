def restore_data(rows, scheme, table, log_file, cd, tg_op):
    return """
    update {scheme}.{table}
    set
 {set_list}
    from {scheme}.{log_file} t2
    where t2.cd = '{cd}'
    and t2.tg_op = '{tg_op}'
    and {scheme}.{table}.id = (t2.j ->> 'id')::bigint
    """.format(
            cols=",\n".join(
                ["(t2.j ->> '{}')::{}".format(i[0], i[1]) for i in rows]
                ), scheme=scheme, table=table, log_file=log_file, cd=cd, tg_op=tg_op,
            set_list="".join(
                ["      ,{} = (t2.j ->> '{}')::{}\n".format(i[0], i[0], i[1]) for i in rows]
            )
    )

def create_table_ref(table_name):
    return """
    create table esse.ref_{table_name}
    (
       description    varchar(100),
       description_ru varchar(100),
       code           smallint,
       id             serial not null
       constraint ref_{table_name}_pkey
                 primary key  
    ); """.format(table_name=table_name)


def update_da_net(scheme, table, col, val_da, val_net):
  return """
  update {scheme}.{table}
  set
{col};
  """.format(scheme=scheme, table=table,
             col=",\n".join(update_da_net_list(col, val_da=val_da, val_net=val_net)))



def create_type(number_of_unit, col):
    return """
    create type unit{number_of_unit}
    as (
{set_list},
        );""".format(number_of_unit=number_of_unit,
                    set_list=",\n".join(create_type_list(col))
                     )

def create_type_list(columns_in):
    return ["         {columns} {types}".format(columns=i[0], types=i[1]) for i in columns_in]

def update_da_net_list(col, val_da, val_net):
  return ["""   {col} = case
                    when {col} = {val_da} then 1  
                    when {col} = {val_net} then 2
            end""".format(col=i[0], val_da=val_da, val_net=val_net) for i in col]

def ins_into_mod_list(cols):
    l = [
        """(select 
                description_ru as {cols}
            from {ref_tab}
            where {ref_tab}.id = t1.{cols} 
            limit 1),""".format(ref_tab=i[0], cols=i[1]) for i in cols
    ]
    for j in l:
        print(j)

def func_log(scheme, table):
    return """create or replace function {scheme}.{table}_log() returns trigger language plpgsql
as
    $$
        begin
            if (TG_OP = 'INSERT') then
                    insert into {scheme}.{table}_log
                                            (mu
                                            ,client
                                            ,ver
                                            ,tg_when
                                            ,tg_tab
                                            ,tg_op
                                            ,j)
                        SELECT
                             user
                            ,inet_client_addr()
                            ,id
                            ,TG_WHEN
                            ,TG_TABLE_NAME
                            ,TG_OP
                            ,row_to_json (NEW)
                                from {scheme}.units_ver
                                order by cd
                                desc limit 1;
            elsif (TG_OP = 'UPDATE') then
                    insert into {scheme}.{table}_log
                                (mu
                                ,client
                                ,ver
                                ,tg_when
                                ,tg_tab
                                ,tg_op
                                ,j)
                                SELECT
                                    user
                                    ,inet_client_addr()
                                    ,id
                                    ,TG_WHEN
                                    ,TG_TABLE_NAME
                                    ,'UDELETE'
                                    ,row_to_json (OLD)
                                    from {scheme}.units_ver
                                    order by cd
                                    desc limit 1;
                    insert into {scheme}.{table}_log
                                (mu
                                ,client
                                ,ver
                                ,tg_when
                                ,tg_tab
                                ,tg_op
                                ,j) SELECT
                                        user
                                        ,inet_client_addr()
                                        ,id
                                        ,TG_WHEN
                                        ,TG_TABLE_NAME
                                        ,'UINSERT'
                                        ,row_to_json (NEW)
                                        from {scheme}.units_ver
                                        order by cd
                                        desc limit 1;
            elsif  (TG_OP = 'DELETE') then
                    insert into {scheme}.{table}_log
                                (mu
                                ,client
                                ,ver
                                ,tg_when
                                ,tg_tab
                                ,tg_op
                                ,j) SELECT
                                        user
                                        ,inet_client_addr()
                                        ,id
                                        ,TG_WHEN
                                        ,TG_TABLE_NAME
                                        ,TG_OP
                                        ,row_to_json (OLD)
                                        from {scheme}.units_ver
                                        order by cd
                                        desc limit 1;
            end if;
        return null;
    END;
$$;

alter function {scheme}.{table}_log() owner to group_full;""".format(table=table, scheme=scheme)

def create_log_table(table):
    return """
    create table {table}_log
    (
    id      bigserial not null
        constraint {table}_log_pkey
            primary key,
    cd      timestamp default now(),
    mu      varchar(100),
    client  varchar(100),
    tg_when varchar(50),
    tg_op   varchar(50),
    tg_tab  varchar(50),
    j       json,
    ver     integer
        constraint units_log_units_ver_id_fk
            references units_ver
    );

alter table {table}_log
    owner to group_full;""".format(table=table)

def create_ver_table(table):
    return """create table {table}_ver
    (
    id       serial not null
        constraint {table}_ver_pkey
            primary key,
    cd       timestamp default now(),
    ver_major varchar(15),
    ver_minor varchar(15)
    );

alter table {table}_ver
    owner to group_full;""".format(table=table)


def ins_into_mod(table_into, table_from, cols_from, cols_into):
    return """
    insert into sch_res.{table_into} (
        unit001_id,
        {cols_into}
)
select
        f2.id,
        {cols_from}
from esse.{table_from} f1
join sch_res.unit001 f2
on (f2.f01_001 = f1."t.1.2"::varchar(100))
and (f2.f01_005 = f1."t.1."::varchar(100));""".format(table_into=table_into, table_from=table_from,
                                                      cols_from = "\n\t".join(ins_into_mod_list(cols_from)),
                                                      cols_into="\n\t". join(ins_into_mod_list(cols_into)))

def create_triggers (scheme, table):
    return """
create trigger tg_{table}_after
after insert or update or delete on {scheme}.{table}
    for each row execute function {scheme}.{table}_log();
    
create trigger tg_{table}_after_increment
after insert or update or delete on {scheme}.{table}
    for each statement execute function {scheme}.version_increment();
""".format(scheme=scheme, table=table)

def create_view(table, cols):
    return """create or replace view sch_res.v_{table}
    as
        select
            t2.research_type_id
            ,t2.f01_005
            ,t2.f01_001
            ,{insert_list}
            t2.id
        from sch_res.{table} t1
        join sch_res.unit001 t2
        on t1.unit001_id = t2.id;
        
alter view v_{table}
owner to group_full;
    """.format(table=table,
               insert_list=ins_into_mod_list(cols))

def add_fk(scheme, table, columns):
    _list = [ """alter table {scheme}.{table}
    add constraint fk_{cur_col}
    foreign key ({cur_col})
    references {ref_tab}(id);""".format(cur_col=i[1], ref_tab=i[0], scheme=scheme, table=table) for i in columns
    ]
    for i in _list:
        print(i)
    return ';'

def set_schema(scheme_from, scheme_to, cols):
    _list = [ """alter table {scheme_from}.{table}
    set schema {scheme_to};""".format(scheme_from=scheme_from, scheme_to=scheme_to,
                                     table=i[0]) for i in cols
    ]
    for j in _list:
        print(j)
    return ';'

def update_table(table, columns):
    list_update = ["""
update sch_res.{table}
    set
        {cur_col} = t2.id
from sch_res.{ref_table} t2
where {cur_col} = t2.code;""".format(table=table,
                                    cur_col=i[1],
                                    ref_table=i[0]) for i in columns]
    for j in list_update:
        print(j)

def alter_table_int(scheme, table, cols):
    list_alter = [
        """alter table {scheme}.{table}
alter column {cur_col} type int using {cur_col}::int;""".format(table=table, scheme=scheme
                                                                          ,cur_col=i[1]) for i in cols
    ]
    for k in list_alter:
        print(k)
