def partition(cols):
    row_len = 1
    rows = []
    for i in range(int(len(cols) / row_len)):
        rows.append(cols[i * row_len:i * row_len + row_len])
    return rows

def connect():
    conn = None
    try:
        # print("Connecting to db...")
        conn = p.connect(**conf_list)
    except (Exception, p.DatabaseError) as error:
        print(error)
        sys.exit(1)
    # print("Connection successful")
    return conn

def filter(scheme, table, cols_in):
    # Создаем dfs с уникальными значениями
    list_in = \
        [
            """select distinct {cur_col} from {scheme}.{table}"""
                .format(scheme=scheme
                        , table=table
                        , cur_col=i[0]) for i in cols_in
        ]
    df_in = []
    for i in range(len(list_in)):
        df_in\
            .append(pd.DataFrame(
                                    pd.read_sql_query('{cur_query}'
                                                      .format(scheme=scheme
                                                            ,table=table
                                                            ,cur_query=list_in[i])
                                                            ,con=connect()
                                                      )
                                    ,index=[k for k in range(len(values))]
                                 )
                    )

    list_out = []
    for j in range(len(colons_0)):
        list_out.append(pd.DataFrame
                        .transpose(pd.DataFrame(data=[values]
                                                ,index=partition(colons_0)[j])
                                  )
                        )
    print(df_in[0])


def create_df(scheme, table, cols_in):
    list_in = \
        [
            """select distinct {cur_col} from {scheme}.{table} where {cur_col}::varchar ~ '^\d+$'"""
                .format(scheme=scheme
                        , table=table
                        , cur_col=i[0]) for i in cols_in
        ]
    df_in = []
    for i in range(len(list_in)):
        # try:
        df_in \
            .append(pd.DataFrame(
            pd.read_sql_query('{cur_query}'
                              .format(scheme=scheme
                                      ,table=table
                                      ,cur_query=list_in[i])
                              ,con=connect()
                              )
            )
        )
    # except pd.io.sql.DatabaseError:
    #     pass


    col_list_join = []
    for i in range(len(df_in)):
        if df_in[i].empty:
            pass
        else:
            col_list_join.append(df_in[i].columns.values[0])

    col_list_join_df = pd.DataFrame(col_list_join)
    df_from_xls = pd.DataFrame(partition(colons_1))
    df_result = pd.DataFrame.merge(col_list_join_df, df_from_xls)

    return df_result.to_string(index=False)

