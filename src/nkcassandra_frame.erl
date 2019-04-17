%% -------------------------------------------------------------------
%%
%% Copyright (c) 2019 Carlos Gonzalez Florido.  All Rights Reserved.
%% Based on ECQL project by Feng Lee
%% Copyright (c) 2015-2016 Feng Lee <feng@emqtt.io>. All Rights Reserved.
%%
%% Permission is hereby granted, free of charge, to any person obtaining a copy
%% of this software and associated documentation files (the "Software"), to deal
%% in the Software without restriction, including without limitation the rights
%% to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
%% copies of the Software, and to permit persons to whom the Software is
%% furnished to do so, subject to the following conditions:
%%
%% The above copyright notice and this permission notice shall be included in all
%% copies or substantial portions of the Software.
%%
%% THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
%% IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
%% FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
%% AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
%% LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
%% OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
%% SOFTWARE.
%%-----------------------------------------------------------------------------
%% @author Feng Lee <feng@emqtt.io>
%%
%% @doc CQL Frame:
%%
%% 0         8        16        24        32         40
%% +---------+---------+---------+---------+---------+
%% | version |  flags  |      stream       | opcode  |
%% +---------+---------+---------+---------+---------+
%% |                length                 |
%% +---------+---------+---------+---------+
%% |                                       |
%% .            ...  body ...              .
%% .                                       .
%% .                                       .
%% +----------------------------------------
%%
%% @end
%%-----------------------------------------------------------------------------

-module(nkcassandra_frame).

-include("ecql.hrl").
-include("ecql_types.hrl").

-export([parse/1, serialize/1]).


%% @doc
parse(Bin) when byte_size(Bin) >= 9 ->
    <<
        Vsn:?BYTE,
        Flags:?BYTE,
        Stream:?SHORT,
        OpCode:?BYTE,
        Length:32/big-integer,
        Rest/binary
    >> = Bin,
    ?VER_RESP = Vsn,
    case byte_size(Bin) >= Length of
        true ->
            Frame = #ecql_frame{
                version = ?VER_RESP,
                flags = Flags,
                stream = Stream,
                opcode = OpCode,
                length = Length
            },
            <<Body:Length/binary, Rest2/binary>> = Rest,
            Resp = parse_resp(Frame#ecql_frame{body = Body}),
            {ok, Frame#ecql_frame{message = Resp}, Rest2};
        false ->
            more
    end;

parse(_Bin) ->
    more.


%% @private
parse_resp(#ecql_frame{opcode = ?OP_ERROR, body = Body}) ->
    <<Code:?INT, Rest/binary>> = Body,
    {Message, Rest1} = parse_string(Rest),
    parse_error(#ecql_error{code = Code, message = Message}, Rest1);

parse_resp(#ecql_frame{opcode = ?OP_READY}) ->
    #ecql_ready{};

parse_resp(#ecql_frame{opcode = ?OP_AUTHENTICATE, body = Body}) ->
    {ClassName, _Rest} = parse_string(Body),
    #ecql_authenticate{class = ClassName};

parse_resp(#ecql_frame{opcode = ?OP_SUPPORTED, body = Body}) ->
    {Multimap, _Rest} =  parse_string_multimap(Body),
    #ecql_supported{options = Multimap};

parse_resp(#ecql_frame{opcode = ?OP_RESULT, body = Body}) ->
    <<Kind:?INT, Bin/binary>> = Body,
    parse_result(Bin, #ecql_result{kind = result_kind(Kind)});

parse_resp(#ecql_frame{opcode = ?OP_EVENT, body = Body}) ->
    {EventType, Rest} = parse_string(Body),
    {EventData, _Rest} = parse_event(EventType, Rest),
    #ecql_event{type = EventType, data = EventData};

parse_resp(#ecql_frame{opcode = ?OP_AUTH_CHALLENGE, body = Body}) ->
    {Token, _Rest} = parse_bytes(Body),
    #ecql_auth_challenge{token = Token};

parse_resp(#ecql_frame{opcode = ?OP_AUTH_SUCCESS, body = Body}) ->
    {Token, _Rest} = parse_bytes(Body),
    #ecql_auth_success{token = Token}.


%% @private
parse_error(Error = #ecql_error{code = ?ERR_UNAVAILABE}, Bin) ->
    <<Cl:?SHORT, Required:?INT, Alive:?INT, _/binary>> = Bin,
    Error#ecql_error{detail = [
        {consistency, nkcassandra_types:consistency_name(Cl)},
        {required, Required},
        {alive, Alive}
    ]};

parse_error(Error = #ecql_error{code = ?ERR_WRITE_TIMEOUT}, Bin) ->
    <<Cl:?SHORT, Received:?INT, BlockFor:?INT, Rest/binary>> = Bin,
    {WriteType, _} = parse_string(Rest),
    Error#ecql_error{detail = [
        {consistency, nkcassandra_types:consistency_name(Cl)},
        {received, Received},
        {blockfor, BlockFor},
        {write_type, WriteType}
    ]};

parse_error(Error = #ecql_error{code = ?ERR_READ_TIMEOUT}, Bin) ->
    <<Cl:?SHORT, Received:?INT, BlockFor:?INT, Present:8, _Rest/binary>> = Bin,
    Error#ecql_error{detail = [
        {consistency, nkcassandra_types:consistency_name(Cl)},
        {received, Received},
        {blockfor, BlockFor},
        {data_present, Present}
    ]};

parse_error(Error = #ecql_error{code = ?ERR_ALREADY_EXISTS}, Bin) ->
    {Ks, Rest} = parse_string(Bin),
    {Table, _} = parse_string(Rest),
    Error#ecql_error{detail = [{ks, Ks}, {table, Table}]};

parse_error(Error = #ecql_error{code = ?ERR_UNPREPARED}, Bin) ->
    {Id, _} = parse_short_bytes(Bin),
    Error#ecql_error{detail = Id};

parse_error(Error, Bin) -> %% default
    Error#ecql_error{detail = Bin}.


%% @private
parse_event(EvenType, Bin) when EvenType =:= <<"TOPOLOGY_CHANGE">>; EvenType =:= <<"STATUS_CHANGE">> ->
    {Change, Rest} = parse_string(Bin),
    {IpBytes, Rest2} = parse_bytes(Rest),
    {Ip, _} = nkcassandra_types:decode(inet, IpBytes),
    {{Change, Ip}, Rest2};

parse_event(<<"SCHEMA_CHANGE">>, Bin) ->
    {ChangeType, Rest} = parse_string(Bin),
    {Target, Rest1} = parse_string(Rest),
    {Keyspace, Name, RestX} =
        if Target == <<"KEYSPACE">> ->
            {Ks, Rest2} = parse_string(Rest1),
            {Ks, undefined, Rest2};
            Target == <<"TABLE">> orelse Target == <<"TYPE">> ->
                {Ks, Rest2} = parse_string(Rest1),
                {N, Rest3} = parse_string(Rest2),
                {Ks, N, Rest3};
            true ->
                {Rest1, <<>>}
        end,
    {{ChangeType, Target, Keyspace, Name}, RestX};

parse_event(_EventType, Rest) ->
    {Rest, <<>>}.


%% @private
parse_result(_Bin, Resp = #ecql_result{kind = void}) ->
    Resp;

parse_result(Bin, Resp = #ecql_result{kind = rows}) ->
    {Meta, Rest}   = parse_rows_meta(Bin),
    {Rows, _Rest1} = parse_rows_content(Meta, Rest),
    Resp#ecql_result{data = #ecql_rows{meta = Meta, data = Rows}};

parse_result(Bin, Resp = #ecql_result{kind = set_keyspace}) ->
    {Keyspace, _Rest} = parse_string(Bin),
    Result = #ecql_set_keyspace{keyspace = Keyspace},
    Resp#ecql_result{data = Result};

parse_result(Bin, Resp = #ecql_result{kind = prepared}) ->
    {Id, _Rest} = parse_short_bytes(Bin),
    Resp#ecql_result{data = #ecql_prepared{id = Id}};

parse_result(Bin, Resp = #ecql_result{kind = schema_change}) ->
    Resp#ecql_result{data = parse_schema_change(Bin)}.


%% @private
parse_schema_change(Bin) ->
    {Type,    Rest}  = parse_string(Bin),
    {Target,  Rest1} = parse_string(Rest),
    {Options, _}     = parse_string(Rest1),
    #ecql_schema_change{type = Type, target = Target, options = Options}.

parse_rows_meta(<<Flags:4/binary, Count:?INT, Bin/binary>>) ->
    <<_Unused:29, NoMetadata:1, HashMorePages:1, GlobalTabSpec:1>> = Flags,
    {PagingState, Rest} = parse_paging_state(bool(HashMorePages), Bin),
    {TableSpec, Rest1} = parse_global_table_spec(bool(GlobalTabSpec), Rest),
    {Columns, Rest2} = parse_columns(bool(NoMetadata), bool(GlobalTabSpec), Count, Rest1),
    {#ecql_rows_meta{count = Count, columns = Columns, paging_state = PagingState, table_spec = TableSpec}, Rest2}.


%% @private
parse_paging_state(false, Bin) ->
    {undefined, Bin};
parse_paging_state(true, Bin) ->
    parse_bytes(Bin).


%% @private
parse_global_table_spec(false, Bin) ->
    {undefined, Bin};
parse_global_table_spec(true, Bin) ->
    {Keyspace, Rest} = parse_string(Bin),
    {Table, Rest1} = parse_string(Rest),
    {<<Keyspace/binary, ".", Table/binary>>, Rest1}.


%% @private
parse_columns(true, _GlobalTabSpec, _Count, Bin) ->
    {[], Bin};
parse_columns(false, _GlobalTabSpec, 0, Bin) ->
    {[], Bin};
parse_columns(false, GlobalTabSpec, Count, Bin) ->
    parse_column(GlobalTabSpec, Count, Bin).


%% @private
parse_column(GlobalTabSpec, Count, Bin) ->
    parse_column(GlobalTabSpec, Count, Bin, []).


%% @private
parse_column(_GlobalTabSpec, 0, Bin, Acc) ->
    {lists:reverse(Acc), Bin};
parse_column(true, N, Bin, Acc) ->
    {Column, Rest} = parse_string(Bin),
    {Type, Rest1} = parse_type(Rest),
    parse_column(true, N - 1, Rest1, [{Column, Type}|Acc]);

parse_column(false, N, Bin, Acc) ->
    {_Keyspace, Rest} = parse_string(Bin),
    {_Table, Rest1} = parse_string(Rest),
    {Column, Rest2} = parse_string(Rest1),
    {Type, Rest3} = parse_type(Rest2),
    parse_column(false, N - 1, Rest3, [{Column, Type}|Acc]).


%% @private
parse_type(<<?TYPE_CUSTOM:?SHORT, Bin/binary>>) ->
    {Class, Rest} = parse_string(Bin),
    {{custom, Class}, Rest};
parse_type(<<?TYPE_ASCII:?SHORT, Bin/binary>>) ->
    {ascii, Bin};
parse_type(<<?TYPE_BIGINT:?SHORT, Bin/binary>>) ->
    {bigint, Bin};
parse_type(<<?TYPE_BLOB:?SHORT, Bin/binary>>) ->
    {blob, Bin};
parse_type(<<?TYPE_BOOLEAN:?SHORT, Bin/binary>>) ->
    {boolean, Bin};
parse_type(<<?TYPE_COUNTER:?SHORT, Bin/binary>>) ->
    {counter, Bin};
parse_type(<<?TYPE_DECIMAL:?SHORT, Bin/binary>>) ->
    {decimal, Bin};
parse_type(<<?TYPE_DOUBLE:?SHORT, Bin/binary>>) ->
    {double, Bin};
parse_type(<<?TYPE_FLOAT:?SHORT, Bin/binary>>) ->
    {float, Bin};
parse_type(<<?TYPE_INT:?SHORT, Bin/binary>>) ->
    {int, Bin};
parse_type(<<?TYPE_TIMESTAMP:?SHORT, Bin/binary>>) ->
    {timestamp, Bin};
parse_type(<<?TYPE_UUID:?SHORT, Bin/binary>>) ->
    {uuid, Bin};
parse_type(<<?TYPE_VARCHAR:?SHORT, Bin/binary>>) ->
    {varchar, Bin};
parse_type(<<?TYPE_VARINT:?SHORT, Bin/binary>>) ->
    {varint, Bin};
parse_type(<<?TYPE_TIMEUUID:?SHORT, Bin/binary>>) ->
    {timeuuid, Bin};
parse_type(<<?TYPE_INET:?SHORT, Bin/binary>>) ->
    {inet, Bin};
parse_type(<<?TYPE_LIST:?SHORT, Bin/binary>>) ->
    {Type, Rest} = parse_type(Bin),
    {{list, Type}, Rest};
parse_type(<<?TYPE_MAP:?SHORT, Bin/binary>>) ->
    {KeyType, Rest} = parse_type(Bin),
    {ValType, Rest1} = parse_type(Rest),
    {{map, {KeyType, ValType}}, Rest1};
parse_type(<<?TYPE_SET:?SHORT, Bin/binary>>) ->
    {Type, Rest} = parse_type(Bin),
    {{set, Type}, Rest};
parse_type(<<?TYPE_UDT:?SHORT, _Bin/binary>>) ->
    throw({error, unsupport_udt_type});
parse_type(<<?TYPE_TUPLE:?SHORT, Bin/binary>>) ->
    <<N:?SHORT, Rest/binary>> = Bin,
    {Rest1, ElTypes} =
        lists:foldl(fun(_I, {Rest1, Acc}) ->
            {Type, Rest2} = parse_type(Rest1),
            {Rest2, [Type | Acc]}
        end, {Rest, []}, lists:seq(1, N)),
    {{tuple, list_to_tuple(lists:reverse(ElTypes))}, Rest1}.


%% @private
parse_rows_content(Meta, <<Count:?INT, Bin/binary>>) ->
    parse_rows_content(Meta, Count, Bin).


%% @private
parse_rows_content(Meta, Count, Bin) ->
    parse_row(Meta, Count, Bin, []).

%% @private
parse_row(_Meta, 0, Bin, Rows) ->
    {lists:reverse(Rows), Bin};

parse_row(Meta, Count, Bin, Rows) ->
    {Row, Rest} = parse_row(Meta, Bin),
    parse_row(Meta, Count - 1, Rest, [Row|Rows]).


%% @private
parse_row(#ecql_rows_meta{columns = Columns}, Bin) ->
    {Cells, Rest} = lists:foldl(fun(Col, {CellAcc, LeftBin}) ->
        {Cell, LeftBin1} = parse_cell(Col, LeftBin),
        {[Cell | CellAcc], LeftBin1}
    end, {[], Bin}, Columns),
    {lists:reverse(Cells), Rest}.


%% @private
parse_cell(_Col, <<-1:?INT, Bin/binary>>) ->
    {null, Bin};
parse_cell({_Name, Type}, Bin) ->
    {Bytes, Rest} = parse_bytes(Bin),
    Val = nkcassandra_types:decode(Type, Bytes),
    {Val, Rest}.

%% @private
parse_string_multimap(<<Len:?SHORT, Bin/binary>>) ->
    parse_string_multimap(Len, Bin, []).

parse_string_multimap(0, Rest, Acc) ->
    {lists:reverse(Acc), Rest};

parse_string_multimap(Len, Bin, Acc) ->
    {Key, Rest} = parse_string(Bin),
    {StrList, Rest1} = parse_string_list(Rest),
    parse_string_multimap(Len - 1, Rest1, [{Key, StrList} | Acc]).


%% @private
parse_string_list(<<Len:?SHORT, Bin/binary>>) ->
    parse_string_list(Len, Bin, []).

parse_string_list(0, Rest, Acc) ->
    {lists:reverse(Acc), Rest};

parse_string_list(Len, Bin, Acc) ->
    {Str, Rest} = parse_string(Bin),
    parse_string_list(Len - 1, Rest, [Str|Acc]).


%% @private
parse_string(<<Len:?SHORT, Str:Len/binary, Rest/binary>>) ->
    {Str, Rest}.


%% @private
parse_bytes(<<-1:?INT, Bin/binary>>) ->
    {<<>>, Bin};
parse_bytes(<<Size:?INT, Bin/binary>>) ->
    <<Bytes:Size/binary, Rest/binary>> = Bin,
    {Bytes, Rest}.

parse_short_bytes(<<Size:?SHORT, Bin/binary>>) ->
    <<Bytes:Size/binary, Rest/binary>> = Bin,
    {Bytes, Rest}.


%% @private
-spec serialize(ecql_frame()) ->
    binary().

serialize(Frame) ->
    serialize(header, serialize(body, Frame)).


%% @private
serialize(body, Frame = #ecql_frame{message = Req}) ->
    Body = serialize_req(Req),
    Frame#ecql_frame{length = size(Body), body = Body};

serialize(header, #ecql_frame{version = Version,
    flags   = Flags,
    stream  = Stream,
    opcode  = OpCode,
    length  = Length,
    body    = Body}) ->
    <<Version:?BYTE, Flags:?BYTE, Stream:?SHORT, OpCode:?BYTE, Length:32/big-integer, Body/binary>>.


%% @private
serialize_req(#ecql_startup{version = Ver, compression = undefined}) ->
    serialize_string_map([{<<"CQL_VERSION">>, Ver}]);

serialize_req(#ecql_startup{version = Ver, compression = Comp}) ->
    serialize_string_map([{<<"CQL_VERSION">>, Ver}, {<<"COMPRESSION">>, Comp}]);

serialize_req(#ecql_auth_response{token = Token}) ->
    serialize_bytes(Token);

serialize_req(#ecql_options{}) ->
    <<>>;

serialize_req(Query = #ecql_query{query = CQL}) ->
    << (serialize_long_string(CQL))/binary, (serialize_query_parameters(Query))/binary >>;

serialize_req(#ecql_prepare{query = Query}) ->
    serialize_long_string(Query);

serialize_req(#ecql_execute{id = Id, query = Query}) ->
    << (serialize_short_bytes(Id))/binary, (serialize_query_parameters(Query))/binary >>;

serialize_req(#ecql_batch{}=Batch) ->
    #ecql_batch{
        type = Type,
        queries = Queries,
        consistency = Consistency,
        flags = Flags,
        with_names = WithNames,
        serial_consistency = SerialConsistency,
        timestamp = Timestamp
    } = Batch,
    QueriesBin = << <<(serialize_batch_query(Query))/binary>> || Query <- Queries >>,
    Flags = <<
        0:5,
        (flag(WithNames))/binary,
        (flag(SerialConsistency)):1,
        (flag(Timestamp)):1, 0:1
    >>,
    Parameters = [
        {serialize_consistency, SerialConsistency},
        {timestamp, Timestamp}
    ],
    ParamtersBin = << <<(serialize_parameter(Name, Val))/binary>> || {Name, Val} <- Parameters >>,
    <<Type:?BYTE, (length(Queries)):?SHORT, QueriesBin/binary, Consistency:?SHORT, Flags:?BYTE, ParamtersBin/binary>>;

serialize_req(#ecql_register{event_types = EventTypes}) ->
    serialize_string_list(EventTypes).


%% @private
serialize_batch_query(#ecql_batch_query{kind = 0, query_or_id = Str, values = Values}) ->
    <<0:?BYTE, (serialize_long_string(Str))/binary, (serialize_batch_query_values(Values))/binary>>;

serialize_batch_query(#ecql_batch_query{kind = 1, query_or_id = Id, values = Values}) ->
    <<0:?BYTE, (serialize_short_bytes(Id))/binary, (serialize_batch_query_values(Values))/binary>>.


%% @private
serialize_batch_query_values([]) ->
    <<>>;

serialize_batch_query_values([H|_] = Values) when is_tuple(H) ->
    ValuesBin = << <<(serialize_string(Name))/binary, (serialize_bytes(Val))/binary>> || {Name, Val} <- Values >>,
    << (length(Values)):?SHORT, ValuesBin/binary>>;

serialize_batch_query_values([H|_] = Values) when is_binary(H) ->
    ValuesBin = << <<(serialize_bytes(Val))/binary>> || Val <- Values >>,
    << (length(Values)):?SHORT, ValuesBin/binary>>.


%% @private
serialize_query_parameters(#ecql_query{}=Query) ->
    #ecql_query{
        consistency = Consistency,
        values = Values,
        skip_metadata = SkipMetadata,
        result_page_size = PageSize,
        paging_state = PagingState,
        serial_consistency = SerialConsistency,
        timestamp = Timestamp
    } = Query,
    Flags = <<
        0:1,
        (flag(values, Values)):1,
        (flag(Timestamp)):1,
        (flag(SerialConsistency)):1,
        (flag(PagingState)):1,
        (flag(PageSize)):1,
        (flag(SkipMetadata)):1,
        (flag(Values)):1
    >>,
    [_Q, _C, _F|Parameters] = lists:zip(
            record_info(fields, ecql_query),
            tl(tuple_to_list(Query))),
    Bin = << <<(serialize_parameter(Name, Val))/binary>> || {Name, Val} <- Parameters, Val =/= undefined >>,
    <<Consistency:?SHORT, Flags/binary, Bin/binary>>.


%% @private
serialize_parameter(values, []) ->
    <<>>;

serialize_parameter(values, [H |_] = Vals) when is_tuple(H) ->
    Bin = << <<(serialize_string(Name))/binary, (serialize_bytes(Val))/binary>> || {Name, Val} <- Vals >>,
    <<(length(Vals)):?SHORT, Bin/binary>>;

serialize_parameter(values, [H |_] = Vals) when is_binary(H) ->
    Bin = << <<(serialize_bytes(Val))/binary>> || Val <- Vals >>,
    <<(length(Vals)):?SHORT, Bin/binary>>;

serialize_parameter(skip_metadata, _) ->
    <<>>;

serialize_parameter(result_page_size, PageSize) ->
    <<PageSize:?INT>>;

serialize_parameter(paging_state, PagingState) ->
    serialize_bytes(PagingState);

serialize_parameter(serial_consistency, SerialConsistency) ->
    <<SerialConsistency:?SHORT>>;

serialize_parameter(timestamp, Timestamp) ->
    <<Timestamp:?LONG>>.

%%serialize_string_multimap(Map) ->
%%    Bin = << <<(serialize_string(K))/binary, (serialize_string_list(L))/binary>> || {K, L} <- Map >>,
%%    <<(length(Map)):?SHORT, Bin/binary>>.


%% @private
serialize_string_map(Map) ->
    Bin = << <<(serialize_string(K))/binary, (serialize_string(V))/binary>> || {K, V} <- Map >>,
    <<(length(Map)):?SHORT, Bin/binary>>.


%% @private
serialize_string_list(List) ->
    Bin = << <<(serialize_string(S))/binary>> || S <- List >>,
    <<(length(List)):?SHORT, Bin/binary>>.


%% @private
serialize_string(S) ->
    <<(size(S)):?SHORT, S/binary>>.


%% @private
serialize_long_string(S) ->
    <<(size(S)):?INT, S/binary>>.


%% @private
serialize_short_bytes(Bytes) ->
    <<(size(Bytes)):?SHORT, Bytes/binary>>.


%% @private
serialize_bytes(Bin) ->
    nkcassandra_types:to_bytes(Bin).


%%serialize_option_list(Options) ->
%%    Bin = << <<(serialize_option(Opt))/binary>> || Opt <- Options >>,
%%    <<(size(Options)):?SHORT, Bin/binary>>.

%%serialize_option({Id, Val}) ->
%%TODO:...
%%    <<>>.

result_kind(16#01) -> void;
result_kind(16#02) -> rows;
result_kind(16#03) -> set_keyspace;
result_kind(16#04) -> prepared;
result_kind(16#05) -> schema_change.

flag(values, undefined)                   -> 0;
flag(values, [])                          -> 0;
flag(values, [Val|_]) when is_binary(Val) -> 0;
flag(values, [Val|_]) when is_tuple(Val)  -> 1.

flag(undefined) -> 0;
flag([])        -> 0;
flag(false)     -> 0;
flag(true)      -> 1;
flag(_Val)      -> 1.

bool(1) -> true;
bool(0) -> false.

