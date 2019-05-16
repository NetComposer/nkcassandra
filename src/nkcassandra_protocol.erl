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
%%------------------------------------------------------------------------------

-module(nkcassandra_protocol).
-behavior(nkpacket_protocol).

-include("ecql.hrl").
-include("ecql_types.hrl").


-export([connect/3]).
-export([options/1,  query/2, query/3, query/4,
          prepare/2, prepare/3, execute/2, execute/3, execute/4]).
-export([get_all/0, stop/1]).
-export([transports/1, default_port/1]).
-export([conn_init/1, conn_parse/3, conn_encode/2, conn_handle_call/4,
         conn_handle_cast/3, conn_handle_info/3, conn_stop/3]).
-export_type([cql_result/0, cql_error/0]).


-define(DEBUG(Txt, Args, State),
    case State#state.debug of
        true -> ?LLOG(debug, Txt, Args, State);
        _ -> ok
    end).

-define(LLOG(Type, Txt, Args, State),
    lager:Type(
        [
            {srv_id, State#state.srv}
        ],
        "NkCASSANDRA Client (~s) "++Txt,
        [
            State#state.srv
            | Args
        ])).


-define(SYNC_CALL_TIMEOUT, 3*5*1000).
-define(TIMEOUT, 180000).
-define(OP_TIMEOUT, 30000).

-define(PASSWORD_AUTHENTICATOR, <<"org.apache.cassandra.auth.PasswordAuthenticator">>).

-include_lib("nkserver/include/nkserver.hrl").
-include_lib("nkpacket/include/nkpacket.hrl").
-include("nkcassandra.hrl").



%% ===================================================================
%% Types
%% ===================================================================

-type options() ::
    #{
        srv => nkserver:id(),
        keyspace => binary() | undefined,
        debug => boolean(),
        keyspace => string()|binary(),
        user => string() | binary(),
        password => string() | binary()
    }.

-type query_string() :: string() | iodata().

-type cql_result() ::
    Keyspace :: binary()
    | {TableSpec :: binary(), Columns :: [tuple()], Rows :: list()}
    | {Type :: binary(), Target :: binary(), Options :: any()}.


-type prepared_name() :: atom().

-type prepared_id() :: binary().

-type cql_error() ::
    {cassandra, integer(), binary()}.


%% ===================================================================
%% Public
%% ===================================================================

%% @doc

%% @doc Starts a new session and sets keyspace (if defined)
-spec connect(inet:ip_address(), inet:port_number(), options()) ->
    {ok, pid()} | {error, term()|cql_error()}.

connect(Ip, Port, Opts) ->
    ConnOpts = #{
        idle_timeout => ?TIMEOUT,
        send_timeout => ?TIMEOUT,
        send_timeout_close => true,
        user_state => Opts
    },
    Conn = #nkconn{
        protocol = nkcassandra_protocol,
        transp = tcp,
        ip = Ip,
        port = Port,
        opts = ConnOpts
    },
    case nkpacket:connect(Conn, #{}) of
        {ok, #nkport{pid=Pid}} ->
            case call(Pid, startup) of
                ok ->
                    case call(Pid, set_keyspace) of
                        {ok, _} ->
                            {ok, Pid};
                        {error, Error} ->
                            {error, Error}
                    end;
                {error, Error} ->
                    {error, Error}
            end;
        {error, Error} ->
            {error, Error}
    end.


%% @doc Query.
-spec query(pid(), query_string()) ->
    {ok, cql_result()} | {error, term()|cql_error()}.

query(Pid, Query) ->
    query(Pid, Query, undefined, one).


%% @doc
-spec query(pid(), query_string(), list()) ->
    {ok, cql_result()} | {error, any()}.

query(Pid, Query, Values) when is_list(Values) ->
    query(Pid, Query, Values, one).


%% @doc
-spec query(pid(), query_string(), list(), atom()) ->
    {ok, cql_result()} | {error, term()|cql_error()}.

query(Pid, Query, Values, CL) when is_atom(CL) ->
    Obj = #ecql_query{
        query = iolist_to_binary(Query),
        consistency = nkcassandra_types:consistency_value(CL),
        values = encode_values(Values)
    },
    call(Pid, {query, Obj}).


%% @doc Prepare (anonymous)
-spec prepare(pid(), query_string()) ->
    {ok, prepared_id()} | {error, term()|cql_error()}.

prepare(Pid, Query) ->
    call(Pid, {prepare, iolist_to_binary(Query)}).


%% @doc Prepare (with name)
-spec prepare(pid(), prepared_name(), query_string()) ->
    {ok, prepared_id()} | {error, term()|cql_error()}.

prepare(Pid, Name, Query) when is_atom(Name) ->
    call(Pid, {prepare, Name, iolist_to_binary(Query)}).


%% @doc Execute.
-spec execute(pid(), prepared_name()|prepared_id()) ->
    {ok, cql_result()} | {error, term()|cql_error()}.

execute(Pid, Id) when is_atom(Id) orelse is_binary(Id) ->
    execute(Pid, Id, undefined, one).


-spec execute(pid(), prepared_name()|prepared_id(), list()) ->
    {ok, cql_result()} | {error, term()|cql_error()}.

execute(Pid, Id, Values) when
        (is_atom(Id) orelse is_binary(Id)) andalso is_list(Values) ->
    execute(Pid, Id, Values, one).

-spec execute(pid(), prepared_name()|prepared_id(), list(), atom()) ->
    {ok, cql_result()} | {error, term()|cql_error()}.

execute(Pid, Id, Values, CL) when
        (is_atom(Id) orelse is_binary(Id)) andalso is_list(Values) andalso is_atom(CL) ->
    QObj = #ecql_query{
        consistency = nkcassandra_types:consistency_value(CL),
        values = encode_values(Values)
    },
    call(Pid, {execute, Id, QObj}).


-spec options(pid()) ->
    {ok, reference()} | {error, term()|cql_error()}.

options(Pid) ->
    call(Pid, options).


%% @doc
-spec stop(pid()) ->
    ok | {error, term()}.

stop(Pid) ->
    gen_server:cast(Pid, do_stop).


%% @private
get_all() ->
    nklib_proc:values(?MODULE).



%% ===================================================================
%% Protocol
%% ===================================================================

-record(trans, {
    stream_id :: stream_id(),
    timer :: reference(),
    from :: {pid(), term()} | {async, pid(), term()}
}).


-type prepared() ::
    #{
        {pending, stream_id()} => prepared_name(),
        prepared_name() => prepared_id()
    }.


-record(state, {
    srv :: nkservice:id(),
    keyspace :: binary() | undefined,
    auth :: binary(),
    debug :: boolean(),
    trans = #{} :: #{stream_id() => #trans{}},
    next_stream_id :: stream_id(),
    prepared = #{} :: prepared(),
    buffer = <<>> :: binary()
}).


%% @private
-spec transports(nklib:scheme()) ->
    [nkpacket:transport()].

transports(cassandra) -> [tcp, tls];
transports(tcp) -> [tcp];
transports(tls) -> [tls].


-spec default_port(nkpacket:transport()) ->
    inet:port_number() | invalid.

default_port(_) -> 9042.


%% ===================================================================
%% WS Protocol callbacks
%% ===================================================================


-spec conn_init(nkpacket:nkport()) ->
    {ok, #state{}}.

conn_init(NkPort) ->
    {ok, Opts} = nkpacket:get_user_state(NkPort),
    nklib_proc:put(?MODULE),
    State = #state{
        srv = maps:get(srv, Opts, ''),
        debug = maps:get(debug, Opts, false),
        keyspace = maps:get(keyspace, Opts, undefined),
        auth = auth_token(Opts),
        next_stream_id = rand:uniform(16#FF)
    },
    ?DEBUG("new connection (~p)", [self()], State),
    {ok, State}.


%% @private
-spec conn_parse(term()|close, nkpacket:nkport(), #state{}) ->
    {ok, #state{}} | {stop, term(), #state{}}.

conn_parse(close, _NkPort, State) ->
    {ok, State};

conn_parse(Bin, NkPort, #state{buffer=Buffer}=State) ->
    Bin2 = <<Buffer/binary, Bin/binary>>,
    case nkcassandra_frame:parse(Bin2) of
        {ok, #ecql_frame{opcode=OpCode, version=?VER_RESP}=Frame, Bin3} ->
            #ecql_frame{message=Msg, stream=StreamId} = Frame,
            Result = case OpCode of
                ?OP_AUTHENTICATE ->
                    #ecql_authenticate{class = Class} = Msg,
                    %lager:error("NKLOG AUTH"),
                    parse_authenticate(Class, NkPort, StreamId, State);
                _ ->
                    %lager:error("NKLOG RECEIVED RESP (~p) ~p ~p", [self(), Msg, StreamId]),
                    parse_frame(OpCode, Msg, StreamId, State)
            end,
            case Result of
                {ok, State2} ->
                    conn_parse(Bin3, NkPort, State2#state{buffer = <<>>});
                {stop, Stop, State2} ->
                    {stop, Stop, State2}
            end;
        more ->
            {ok, State#state{buffer=Bin2}}
    end.

%% @private
conn_encode(Msg, _NkPort) ->
    {ok, Msg}.


%% @private
conn_handle_call(startup, From, NkPort, State) ->
    Frame = #ecql_frame{
        version = ?VER_REQ,
        opcode = ?OP_STARTUP,
        message = #ecql_startup{}
    },
    request(Frame, From, NkPort, State);

conn_handle_call(set_keyspace, From, _NkPort, #state{keyspace=undefined}=State) ->
    gen_server:reply(From, {ok, undefined}),
    {ok, State};

conn_handle_call(set_keyspace, From, NkPort, #state{keyspace=Keyspace}=State) ->
    Query = #ecql_query{query = <<"use ", (to_bin(Keyspace))/binary>>},
    conn_handle_call({query, Query}, From, NkPort, State);

conn_handle_call({query, Query}, From, NkPort, State) ->
    Frame = #ecql_frame{
        version = ?VER_REQ,
        opcode = ?OP_QUERY,
        message = Query
    },
    request(Frame, From, NkPort, State);

conn_handle_call({prepare, Query}, From, NkPort, State) ->
    Frame = #ecql_frame{
        version = ?VER_REQ,
        opcode = ?OP_PREPARE,
        message = #ecql_prepare{query=Query}
    },
    request(From, Frame, NkPort, State);

conn_handle_call({prepare, Name, Query}, From, NkPort, State) ->
    #state{prepared = Prepared, next_stream_id = StreamId} = State,
    case maps:find(Name, Prepared) of
        {ok, Id} ->
            gen_server:reply(From, {ok, Id}),
            {ok, State};
        error ->
            Frame = #ecql_frame{
                version = ?VER_REQ,
                opcode = ?OP_PREPARE,
                message = #ecql_prepare{query=Query}
            },
            Prepared2 = Prepared#{{pending, StreamId} => Name},
            request(Frame, From, NkPort, State#state{prepared = Prepared2})
    end;

conn_handle_call({execute, Name, Query}, From, NkPort, State) when is_atom(Name) ->
    #state{prepared = Prepared} = State,
    case maps:find(Name, Prepared) of
        {ok, Id} ->
            conn_handle_call({execute, Id, Query}, From, NkPort, State);
        error ->
            gen_server:reply(From, {error, not_prepared}),
            {ok, State}
    end;

conn_handle_call({execute, Id, Query}, From, NkPort, State) when is_binary(Id) ->
    Frame = #ecql_frame{
        version = ?VER_REQ,
        opcode = ?OP_EXECUTE,
        message = #ecql_execute{id=Id, query=Query}
    },
    request(Frame, From, NkPort, State);

conn_handle_call(options, From, NkPort, State) ->
    Frame = #ecql_frame{
        version = ?VER_REQ,
        opcode = ?OP_OPTIONS,
        message = #ecql_options{}
    },
    request(Frame, From, NkPort, State);

conn_handle_call(get_state, From, _NkPort, State) ->
    gen_server:reply(From, lager:pr(State, ?MODULE)),
    {ok, State}.


%% @doc 
-spec conn_handle_cast(term(), nkpacket:nkport(), #state{}) ->
    {ok, #state{}} | {stop, Reason::term(), #state{}}.

conn_handle_cast(do_stop, _NkPort, State) ->
    ?DEBUG("user stop", [], State),
    {stop, normal, State}.


%% @doc
-spec conn_handle_info(term(), nkpacket:nkport(), #state{}) ->
    {ok, #state{}} | {stop, Reason::term(), #state{}}.

conn_handle_info({op_timeout, StreamId}, _NkPort, State) ->
    ?LLOG(warning, "received timeout for ~p", [StreamId], State),
    {ok, State2} = response({error, op_timeout}, StreamId, State),
    %{stop, op_timeout, State2},
    {ok, State2};

conn_handle_info(_Info, _NkPort, _State) ->
    continue.


%% @doc Called when the connection stops
-spec conn_stop(Reason::term(), nkpacket:nkport(), #state{}) ->
    ok.

conn_stop(_Reason, _NkPort, _State) ->
    ok.



%% ===================================================================
%% Util
%% ===================================================================


%% @private
call(Pid, Msg) ->
    case nklib_util:call2(Pid, Msg, ?SYNC_CALL_TIMEOUT) of
        process_not_found ->
            {error, process_not_found};
        Other ->
            Other
    end.


%% @private
parse_authenticate(?PASSWORD_AUTHENTICATOR, NkPort, StreamId, #state{auth=Token}=State) ->
    Frame = #ecql_frame{
        version = ?VER_REQ,
        opcode = ?OP_AUTH_RESPONSE,
        message = #ecql_auth_response{token = Token},
        stream = StreamId
    },
    Bin = nkcassandra_frame:serialize(Frame),
    send(Bin, NkPort, State);

parse_authenticate(Class, _NkPort, StreamId, State) ->
    stop(self()),
    response({error, {unsupported_auth_class, Class}}, StreamId, State).


%% @private
parse_frame(?OP_READY, #ecql_ready{}, StreamId, State) ->
    response(ok, StreamId, State);

parse_frame(?OP_ERROR, Data, StreamId, State) ->
    #ecql_error{code = Code, message = Message} = Data,
    response({error, {cql_error, Code, Message}}, StreamId, State);

parse_frame(?OP_AUTH_CHALLENGE, #ecql_auth_challenge{}, StreamId, State) ->
    stop(self()),
    response({error, auth_challenge}, StreamId, State);

parse_frame(?OP_AUTH_SUCCESS, #ecql_auth_success{}, StreamId, State) ->
    response(ok, StreamId, State);

parse_frame(?OP_SUPPORTED, Data, StreamId, State) ->
    #ecql_supported{options = Options} = Data,
    response({ok, Options}, StreamId, State);

parse_frame(?OP_RESULT, Msg, StreamId, State) ->
    #ecql_result{kind = Kind, data = Data} = Msg,
    received_result(Kind, Data, StreamId, State).


%% @private
received_result(void, _Data, StreamId, State) ->
    response(ok, StreamId, State);

received_result(rows, #ecql_rows{meta = Meta, data = Rows}, StreamId, State) ->
    #ecql_rows_meta{columns = Columns, table_spec = TableSpec} = Meta,
    response({ok, {TableSpec, Columns, Rows}}, StreamId, State);

received_result(set_keyspace, #ecql_set_keyspace{keyspace = Keyspace}, StreamId, State) ->
    response({ok, Keyspace}, StreamId, State);

received_result(prepared, #ecql_prepared{id = Id}, StreamId, State) ->
    #state{prepared = Prepared} = State,
    Prepared2 = case maps:take({pending, StreamId}, Prepared) of
        {Name, Prepared0} ->
            Prepared0#{Name => Id};
        error ->
            Prepared
    end,
    response({ok, Id}, StreamId, State#state{prepared = Prepared2});

received_result(schema_change, Data, StreamId, State) ->
    #ecql_schema_change{type = Type, target = Target, options = Options} = Data,
    response({ok, {Type, Target, Options}}, StreamId, State);

received_result(_OpCode, Resp, StreamId, State) ->
    response({error, {unknown_code, _OpCode, Resp}}, StreamId, State).


%% @private
request(Frame, From, NkPort, #state{trans=Trans, next_stream_id = StreamId}=State) ->
    Bin = nkcassandra_frame:serialize(Frame#ecql_frame{stream=StreamId}),
    Timer = case From of
        undefined ->
            undefined;
        _ ->
            erlang:send_after(?OP_TIMEOUT, self(), {op_timeout, StreamId})
    end,
    Op = #trans{
        stream_id = StreamId,
        timer = Timer,
        from = From
    },
    % Size seems to be always 1 at maximum
    Trans2 = Trans#{StreamId => Op},
    State2 = incr_stream_id(State#state{trans=Trans2}),
    %lager:error("NKLOG SEND REQUEST (~p) ~p ~p ~p", [self(), StreamId, From, map_size(Trans2)]),
    send(Bin, NkPort, State2).


%% @private
response(Reply, StreamId, #state{trans=Trans}=State) ->
    Trans2 = case maps:take(StreamId, Trans) of
        {#trans{from=undefined}, Trans0} ->
            Trans0;
        {#trans{timer=Timer, from=From}, Trans0} ->
            nklib_util:cancel_timer(Timer),
            gen_server:reply(From, Reply),
            Trans0;
        error ->
            ?LLOG(warning, "Op not found on response: ~p (~p)", [StreamId, Reply], State),
            Trans
    end,
    {ok, State#state{trans=Trans2}}.


%% @private
send(Bin, NkPort, State) ->
    case nkpacket_connection:send(NkPort, Bin) of
        ok ->
            {ok, State};
        {error, Error} ->
            ?LLOG(warning, "error sending: ~p", [Error], State),
            {stop, normal, State}
    end.


%% @private
incr_stream_id(#state{next_stream_id = 16#ffff}=State) ->
    State#state{next_stream_id = 1};

incr_stream_id(#state{next_stream_id = Id}=State) ->
    State#state{next_stream_id = Id + 1}.


%% @private
encode_values(undefined) ->
    undefined;

encode_values(Values) ->
    encode_value(Values, []).


%% @private
encode_value([], Acc) ->
    lists:reverse(Acc);

encode_value([V|Values], Acc) when is_binary(V); is_atom(V); is_list(V) ->
    encode_value(Values, [nkcassandra_types:encode(V) | Acc]);

encode_value([{NameOrType, Val} | Values], Acc) ->
    case nkcassandra_types:value(NameOrType) of
        undefined ->
            encode_value(Values, [{to_bin(NameOrType), nkcassandra_types:encode(Val)} | Acc]);
        _ ->
            encode_value(Values, [nkcassandra_types:encode(NameOrType, Val) | Acc])
    end.


auth_token(#{user:=User}=Opts) ->
    Pass = maps:get(password, Opts, <<>>),
    <<0, (to_bin(User))/binary, 0, (to_bin(Pass))/binary>>;

auth_token(_) ->
    <<>>.


%% @private
to_bin(Term) when is_binary(Term) -> Term;
to_bin(Term) -> nklib_util:to_binary(Term).

