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


-module(nkcassandra_types).

-include("ecql.hrl").
-include("ecql_types.hrl").

-export([consistency_value/1, consistency_name/1]).
-export([name/1, value/1, encode/1, encode/2, decode/2, to_bytes/1, from_bytes/1]).
-export([test/0]).

%%------------------------------------------------------------------------------
%% Type Name
%%------------------------------------------------------------------------------


%% @doc
consistency_value(any)          -> ?CL_ANY;
consistency_value(one)          -> ?CL_ONE;
consistency_value(two)          -> ?CL_TWO;
consistency_value(three)        -> ?CL_THREE;
consistency_value(quorum)       -> ?CL_QUORUM;
consistency_value(all)          -> ?CL_ALL;
consistency_value(local_quorum) -> ?CL_LOCAL_QUORUM;
consistency_value(each_quorum)  -> ?CL_EACH_QUORUM;
consistency_value(serial)       -> ?CL_SERIAL;
consistency_value(local_serial) -> ?CL_LOCAL_SERIAL;
consistency_value(local_one)    -> ?CL_LOCAL_ONE.


%% @doc
consistency_name(?CL_ANY)          -> any;
consistency_name(?CL_ONE)          -> one;
consistency_name(?CL_TWO)          -> two;
consistency_name(?CL_THREE)        -> three;
consistency_name(?CL_QUORUM)       -> quorum;
consistency_name(?CL_ALL)          -> all;
consistency_name(?CL_LOCAL_QUORUM) -> local_quorum;
consistency_name(?CL_EACH_QUORUM)  -> each_quorum;
consistency_name(?CL_SERIAL)       -> serial;
consistency_name(?CL_LOCAL_SERIAL) -> local_serial;
consistency_name(?CL_LOCAL_ONE)    -> local_one.


%% @doc
name(?TYPE_CUSTOM)   -> custom;
name(?TYPE_ASCII)    -> ascii;
name(?TYPE_BIGINT)   -> bigint;
name(?TYPE_BLOB)     -> blob;
name(?TYPE_BOOLEAN)  -> boolean;
name(?TYPE_COUNTER)  -> counter;
name(?TYPE_DECIMAL)  -> decimal;
name(?TYPE_DOUBLE)   -> double;
name(?TYPE_FLOAT)    -> float;
name(?TYPE_INT)      -> int;
name(?TYPE_TIMESTAMP)-> timestamp;
name(?TYPE_UUID)     -> uuid;
name(?TYPE_VARCHAR)  -> varchar;
name(?TYPE_VARINT)   -> varint;
name(?TYPE_TIMEUUID) -> timeuuid;
name(?TYPE_INET)     -> inet;
name(?TYPE_LIST)     -> list;
name(?TYPE_MAP)      -> map;
name(?TYPE_SET)      -> set;
name(?TYPE_UDT)      -> udt;
name(?TYPE_TUPLE)    -> tuple.


%% @doc
value(custom)        -> ?TYPE_CUSTOM;
value(ascii)         -> ?TYPE_ASCII;
value(bigint)        -> ?TYPE_BIGINT;
value(blob)          -> ?TYPE_BLOB;
value(boolean)       -> ?TYPE_BOOLEAN;
value(counter)       -> ?TYPE_COUNTER;
value(decimal)       -> ?TYPE_DECIMAL;
value(double)        -> ?TYPE_DOUBLE;
value(float)         -> ?TYPE_FLOAT;
value(int)           -> ?TYPE_INT;
value(timestamp)     -> ?TYPE_TIMESTAMP;
value(uuid)          -> ?TYPE_UUID;
value(varchar)       -> ?TYPE_VARCHAR;
value(varint)        -> ?TYPE_VARINT;
value(timeuuid)      -> ?TYPE_TIMEUUID;
value(inet)          -> ?TYPE_INET;
value(list)          -> ?TYPE_LIST;
value(map)           -> ?TYPE_MAP;
value(set)           -> ?TYPE_SET;
value(udt)           -> ?TYPE_UDT;
value(tuple)         -> ?TYPE_TUPLE;
value(_)             -> unknown.


%%------------------------------------------------------------------------------
%% Encode
%%------------------------------------------------------------------------------

%% @doc
encode(A) when is_atom(A) ->
    encode(text, atom_to_list(A));

encode(L) when is_list(L) ->
    encode(text, L);

encode(B) when is_binary(B) ->
    encode(text, B);

encode({Type, Val}) when is_atom(Type) ->
    encode(Type, Val).


%% @doc
encode(ascii, Bin) ->
    Bin;

encode(bigint, BigInt) ->
    <<BigInt:?LONG>>;

encode(blob, Bin) ->
    Bin;

encode(boolean, false) ->
    <<0>>;

encode(boolean, true) ->
    <<1>>;

encode(counter, Counter) ->
    <<Counter:?LONG>>;

encode(decimal, {Unscaled, Scale}) ->
    <<Scale:?INT, (encode(varint, Unscaled))/binary>>;

encode(double, Double) ->
    <<Double:?DOUBLE>>;

encode(float, Float) ->
    <<Float:?FLOAT>>;

encode(inet, {A, B, C, D}) ->
    <<A, B, C, D>>;

encode(inet, {A, B, C, D, E, F, G, H}) ->
    <<A:?SHORT, B:?SHORT, C:?SHORT, D:?SHORT,
        E:?SHORT, F:?SHORT, G:?SHORT, H:?SHORT>>;

encode(int, Int) ->
    <<Int:?INT>>;

encode({list, Type}, List) ->
    ListBin = <<
        <<(to_bytes(encode(Type, Term)))/binary>>
        || Term <- List
    >>,
    <<(length(List)):?INT, ListBin/binary>>;

encode({map, {KType, VType}}, Map) ->
    Encode = fun(Key, Val) ->
        KeyBin = to_bytes(encode(KType, Key)),
        ValBin = to_bytes(encode(VType, Val)),
        <<KeyBin/binary, ValBin/binary>>
    end,
    MapBin = << <<(Encode(Key, Val))/binary>> || {Key, Val} <- Map >>,
    <<(length(Map)):?INT, MapBin/binary>>;

encode({set, Type}, Set) ->
    encode({list, Type}, lists:usort(Set));

encode(text, Str) when is_list(Str) ->
    list_to_binary(Str);

encode(text, Bin) ->
    Bin;

encode(timestamp, TS) ->
    <<TS:?LONG>>;

encode(uuid, UUID) ->
    <<UUID:16/binary>>;

encode(varchar, Str) when is_list(Str) ->
    list_to_binary(Str);

encode(varchar, Bin) ->
    Bin;

encode(varint, Val) ->
    ByteCount = count_bytes(Val, 0),
    << Val:ByteCount/big-signed-integer-unit:8 >>;

encode(timeuuid, UUID) ->
    <<UUID:16/binary>>;

% encode({tuple, {int, text}}, {1, <<"text">>})
encode({tuple, Types}, Tuple) ->
    <<
        <<(to_bytes(encode(element(Pos, Types), element(Pos, Tuple))))/binary>>
        || Pos <- lists:seq(1, size(Types))
    >>.


%% @doc
to_bytes(Bin) ->
    <<(size(Bin)):?INT, Bin/binary>>.


%%------------------------------------------------------------------------------
%% Decode
%%------------------------------------------------------------------------------


%% @doc
decode(ascii, Bin) ->
    Bin;

decode(bigint, <<Bigint:?LONG>>) ->
    Bigint;

decode(blob, Bin) ->
    Bin;

decode(boolean, <<0>>) ->
    false;

decode(boolean, <<_>>) ->
    true;

decode(counter, <<Counter:?LONG>>) ->
    Counter;

decode(decimal, <<Scale:?INT, Bin/binary>>) ->
    Unscaled = decode(varint, Bin),
    {Unscaled, Scale};

decode(double, <<Double:?DOUBLE>>) ->
    Double;

decode(float, <<Float:?FLOAT>>) ->
    Float;

decode(inet, <<A, B, C, D>>) ->
    {A, B, C, D};

decode(inet, <<A:?SHORT, B:?SHORT, C:?SHORT, D:?SHORT, E:?SHORT, F:?SHORT, G:?SHORT, H:?SHORT>>) ->
    {A, B, C, D, E, F, G, H};

decode(int, <<Int:?INT>>) ->
    Int;

decode({list, Type}, <<_Len:?INT, Bin/binary>>) ->
    [
        decode(Type, TermBin)
        || <<Size:?INT, TermBin:Size/binary>> <= Bin
    ];

decode({map, {KeyType, ValType}}, <<_Len:?INT, Bin/binary>>) ->
    [
        {decode(KeyType, KeyBin), decode(ValType, ValBin)}
        ||
        <<
            KeySize:?INT,
            KeyBin:KeySize/binary,
            ValSize:?INT,
            ValBin:ValSize/binary
        >> <= Bin
    ];

decode({set, Type}, Bin) ->
    lists:usort(decode({list, Type}, Bin));

decode(text, Text) ->
    Text;

decode(timestamp, <<TS:?LONG>>) ->
    TS;

decode(uuid, <<UUID:16/binary>>) ->
    UUID;

decode(varchar, Bin) ->
    Bin;

decode(varint, Bin) ->
    Size = byte_size(Bin),
    <<VarInt:Size/big-signed-integer-unit:8>> = Bin,
    VarInt;

decode(timeuuid, <<UUID:16/binary>>) ->
    UUID;

decode({tuple, Types}, Bin) ->
    Elements = [TermBin || <<Size:?INT, TermBin:Size/binary>> <= Bin],
    {_, List} = lists:foldl(
        fun(TermBin, {Pos, Acc}) ->
            {Pos+1, [decode(element(Pos, Types), TermBin)|Acc]}
        end,
        {1, []},
        Elements),
    list_to_tuple(lists:reverse(List)).


%% @doc
from_bytes(<<Size:?INT, Bin:Size/binary, Rest/binary>>) ->
    {Bin, Rest}.


%% @private
%% From cqerl_datatypes.erl
count_bytes(X, Acc) when X =< 127, X >=    0 -> Acc + 1;
count_bytes(X, Acc) when X >  127, X <   256 -> Acc + 2;
count_bytes(X, Acc) when X <  0,   X >= -128 -> Acc + 1;
count_bytes(X, Acc) when X < -128, X >= -256 -> Acc + 2;
count_bytes(X, Acc) -> count_bytes(X bsr 8, Acc + 1).


%% @private
test() ->
    [1,2,3] = decode({list, counter}, encode({list, counter}, [1,2,3])),
    [{1, <<"t1">>}, {2, <<"t2">>}] =
        decode({map, {int, text}}, encode({map, {int, text}}, [{1, <<"t1">>}, {2, <<"t2">>}])),
    [1,3,5] = decode({set, int}, encode({set, int}, [1, 5, 1, 3])),
    1234567890 = decode(varint, encode(varint, 1234567890)),
    {<<"t">>, 1, 2.0} =
        decode({tuple, {text, int, float}}, encode({tuple, {text, int, float}}, {<<"t">>, 1, 2.0})),
    ok.




