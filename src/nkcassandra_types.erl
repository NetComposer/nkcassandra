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
-export([name/1, value/1, is_type/1, encode/1, encode/2,
    decode/2, decode/3, to_bytes/1, from_bytes/1]).

%%------------------------------------------------------------------------------
%% Type Name
%%------------------------------------------------------------------------------


%% @private
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


%%%% @private
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
value(tuple)         -> ?TYPE_TUPLE.

is_type(T) ->
    try value(T) of _I -> true catch error:_ -> false end.

%%------------------------------------------------------------------------------
%% Encode
%%------------------------------------------------------------------------------

encode(A) when is_atom(A) ->
    encode(text, atom_to_list(A));

encode(L) when is_list(L) ->
    encode(text, L);

encode(B) when is_binary(B) ->
    encode(text, B);

encode({Type, Val}) when is_atom(Type) ->
    encode(Type, Val).

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

encode({list, ElType}, List) ->
    Encode = fun(El) -> to_bytes(encode(ElType, El)) end,
    ListBin = << <<(Encode(El))/binary>> || El <- List >>,
    <<(length(List)):?INT, ListBin/binary>>;

encode({map, {KType, VType}}, Map) ->
    Encode = fun(Key, Val) ->
        KeyBin = to_bytes(encode(KType, Key)),
        ValBin = to_bytes(encode(VType, Val)),
        <<KeyBin/binary, ValBin/binary>>
    end,
    MapBin = << <<(Encode(Key, Val))/binary>> || {Key, Val} <- Map >>,
    <<(length(Map)):?INT, MapBin/binary>>;

encode({set, ElType}, Set) ->
    encode({list, ElType}, ordsets:to_list(ordsets:from_list(Set)));

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

encode(varint, Varint) when 16#80 > Varint andalso Varint >= -16#80 ->
    <<Varint:1/big-signed-unit:8>>;

encode(varint, Varint) ->
    <<Varint:2/big-signed-unit:8>>;

encode(timeuuid, UUID) ->
    <<UUID:16/binary>>;

encode({tuple, Types}, Tuple) ->
    L = lists:zip(tuple_to_list(Types), tuple_to_list(Tuple)),
    Encode = fun(Type, El) -> to_bytes(encode(Type, El)) end,
    << <<(Encode(Type, El))/binary>> || {Type, El} <- L >>.

to_bytes(Bin) ->
    <<(size(Bin)):?INT, Bin/binary>>.


%%------------------------------------------------------------------------------
%% Decode
%%------------------------------------------------------------------------------

decode(Type, Bin) ->
    decode(Type, size(Bin), Bin).

decode(ascii, Size, Bin) ->
    <<Ascii:Size/binary, Rest/binary>> = Bin, {Ascii, Rest};

decode(bigint, 8, <<Bigint:?LONG, Rest/binary>>) ->
    {Bigint, Rest};

decode(blob, Size, Bin) ->
    <<Blob:Size/binary, Rest/binary>> = Bin, {Blob, Rest};

decode(boolean, 1, <<0, Rest/binary>>) ->
    {false, Rest};

decode(boolean, 1, <<_, Rest/binary>>) ->
    {true, Rest};

decode(counter, 8, <<Counter:?LONG, Rest/binary>>) ->
    {Counter, Rest};

decode(decimal, Size, <<Scale:?INT, Bin/binary>>) ->
    {Unscaled, Rest} = decode(varint, Size - 4, Bin),
    {{Unscaled, Scale}, Rest};

decode(double, 8, <<Double:?DOUBLE, Rest/binary>>) ->
    {Double, Rest};

decode(float, 4, <<Float:?FLOAT, Rest/binary>>) ->
    {Float, Rest};

decode(inet, 4, <<A, B, C, D, Rest/binary>>) ->
    {{A, B, C, D}, Rest};

decode(inet, 16, <<A:?SHORT, B:?SHORT, C:?SHORT, D:?SHORT,
    E:?SHORT, F:?SHORT, G:?SHORT, H:?SHORT, Rest/binary>>) ->
    {{A, B, C, D, E, F, G, H}, Rest};

decode(int, 4, Bin) ->
    <<Int:?INT, Rest/binary>> = Bin, {Int, Rest};

decode({list, ElType}, Size, Bin) ->
    <<ListBin:Size/binary, Rest/binary>> = Bin, <<_Len:?INT, ElsBin/binary>> = ListBin,
    {[element(1, decode(ElType, ElSize, ElBin))
        || <<ElSize:?INT, ElBin:ElSize/binary>> <= ElsBin], Rest};

decode({map, {KeyType, ValType}}, Size, Bin) ->
    <<MapBin:Size/binary, Rest/binary>> = Bin, <<_Len:?INT, ElsBin/binary>> = MapBin,
    List = [ {decode(KeyType, KeySize, KeyBin), decode(ValType, ValSize, ValBin)}
        || << KeySize:?INT, KeyBin:KeySize/binary, ValSize:?INT, ValBin:ValSize/binary >> <= ElsBin ],
    {[{Key, Val} || {{Key, _}, {Val, _}} <- List], Rest};

decode({set,  ElType}, Size, Bin) ->
    {List, Rest} = decode({list, ElType}, Size, Bin),
    {ordsets:to_list(ordsets:from_list(List)), Rest};

decode(text, Size, Bin) ->
    <<Text:Size/binary, Rest/binary>> = Bin, {Text, Rest};

decode(timestamp, 8, <<TS:?LONG, Rest/binary>>) ->
    {TS, Rest};

decode(uuid, 16, <<UUID:16/binary, Rest/binary>>) ->
    {UUID, Rest};

decode(varchar, Size, Bin) ->
    <<Varchar:Size/binary, Rest/binary>> = Bin, {Varchar, Rest};

decode(varint, 1, <<Varint:1/big-signed-unit:8, Rest/binary>>) ->
    {Varint, Rest};

decode(varint, 2, <<Varint:2/big-signed-unit:8, Rest/binary>>) ->
    {Varint, Rest};

decode(timeuuid, 16, <<UUID:16/binary, Rest/binary>>) ->
    {UUID, Rest};

decode({tuple, ElTypes}, Size, Bin) ->
    <<TupleBin:Size/binary, Rest/binary>> = Bin,
    Elements = [{ElSize, ElBin} || <<ElSize:?INT, ElBin:ElSize/binary>> <= TupleBin],
    List = [decode(ElType, ElSize, ElBin) || {ElType, {ElSize, ElBin}}
        <- lists:zip(tuple_to_list(ElTypes), Elements)],
    {list_to_tuple([El || {El, _} <- List]), Rest}.

from_bytes(<<Size:?INT, Bin:Size/binary, Rest/binary>>) ->
    {Bin, Rest}.

