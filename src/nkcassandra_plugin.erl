%% -------------------------------------------------------------------
%%
%% Copyright (c) 2017 Carlos Gonzalez Florido.  All Rights Reserved.
%%
%% This file is provided to you under the Apache License,
%% Version 2.0 (the "License"); you may not use this file
%% except in compliance with the License.  You may obtain
%% a copy of the License at
%%
%%   http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing,
%% software distributed under the License is distributed on an
%% "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
%% KIND, either express or implied.  See the License for the
%% specific language governing permissions and limitations
%% under the License.
%%
%% -------------------------------------------------------------------

%% @doc NkCASSANDRA callbacks

-module(nkcassandra_plugin).
-author('Carlos Gonzalez <carlosj.gf@gmail.com>').

-export([plugin_deps/0, plugin_syntax/0, plugin_config/2, plugin_start/2, plugin_stop/2]).

-include("nkcassandra.hrl").
-include_lib("nkservice/include/nkservice.hrl").
-include_lib("brod/include/brod.hrl").


%% ===================================================================
%% Plugin callbacks
%%
%% These are used when NkCASSANDRA is started as a NkSERVICE plugin
%% ===================================================================


%% @doc
plugin_deps() ->
    [].


%% @doc
plugin_syntax() ->
	#{
	    nkcassandra =>
            {list, #{
                id => new_atom,
                nodes => {list, #{
                    host => host,
                    port => {integer, 1, 65535},
                    '__defaults' => #{host=><<"127.0.0.1">>, port=>9042}
                }},
                keyspace => binary,
                '__mandatory' => [id, nodes]
           }}
}.


%% @doc
plugin_config(#{nkcassandra:=List}=Config, #{id:=SrvId}) ->
    case parse_clusters(SrvId, List, #{}) of
        {ok, Clusters} ->
            {ok, Config#{nkcassandra_clusters=>Clusters}};
        {error, Error} ->
            {error, Error}
    end;

plugin_config(Config, _Service) ->
    {ok, Config}.


%% @doc
plugin_start(#{nkcassandra_clusters:=Clients}=Config, _Service) ->
    case start_clusters(maps:to_list(Clients)) of
        ok ->
            {ok, Config};
        {error, Error} ->
            {error, Error}
    end;

plugin_start(Config, _Service) ->
    {ok, Config}.


%% @doc
plugin_stop(#{nkcassandra_clusters:=_Clients}=Config, _Service) ->
    {ok, Config};

plugin_stop(Config, _Service) ->
    {ok, Config}.





%% ===================================================================
%% Util
%% ===================================================================

%% @private
parse_clusters(_SrvId, [], Acc) ->
    {ok, Acc};

parse_clusters(SrvId, [#{id:=Id, nodes:=Nodes} = Map|Rest], Acc) ->
    case maps:is_key(Id, Acc) of
        false ->
            Nodes2 = [{Host, Port} || #{host:=Host, port:=Port} <- Nodes],
            Opts = case Map of
                #{keyspace:=KeySpace} ->
                    [{keyspace, binary_to_atom(KeySpace, utf8)}];
                _ ->
                    []
            end,
            parse_clusters(SrvId, Rest, Acc#{Id=>{Nodes2, Opts}});
        true ->
            {error, duplicated_id}
    end.



%% @private
%% This operation only add the nodes to a cluster id in cqerl_cluster
%% There is currently no way to remove nodes,
%% we can do exit(pid(cqerl_cluster), kill) and add nodes again, but we will remove from all instances
%% of the service
%% In any case it seems to be used only for clients when start
%% Monitoring and everything is done at clients
%%
%% If we change the nodes, even if we restart the service, old nodes will remain!
%% Even if we stop the service, the clients will remain connected

start_clusters([]) ->
    ok;

start_clusters([{Id, {Hosts, Opts}}|Rest]) ->
    case cqerl_cluster:add_nodes(Id, Hosts, Opts) of
        ok ->
            lager:notice("NkCassandra configured cluster '~s' (~p, ~p)", [Id, Hosts, Opts]),
            start_clusters(Rest);
        Other ->
            {error, {could_not_start_cqerl_cluster, Other}}
    end.


