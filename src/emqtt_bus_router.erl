%%-------------------------------------------------------------------------
%% Copyright (c) 2023 EMQ Technologies Co., Ltd. All Rights Reserved.
%%
%% Licensed under the Apache License, Version 2.0 (the "License");
%% you may not use this file except in compliance with the License.
%% You may obtain a copy of the License at
%%
%%     http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing, software
%% distributed under the License is distributed on an "AS IS" BASIS,
%% WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
%% See the License for the specific language governing permissions and
%% limitations under the License.
%%-------------------------------------------------------------------------

-module(emqtt_bus_router).

-include("emqtt_bus.hrl").

-export([ start_link/2
		, stop/1
		]).

-export([domains/1]).

%% gen_server Callbacks
-export([ init/1
        , handle_call/3
        , handle_cast/2
        , handle_info/2
        , terminate/2
        , code_change/3
        ]).

-record(state, {domainId1, bus1, domainId2, bus2}).

start_link(DomainId1, DomainId2)
  when ?IS_DOMAIN_ID(DomainId1) andalso ?IS_DOMAIN_ID(DomainId2) ->
	gen_server:start_link({local, name(DomainId1, DomainId2)}, ?MODULE, [DomainId1, DomainId2], []).

name(DomainId1, DomainId2) ->
	list_to_atom("MQTT-BUS-ROUTER:" ++ integer_to_list(DomainId1) ++ "-" ++ integer_to_list(DomainId2)).

domains(Router) ->
	gen_server:call(Router, domains).

stop(Router) ->
	gen_server:stop(Router).

init([DomainId1, DomainId2]) ->
	{ok, Bus1} = emqtt_bus:ensure_open(DomainId1),
	{ok, Bus2} = emqtt_bus:ensure_open(DomainId2),
	ok = emqtt_bus:controlling_process(Bus1, self()),
	ok = emqtt_bus:controlling_process(Bus2, self()),
	ok = emqtt_bus:subscribe(Bus1, <<"#">>),
	ok = emqtt_bus:subscribe(Bus2, <<"#">>),
	{ok, #state{domainId1 = DomainId1, bus1 = Bus1, domainId2 = DomainId2, bus2 = Bus2}}.

handle_call(domains, _From, State = #state{bus1 = Bus1, bus2 = Bus2}) ->
	{reply, [emqtt_bus:domain(Bus) || Bus <- [Bus1, Bus2]], State};

handle_call(Req, _From, State) ->
    logger:error("Unexpected call: ~p", [Req]),
    {reply, ignore, State}.

handle_cast(Msg, State) ->
    logger:error("Unexpected cast: ~p", [Msg]),
    {noreply, State}.

%% terminate the publish loop
handle_info({publish, _Bus, <<"domain/", _Topic/binary>>, _Payload}, State) ->
    {noreply, State};

handle_info({publish, Bus1, Topic, Payload},
			State = #state{domainId1 = DomainId1, bus1 = Bus1, bus2 = Bus2}) ->
	Bus2 ! {publish, with_domain(DomainId1, Topic), Payload},
    {noreply, State};

handle_info({publish, Bus2, Topic, Payload},
			State = #state{bus1 = Bus1, domainId2 = DomainId2, bus2 = Bus2}) ->
	Bus1 ! {publish, with_domain(DomainId2, Topic), Payload},
    {noreply, State};

handle_info(Info, State) ->
    logger:error("Unexpected info: ~p", [Info]),
    {noreply, State}.

terminate(_Reason, #state{bus1 = Bus1, bus2 = Bus2}) ->
	emqtt_bus:stop(Bus1),
	emqtt_bus:stop(Bus2).

code_change(_OldVsn, State, _Extra) ->
	{ok, State}.

with_domain(DomainId, Topic) ->
	iolist_to_binary(["domain/", integer_to_list(DomainId), "/", Topic]).

