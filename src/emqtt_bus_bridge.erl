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

-module(emqtt_bus_bridge).

-include("emqtt_bus.hrl").

-export([ start_link/2
		, stop/1
		]).

%% gen_server Callbacks
-export([ init/1
        , handle_call/3
        , handle_cast/2
        , handle_info/2
        , terminate/2
        , code_change/3
        ]).

-record(state, {bus, client}).

start_link(DomainId, BrokerOpts)
  when ?IS_DOMAIN_ID(DomainId) andalso is_list(BrokerOpts) ->
	gen_server:start_link(?MODULE, [DomainId, BrokerOpts], []).

stop(Bridge) ->
	gen_server:stop(Bridge).

init([DomainId, BrokerOpts]) ->
	{ok, Bus} = emqtt_bus:ensure_open(DomainId),
	{ok, Client} = emqtt:start_link(BrokerOpts),
	{ok, _} = emqtt:connect(Client),
	ok = emqtt_bus:controlling_process(Bus, self()),
	ok = emqtt_bus:subscribe(Bus, <<"#">>),
	_ = emqtt:subscribe(Client, <<"mqtt-bus/#">>),
	{ok, #state{bus = Bus, client = Client}}.

handle_call(Req, _From, State) ->
    logger:error("Unexpected call: ~p", [Req]),
    {reply, ignore, State}.

handle_cast(Msg, State) ->
    logger:error("Unexpected cast: ~p", [Msg]),
    {noreply, State}.

handle_info({publish, Bus, Topic, Payload},
			State = #state{bus = Bus, client = Client}) ->
	emqtt:publish(Client, Topic, Payload),
    {noreply, State};

handle_info({publish, #{topic := <<"mqtt-bus/", Topic/binary>>,
						payload := Payload}}, State = #state{bus = Bus}) ->
	Bus ! {publish, Topic, Payload},
    {noreply, State};

handle_info(Info, State) ->
    logger:error("Unexpected info: ~p", [Info]),
    {noreply, State}.

terminate(_Reason, #state{bus = Bus, client = Client}) ->
	emqtt_bus:stop(Bus),
	emqtt:stop(Client).

code_change(_OldVsn, State, _Extra) ->
	{ok, State}.

