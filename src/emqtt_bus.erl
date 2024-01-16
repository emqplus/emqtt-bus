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

-module(emqtt_bus).

-include("emqtt_bus.hrl").

-export([ name/1
		, open/1
		, domain/1
		, peers/1
		, subscriptions/1
		, publish/3
		, subscribe/2
		, close/1
		]).

-export([ ensure_open/1
		, start_link/1
		, controlling_process/2
		, stop/1
		]).

%% gen_server Callbacks
-export([ init/1
		, handle_continue/2
        , handle_call/3
        , handle_cast/2
        , handle_info/2
        , terminate/2
        , code_change/3
        ]).

-record(domain, {id, addr, ports}).
-record(state, {sock, domain, controlling_process,
				peers = [], topic_filters = [], subscriptions = []}).

-define(BASE_PORT, 10000).
-define(PORT_NUM, 10).
-define(MULTICAST_ADDR, {224,18,83,0}).

name(DomainId) when ?IS_DOMAIN_ID(DomainId) ->
	list_to_atom("MQTT-BUS:" ++ integer_to_list(DomainId)).

open(DomainId) when ?IS_DOMAIN_ID(DomainId) ->
	start_link(DomainId).

domain(DomainId) when ?IS_DOMAIN_ID(DomainId) ->
	domain(name(DomainId));
domain(Bus) ->
	gen_server:call(Bus, domain).

peers(DomainId) when ?IS_DOMAIN_ID(DomainId) ->
	peers(name(DomainId));
peers(Bus) ->
	gen_server:call(Bus, peers).

subscriptions(DomainId) when ?IS_DOMAIN_ID(DomainId) ->
	subscriptions(name(DomainId));
subscriptions(Bus) ->
	gen_server:call(Bus, subscriptions).

publish(DomainId, Topic, Payload) when ?IS_DOMAIN_ID(DomainId) ->
	publish(name(DomainId), Topic, Payload);
publish(Bus, Topic, Payload) ->
	gen_server:call(Bus, {publish, iolist_to_binary(Topic), iolist_to_binary(Payload)}).

subscribe(DomainId, Topic) when ?IS_DOMAIN_ID(DomainId) ->
	subscribe(name(DomainId), Topic);
subscribe(Bus, Topic) ->
	gen_server:call(Bus, {subscribe, iolist_to_binary(Topic)}).

close(DomainId) when ?IS_DOMAIN_ID(DomainId) ->
	stop(name(DomainId)).

ensure_open(DomainId) ->
	case start_link(DomainId) of
		{ok, Pid} -> {ok, Pid};
		{error, {already_started, Pid}} ->
			{ok, Pid};
        {error, Reason} -> {error, Reason}
	end.

start_link(DomainId) ->
    gen_server:start_link({local, name(DomainId)}, ?MODULE, [DomainId], []).

controlling_process(Bus, Pid) ->
	gen_server:call(Bus, {controlling_process, Pid}).

stop(Bus) ->
	gen_server:stop(Bus).

init([DomainId]) ->
	BasePort = ?BASE_PORT + ?PORT_NUM * DomainId,
	Ports = lists:seq(BasePort, BasePort + ?PORT_NUM - 1),
	MulticastAddr = multicast_addr(DomainId),
	Domain = #domain{id = DomainId, addr = MulticastAddr, ports = Ports},
	case emqtt_bus_udp:open(Ports, MulticastAddr) of
		{ok, Sock} ->
			State = #state{domain = Domain, sock = Sock},
			{ok, State, {continue, discover}};
        {error, Error} -> {stop, Error}
    end.

multicast_addr(DomainId) ->
    {A, B, C, D} = ?MULTICAST_ADDR, {A, B, C, D + DomainId}.

handle_continue(discover, State = #state{sock = Sock, domain = #domain{addr = Addr, ports = Ports}}) ->
	Packet = ?PACKET(?PINGREQ),
    lists:foreach(fun(Port) ->
					  emqtt_bus_udp:send(Sock, Addr, Port, Packet)
				  end, Ports),
	{noreply, State};

handle_continue({send_subscribe, _Addr, _Port}, State = #state{topic_filters = []}) ->
	{noreply, State};

handle_continue({send_subscribe, Addr, Port}, State = #state{sock = Sock, topic_filters = TopicFilters}) ->
	Packet = ?SUBSCRIBE_PACKET(1, [{Filter, #{qos => ?QOS_0}} || Filter <- TopicFilters]),
	emqtt_bus_udp:send(Sock, Addr, Port, Packet),
	{noreply, State}.

handle_call({controlling_process, Pid}, _From, State) ->
	{reply, ok, State#state{controlling_process = Pid}};

handle_call(Publish = {publish, _Topic, _Payload}, _From, State) ->
	handle_info(Publish, State),
    {reply, ok, State};

handle_call({subscribe, TopicFilter}, _From, State = #state{sock = Sock, peers = Peers, topic_filters = TopicFilters}) ->
    Packet = ?SUBSCRIBE_PACKET(1, [{TopicFilter, #{qos => 0}}]),
    lists:foreach(fun({Addr, Port}) ->
					  emqtt_bus_udp:send(Sock, Addr, Port, Packet)
                  end, Peers),
    {reply, ok, State#state{topic_filters = lists:usort([TopicFilter|TopicFilters])}};

handle_call(domain, _From, State = #state{domain = Domain}) ->
	{reply, Domain, State};

handle_call(peers, _From, State = #state{peers = Peers}) ->
	{reply, Peers, State};

handle_call(subscriptions, _From, State = #state{subscriptions = Subscriptions}) ->
	{reply, Subscriptions, State};

handle_call(Req, _From, State) ->
    logger:error("Unexpected call: ~p", [Req]),
    {reply, ignore, State}.

handle_cast(Msg, State) ->
    logger:error("Unexpected cast: ~p", [Msg]),
    {noreply, State}.

handle_info({publish, Topic, Payload}, State = #state{sock = Sock, subscriptions = Subscriptions}) ->
    Packet = ?PUBLISH_PACKET(?QOS_0, Topic, undefined, Payload),
	lists:foreach(fun({Filter, {Addr, Port}}) ->
					  case emqtt_bus_topic:match(Topic, Filter) of
						  true -> emqtt_bus_udp:send(Sock, Addr, Port, Packet);
						  false -> ok
					  end
				  end, Subscriptions),
    {noreply, State};

handle_info({udp, Sock, Ip, InPort, Data}, State = #state{sock = Sock}) ->
	try emqtt_frame:parse(Data) of
		{ok, Packet, _, _} ->
		   handle_incoming(Ip, InPort, Packet, State)
	catch
		_:_ ->
			logger:error("Bad Packet: ~p", [Data]),
			{noreply, State}
	end;

handle_info({udp_closed, Sock}, State = #state{sock = Sock}) ->
    {stop, udp_closed, State};

handle_info(Info, State) ->
    logger:error("Unexpected info: ~p", [Info]),
    {noreply, State}.

terminate(_Reason, #state{sock = Sock}) ->
    emqtt_bus_udp:close(Sock).

code_change(_OldVsn, State, _Extra) ->
	{ok, State}.

handle_incoming(Addr, Port, ?PACKET(?PINGREQ), State = #state{sock = Sock}) ->
	io:format("PINGREQ from ~s:~p~n", [inet:ntoa(Addr), Port]),
	emqtt_bus_udp:send(Sock, Addr, Port, ?PACKET(?PINGRESP)),
	{noreply, State};

handle_incoming(Addr, Port, ?PACKET(?PINGRESP), State) ->
	io:format("PINGRESP from ~s:~p~n", [inet:ntoa(Addr), Port]),
	{noreply, add_peer({Addr, Port}, State), {continue, {send_subscribe, Addr, Port}}};

handle_incoming(Addr, Port, ?PUBLISH_PACKET(_QoS, Topic, _PacketId, Payload), State) ->
	io:format("PUBLISH from ~s:~p -> ~p: ~p~n", [inet:ntoa(Addr), Port, Topic, Payload]),
	case State#state.controlling_process of
		undefined -> ok;
		Pid -> Pid ! {publish, self(), Topic, Payload}
	end,
	{noreply, State};

handle_incoming(Addr, Port, ?SUBSCRIBE_PACKET(PacketId, TopicFilters), State = #state{sock = Sock}) ->
	io:format("SUBSCRIBE from ~s:~p: ~p~n", [inet:ntoa(Addr), Port, TopicFilters]),
	ReasonCodes = [QoS || {_, #{qos := QoS}} <- TopicFilters],
	emqtt_bus_udp:send(Sock, Addr, Port, ?SUBACK_PACKET(PacketId, ReasonCodes)),
	Subscriptions = [{Filter, {Addr, Port}} || {Filter, _} <- TopicFilters],
	{noreply, add_subscriptions(Subscriptions, State)};

handle_incoming(Addr, Port, ?SUBACK_PACKET(_PacketId, ReasonCodes), State) ->
	io:format("SUBACK from ~s:~p: ~p~n", [inet:ntoa(Addr), Port, ReasonCodes]),
	{noreply, State};

handle_incoming(Addr, Port, Packet, State) ->
	io:format("Unknown Packet from ~s:~p: ~p~n", [inet:ntoa(Addr), Port, Packet]),
	{noreply, State}.

add_peer(Peer, State = #state{peers = Peers}) ->
	State#state{peers = lists:usort([Peer | Peers])}.

add_subscriptions(New, State = #state{subscriptions = Existed}) ->
	State#state{subscriptions = lists:usort(New ++ Existed)}.

