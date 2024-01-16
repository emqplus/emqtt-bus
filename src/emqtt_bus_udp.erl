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

-module(emqtt_bus_udp).

-export([open/2, send/4, close/1]).

-define(DEFAULT_OPTS,
	   	[binary, {active, true},
		 {reuseaddr, true},
		 {multicast_if, {0,0,0,0}},
		 {multicast_ttl, 1},
		 {multicast_loop, true}
		]).

open(Ports, MulticastAddr) ->
	do_open(Ports, [{add_membership, {MulticastAddr, {0,0,0,0}}}] ++ ?DEFAULT_OPTS).

do_open([], _Opts) ->
    {error, eaddrinuse};
do_open([Port|More], Opts) ->
    case gen_udp:open(Port, Opts) of
        {ok, Sock} ->
			{ok, Sock};
        {error, eaddrinuse} ->
			do_open(More, Opts);
        {error, Reason} ->
            {error, Reason}
    end.

send(Sock, Addr, Port, Packet) ->
    gen_udp:send(Sock, Addr, Port, emqtt_frame:serialize(Packet)).

close(Sock) ->
	gen_udp:close(Sock).

