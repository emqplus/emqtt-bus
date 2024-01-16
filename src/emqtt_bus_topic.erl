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

-module(emqtt_bus_topic).

%% APIs
-export([
    match/2,
    match_any/2,
    levels/1,
    tokens/1,
    words/1,
    wildcard/1,
    join/1,
    prepend/2,
    parse/1,
    parse/2
]).

-define(SHARE, "$share").
-define(QUEUE, "$queue").
-define(SHARE(Group, Topic), join([<<?SHARE>>, Group, Topic])).

-record(share, {group, topic}).

%% Guards
-define(IS_TOPIC(T),
    (is_binary(T) orelse is_record(T, share))
).

-define(MULTI_LEVEL_WILDCARD_NOT_LAST(C, REST),
    ((C =:= '#' orelse C =:= <<"#">>) andalso REST =/= [])
).

%% @doc Is wildcard topic?
wildcard(#share{topic = Topic}) when is_binary(Topic) ->
    wildcard(Topic);
wildcard(Topic) when is_binary(Topic) ->
    wildcard(words(Topic));
wildcard([]) ->
    false;
wildcard(['#' | _]) ->
    true;
wildcard(['+' | _]) ->
    true;
wildcard([_H | T]) ->
    wildcard(T).

%% @doc Match Topic name with filter.
match(<<$$, _/binary>>, <<$+, _/binary>>) ->
    false;
match(<<$$, _/binary>>, <<$#, _/binary>>) ->
    false;
match(Name, Filter) when is_binary(Name), is_binary(Filter) ->
    match(words(Name), words(Filter));
match(#share{} = Name, Filter) ->
    match_share(Name, Filter);
match(Name, #share{} = Filter) ->
    match_share(Name, Filter);
match([], []) ->
    true;
match([H | T1], [H | T2]) ->
    match(T1, T2);
match([_H | T1], ['+' | T2]) ->
    match(T1, T2);
match([<<>> | T1], ['' | T2]) ->
    match(T1, T2);
match(_, ['#']) ->
    true;
match(_, _) ->
    false.

match_share(#share{topic = Name}, Filter) when is_binary(Filter) ->
    %% only match real topic filter for normal topic filter.
    match(words(Name), words(Filter));
match_share(#share{group = Group, topic = Name}, #share{group = Group, topic = Filter}) ->
    %% Matching real topic filter When subed same share group.
    match(words(Name), words(Filter));
match_share(#share{}, _) ->
    %% Otherwise, non-matched.
    false;
match_share(Name, #share{topic = Filter}) when is_binary(Name) ->
    %% Only match real topic filter for normal topic_filter/topic_name.
    match(Name, Filter).

match_any(Topic, Filters) ->
    lists:any(fun(Filter) -> match(Topic, Filter) end, Filters).

%% @doc Prepend a topic prefix.
%% Ensured to have only one / between prefix and suffix.
prepend(undefined, W) ->
    bin(W);
prepend(<<>>, W) ->
    bin(W);
prepend(Parent0, W) ->
    Parent = bin(Parent0),
    case binary:last(Parent) of
        $/ -> <<Parent/binary, (bin(W))/binary>>;
        _ -> <<Parent/binary, $/, (bin(W))/binary>>
    end.

bin('') -> <<>>;
bin('+') -> <<"+">>;
bin('#') -> <<"#">>;
bin(B) when is_binary(B) -> B;
bin(L) when is_list(L) -> list_to_binary(L).

levels(#share{topic = Topic}) when is_binary(Topic) ->
    levels(Topic);
levels(Topic) when is_binary(Topic) ->
    length(tokens(Topic)).

-compile({inline, [tokens/1]}).
%% @doc Split topic to tokens.
tokens(Topic) ->
    binary:split(Topic, <<"/">>, [global]).

%% @doc Split Topic Path to Words
words(#share{topic = Topic}) when is_binary(Topic) ->
    words(Topic);
words(Topic) when is_binary(Topic) ->
    [word(W) || W <- tokens(Topic)].

word(<<>>) -> '';
word(<<"+">>) -> '+';
word(<<"#">>) -> '#';
word(Bin) -> Bin.

join([]) ->
    <<>>;
join([Word | Words]) ->
    do_join(bin(Word), Words).

do_join(TopicAcc, []) ->
    TopicAcc;
%% MQTT-5.0 [MQTT-4.7.1-1]
do_join(_TopicAcc, [C | Words]) when ?MULTI_LEVEL_WILDCARD_NOT_LAST(C, Words) ->
    error('topic_invalid_#');
do_join(TopicAcc, [Word | Words]) ->
    do_join(<<TopicAcc/binary, "/", (bin(Word))/binary>>, Words).

parse(TopicFilter) when is_binary(TopicFilter) ->
    parse(TopicFilter, #{});
parse({TopicFilter, Options}) when is_binary(TopicFilter) ->
    parse(TopicFilter, Options).

%% <<"$queue/[real_topic_filter]>">> equivalent to <<"$share/$queue/[real_topic_filter]">>
%% So the head of `real_topic_filter` MUST NOT be `<<$queue>>` or `<<$share>>`
parse(#share{topic = Topic = <<?QUEUE, "/", _/binary>>}, _Options) ->
    error({invalid_topic_filter, Topic});
parse(#share{topic = Topic = <<?SHARE, "/", _/binary>>}, _Options) ->
    error({invalid_topic_filter, Topic});
parse(<<?QUEUE, "/", Topic/binary>>, Options) ->
    parse(#share{group = <<?QUEUE>>, topic = Topic}, Options);
parse(TopicFilter = <<?SHARE, "/", Rest/binary>>, Options) ->
    case binary:split(Rest, <<"/">>) of
        [_Any] ->
            error({invalid_topic_filter, TopicFilter});
        %% `Group` could be `$share` or `$queue`
        [Group, Topic] ->
            case binary:match(Group, [<<"+">>, <<"#">>]) of
                nomatch -> parse(#share{group = Group, topic = Topic}, Options);
                _ -> error({invalid_topic_filter, TopicFilter})
            end
    end;
parse(TopicFilter = <<"$exclusive/", Topic/binary>>, Options) ->
    case Topic of
        <<>> ->
            error({invalid_topic_filter, TopicFilter});
        _ ->
            {Topic, Options#{is_exclusive => true}}
    end;
parse(TopicFilter, Options) when
    ?IS_TOPIC(TopicFilter)
->
    {TopicFilter, Options}.
