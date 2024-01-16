PROJECT_NAME = emqtt_bus

all: compile

compile:
	rebar3 compile

clean:
	rebar3 clean

run:
	rebar3 shell --apps emqtt_bus

test:
	rebar3 eunit

release:
	rebar3 release

.PHONY: all compile clean run test release
