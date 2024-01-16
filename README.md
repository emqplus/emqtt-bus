emqtt-bus
=========

A broker-less MQTT messaging bus for distributed real-time applications.

Build
-----

    $ rebar3 compile

Run
---
    $ make run

PubSub
------

Open terminal 1:

```erlang
emqtt_bus:subscribe("t").
```

Open terminal 2:

```erlang
emqtt_bus:publish("t", "p").
```

Query peers on any terminal:

```erlang
emqtt_bus:peers().
```
