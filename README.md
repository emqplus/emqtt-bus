emqtt-bus
=========

A broker-less MQTT messaging bus for distributed real-time applications.

Build
-----

    $ make

Run
---
    $ make run

Pub/Sub
-------

Open console 1:

```erlang
emqtt_bus:subscribe("t").
```

Open console 2:

```erlang
emqtt_bus:publish("t", "p").
```

Query peers on any console:

```erlang
emqtt_bus:peers().
```

Domain
------

Each MQTT bus is physically isolated in a domain, which is identified by the domain ID.

Create a MQTT bus in domain 1:

```erlang
emqtt_bus:open(1).
emqtt_bus:subscribe(1, "t").
emqtt_bus:publish(1, "t", "p").
```
Create a MQTT bus in domain 1:

```erlang
emqtt_bus:open(2).
emqtt_bus:subscribe(2, "t").
emqtt_bus:publish(2, "t", "p").
```

Router
------

A router routes messages between different MQTT buses.

```erlang
emqtt_bus_router:start_link(1, 2).
```

Bridge
------

A bridge routes messages between an MQTT bus and an MQTT broker.

```erlang
emqtt_bus_bridge:start_link(1, [{host, "broker.emqx.io"}]).
emqtt_bus:publish(1, "t", "d").
```

Use mqttx to connect to `broker.emqx.io` and subscribe to the "t" topic, you will receive messages published from the MQTT bus 1.

