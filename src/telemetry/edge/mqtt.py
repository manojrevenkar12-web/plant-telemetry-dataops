"""MQTT transport between line devices and the plant edge gateway.

Devices publish to `plant/<plant>/line/<line>/<device>/<metric>` with QoS 1 (at-least-once, so
duplicates are expected and removed downstream). The gateway subscribes to `plant/<plant>/#`
and hands each payload to `EdgeGateway.handle`. In a plant the broker is Mosquitto or the
broker built into the OT platform; here it is amqtt so the whole path runs in one process.
"""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Iterable
from datetime import UTC, datetime

from amqtt.broker import Broker
from amqtt.client import MQTTClient

from .devices import Emission
from .gateway import EdgeGateway

logging.getLogger("amqtt").setLevel(logging.WARNING)
logging.getLogger("transitions").setLevel(logging.WARNING)


def broker_config(port: int) -> dict:
    return {
        "listeners": {"default": {"type": "tcp", "bind": f"127.0.0.1:{port}"}},
        "plugins": {"amqtt.plugins.authentication.AnonymousAuthPlugin": {"allow_anonymous": True}},
    }


async def start_broker(port: int) -> Broker:
    broker = Broker(broker_config(port))
    await broker.start()
    return broker


async def bridge(gateway: EdgeGateway, url: str, stop: asyncio.Event, ready: asyncio.Event | None = None) -> int:
    """Subscribe the gateway to its plant's topics until `stop` is set. Returns messages handled."""
    client = MQTTClient(client_id=gateway.gateway_id)
    await client.connect(url)
    await client.subscribe([(f"plant/{gateway.plant_id}/#", 1)])
    if ready:
        ready.set()
    handled = 0
    try:
        while not stop.is_set():
            try:
                msg = await client.deliver_message(timeout_duration=0.2)
            except TimeoutError:
                continue
            if msg is None:
                continue
            gateway.handle(bytes(msg.data), datetime.now(UTC))
            handled += 1
    finally:
        await client.disconnect()
    return handled


async def publish(emissions: Iterable[Emission], url: str) -> int:
    client = MQTTClient(client_id="line-devices")
    await client.connect(url)
    n = 0
    for em in emissions:
        await client.publish(em.topic, em.payload, qos=1)
        n += 1
    await client.disconnect()
    return n
