import asyncio
import itertools
import socket

from telemetry.bus.base import TOPIC_TELEMETRY
from telemetry.bus.memory import MemoryBus
from telemetry.edge import devices, mqtt
from telemetry.edge.gateway import EdgeGateway


def _free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def test_devices_to_gateway_over_mqtt():
    async def run():
        port = _free_port()
        url = f"mqtt://127.0.0.1:{port}"
        broker = await mqtt.start_broker(port)
        bus = MemoryBus()
        gw = EdgeGateway("PL01", bus.producer())
        stop, ready = asyncio.Event(), asyncio.Event()
        task = asyncio.create_task(mqtt.bridge(gw, url, stop, ready))
        await ready.wait()
        ems = [e for e in itertools.islice(devices.simulate(5), 3000) if e.plant_id in ("PL01", "NL01")]
        await mqtt.publish(ems, url)
        expected = sum(e.plant_id == "PL01" for e in ems)
        for _ in range(100):
            if gw.stats.received >= expected:
                break
            await asyncio.sleep(0.1)
        stop.set()
        await task
        gw.forward()
        await broker.shutdown()
        return expected, gw.stats.received, bus.size(TOPIC_TELEMETRY)

    expected, received, on_bus = asyncio.run(run())
    assert expected > 100 and received == expected  # the gateway only subscribes to its own plant
    assert on_bus == received
