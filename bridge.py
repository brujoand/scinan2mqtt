import asyncio
import json
import logging
import os
import time
from dataclasses import dataclass, field

import aiomqtt

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("scinan-mqtt-bridge")

MQTT_HOST = os.environ["MQTT_HOST"]
MQTT_PORT = int(os.environ.get("MQTT_PORT", "1883"))
MQTT_USER = os.environ.get("MQTT_USER")
MQTT_PASS = os.environ.get("MQTT_PASS")
TOPIC_PREFIX = os.environ.get("TOPIC_PREFIX", "scinan")
DISCOVERY_PREFIX = os.environ.get("DISCOVERY_PREFIX", "homeassistant")
# The thermostat pushes its status every ~15 s unprompted, so a minute of
# silence means the TCP session is dead even if the socket has not noticed.
SILENCE_TIMEOUT = int(os.environ.get("SILENCE_TIMEOUT", "60"))
RECONNECT_DELAY = int(os.environ.get("RECONNECT_DELAY", "10"))
# The thermostat closes a session after ~30 s without traffic from the client.
KEEPALIVE_INTERVAL = int(os.environ.get("KEEPALIVE_INTERVAL", "10"))

BRIDGE_AVAILABILITY = f"{TOPIC_PREFIX}/bridge/availability"


@dataclass
class Thermostat:
    name: str
    host: str
    port: int = 2000
    device_id: str | None = None
    min_temp: float = 5.0
    max_temp: float = 35.0
    writer: asyncio.StreamWriter | None = field(default=None, repr=False)
    discovered: bool = False
    last_status: float = float("-inf")

    @property
    def base(self) -> str:
        return f"{TOPIC_PREFIX}/{self.device_id}"

    def send(self, sensor: str, value: str) -> None:
        if self.writer is None or self.device_id is None:
            raise ConnectionError(f"{self.name}: not connected")
        self.writer.write(f"/{self.device_id}/{sensor}/1/{value}\n".encode())


def parse_thermostats(spec: str) -> list[Thermostat]:
    """THERMOSTATS is `name=host[:port]` entries separated by commas."""
    thermostats = []
    for entry in filter(None, (e.strip() for e in spec.split(","))):
        name, _, address = entry.partition("=")
        host, _, port = address.partition(":")
        if not name or not host:
            raise ValueError(f"bad THERMOSTATS entry: {entry!r}")
        thermostats.append(
            Thermostat(name=name.strip(), host=host.strip(), port=int(port or 2000))
        )
    if not thermostats:
        raise ValueError("THERMOSTATS is empty")
    return thermostats


def parse_status(data: str) -> dict:
    """Decode the S00 payload of a type 9 thermostat.

    Field order, from the vendor app's parseThermostatAllStatus:
    power, measured, target, unit, away, program, fan, run mode, system mode,
    min, max. Only the fields below have a verified meaning.
    """
    fields = data.split(",")
    if len(fields) < 5:
        raise ValueError(f"short status: {data!r}")
    power = fields[0] == "1"
    status = {
        "power": power,
        "mode": "heat" if power else "off",
        "current_temperature": float(fields[1]),
        "target_temperature": float(fields[2]),
        "away": fields[4] == "1",
    }
    if len(fields) > 5:
        status["program"] = fields[5] == "1"
    if len(fields) > 10:
        status["min_temp"] = float(fields[9])
        status["max_temp"] = float(fields[10])
    return status


def discovery_payload(thermostat: Thermostat) -> dict:
    state = f"{thermostat.base}/state"
    return {
        "name": None,
        "unique_id": f"scinan_{thermostat.device_id}",
        "device": {
            "identifiers": [f"scinan_{thermostat.device_id}"],
            "name": thermostat.name,
            "manufacturer": "Scinan",
            "model": "WiFi thermostat",
            "connections": [
                [
                    "mac",
                    ":".join(
                        thermostat.device_id[i : i + 2] for i in range(4, 16, 2)
                    ).lower(),
                ]
            ],
        },
        "availability": [
            {"topic": BRIDGE_AVAILABILITY},
            {"topic": f"{thermostat.base}/availability"},
        ],
        "availability_mode": "all",
        "modes": ["off", "heat"],
        "mode_state_topic": state,
        "mode_state_template": "{{ value_json.mode }}",
        "mode_command_topic": f"{thermostat.base}/set/mode",
        "temperature_state_topic": state,
        "temperature_state_template": "{{ value_json.target_temperature }}",
        "temperature_command_topic": f"{thermostat.base}/set/target_temperature",
        "current_temperature_topic": state,
        "current_temperature_template": "{{ value_json.current_temperature }}",
        "min_temp": thermostat.min_temp,
        "max_temp": thermostat.max_temp,
        "temp_step": 0.5,
        "temperature_unit": "C",
    }


async def handle_line(
    client: aiomqtt.Client, thermostat: Thermostat, line: str
) -> None:
    parts = line.split("/", 4)
    if len(parts) != 5:
        log.debug("%s: ignoring %r", thermostat.name, line)
        return
    _, device_id, command, _, data = parts

    if command == "type":
        if thermostat.device_id not in (None, device_id):
            raise ConnectionError(
                f"{thermostat.name}: device id changed from {thermostat.device_id} to {device_id}"
            )
        thermostat.device_id = device_id
        log.info("%s: connected to %s (type %s)", thermostat.name, device_id, data)
        thermostat.send("S00", "-1")
    elif command == "S00" and thermostat.device_id == device_id:
        status = parse_status(data)
        thermostat.last_status = time.monotonic()
        thermostat.min_temp = status.get("min_temp", thermostat.min_temp)
        thermostat.max_temp = status.get("max_temp", thermostat.max_temp)
        if not thermostat.discovered:
            topic = f"{DISCOVERY_PREFIX}/climate/scinan_{device_id}/config"
            await client.publish(
                topic, json.dumps(discovery_payload(thermostat)), qos=1, retain=True
            )
            thermostat.discovered = True
        await client.publish(
            f"{thermostat.base}/state", json.dumps(status), qos=1, retain=True
        )
        await client.publish(
            f"{thermostat.base}/availability", "online", qos=1, retain=True
        )


async def thermostat_session(client: aiomqtt.Client, thermostat: Thermostat) -> None:
    reader, writer = await asyncio.wait_for(
        asyncio.open_connection(thermostat.host, thermostat.port), 10
    )
    thermostat.writer = writer
    keepalive = asyncio.create_task(send_keepalive(thermostat))
    try:
        writer.write(b"/SCINANIOT/DEVICE_REQUEST\n")
        while True:
            raw = await asyncio.wait_for(reader.readline(), SILENCE_TIMEOUT)
            if not raw:
                raise ConnectionError("closed by thermostat")
            await handle_line(client, thermostat, raw.decode(errors="replace").strip())
    finally:
        keepalive.cancel()
        thermostat.writer = None
        writer.close()


async def send_keepalive(thermostat: Thermostat) -> None:
    while True:
        await asyncio.sleep(KEEPALIVE_INTERVAL)
        if thermostat.writer is not None and thermostat.device_id is not None:
            thermostat.send("S00", "-1")


async def run_thermostat(client: aiomqtt.Client, thermostat: Thermostat) -> None:
    # The thermostat serves one TCP client at a time, so this bridge must be the
    # only thing talking to it.
    while True:
        try:
            await thermostat_session(client, thermostat)
        except (OSError, TimeoutError, ValueError) as err:
            log.warning(
                "%s (%s): %s",
                thermostat.name,
                thermostat.host,
                str(err) or type(err).__name__,
            )
        if time.monotonic() - thermostat.last_status < SILENCE_TIMEOUT:
            await asyncio.sleep(1)
            continue
        if thermostat.device_id:
            await client.publish(
                f"{thermostat.base}/availability", "offline", qos=1, retain=True
            )
        await asyncio.sleep(RECONNECT_DELAY)


def handle_command(thermostats: list[Thermostat], topic: str, payload: str) -> None:
    _, device_id, _, setting = topic.rsplit("/", 3)
    thermostat = next((t for t in thermostats if t.device_id == device_id), None)
    if thermostat is None:
        log.warning("command for unknown device %s", device_id)
        return
    try:
        if setting == "target_temperature":
            target = round(float(payload) * 2) / 2
            if not thermostat.min_temp <= target <= thermostat.max_temp:
                raise ValueError(
                    f"{target} outside {thermostat.min_temp}-{thermostat.max_temp}"
                )
            thermostat.send("S02", f"{target:.1f}")
        elif setting == "mode":
            thermostat.send("S01", {"heat": "1", "off": "0"}[payload])
        else:
            raise ValueError(f"unknown setting {setting!r}")
        log.info("%s: set %s to %s", thermostat.name, setting, payload)
    except (ValueError, KeyError, ConnectionError) as err:
        log.warning("%s: rejected %s=%r: %s", thermostat.name, setting, payload, err)


async def run_bridge(thermostats: list[Thermostat]) -> None:
    will = aiomqtt.Will(BRIDGE_AVAILABILITY, "offline", qos=1, retain=True)
    async with aiomqtt.Client(
        MQTT_HOST, MQTT_PORT, username=MQTT_USER, password=MQTT_PASS, will=will
    ) as client:
        log.info("connected to mqtt at %s:%s", MQTT_HOST, MQTT_PORT)
        await client.publish(BRIDGE_AVAILABILITY, "online", qos=1, retain=True)
        await client.subscribe(f"{TOPIC_PREFIX}/+/set/+", qos=1)
        async with asyncio.TaskGroup() as tasks:
            for thermostat in thermostats:
                thermostat.discovered = False
                tasks.create_task(run_thermostat(client, thermostat))
            async for message in client.messages:
                handle_command(
                    thermostats,
                    message.topic.value,
                    message.payload.decode(errors="replace").strip(),
                )


async def main() -> None:
    thermostats = parse_thermostats(os.environ["THERMOSTATS"])
    while True:
        try:
            await run_bridge(thermostats)
        except* aiomqtt.MqttError as errors:
            log.warning("mqtt: %s", "; ".join(str(e) for e in errors.exceptions))
        await asyncio.sleep(RECONNECT_DELAY)


if __name__ == "__main__":
    asyncio.run(main())
