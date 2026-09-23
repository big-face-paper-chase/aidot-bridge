#!/usr/bin/env python3
"""
AiDot LAN bridge — runs on a device inside your home network (e.g. an old
Android phone in Termux) and exposes your AiDot/Linkind lights over HTTP so
Moose can control them remotely (over Tailscale).

Setup (Termux on the home device):
    pkg update && pkg install python -y
    pip install python-aidot aiohttp
    termux-wake-lock            # keep it alive in the background
    python bridge.py --email you@example.com --token <pick-a-secret>
    # password is prompted securely, never stored

Then tell Moose: the token you picked + this device's Tailscale IP.
"""
import argparse
import asyncio
import getpass
import json
import logging
import sys
from typing import Any, Dict, Optional

import aiohttp
from aiohttp import web

from aidot.client import AidotClient
from aidot.device_client import DeviceClient
from aidot.discover import Discover
from aidot.const import CONF_ID, CONF_NAME, CONF_IPADDRESS
from aidot import exceptions as aidot_exc

logging.basicConfig(
    level=logging.WARNING,
    format="%(asctime)s %(levelname)s %(message)s",
)
log = logging.getLogger("aidot-bridge")

DISCOVER_WAIT_S = 20
REDISCOVER_EVERY_S = 5 * 60


class Bridge:
    def __init__(self, email: str, password: str):
        self.email = email
        self.password = password
        self.client: Optional[AidotClient] = None
        self.session: Optional[aiohttp.ClientSession] = None
        self.login_info: Dict[str, Any] = {}
        self.devices: Dict[str, Dict[str, Any]] = {}        # dev_id -> cloud device dict
        self.clients: Dict[str, DeviceClient] = {}          # dev_id -> live client
        self.ips: Dict[str, str] = {}                       # dev_id -> LAN ip
        self._discover: Optional[Discover] = None

    # ---------- cloud ----------

    async def cloud_login(self) -> None:
        self.session = aiohttp.ClientSession()
        self.client = AidotClient(
            self.session, country_code="US",
            username=self.email, password=self.password,
        )
        try:
            self.login_info = await self.client.async_post_login()
        except aidot_exc.AidotUserOrPassIncorrect:
            print("ERROR: AiDot rejected the email/password. Check them and retry.",
                  file=sys.stderr)
            sys.exit(2)
        result = await self.client.async_get_all_device()
        for dev in result.get("device_list", []):
            self.devices[dev[CONF_ID]] = dev
        names = [d.get(CONF_NAME, d[CONF_ID]) for d in self.devices.values()]
        print(f"Cloud login OK. {len(self.devices)} device(s): {names}", flush=True)
        if not self.devices:
            print("No devices on this AiDot account. Add lights in the AiDot app first.",
                  file=sys.stderr)

    # ---------- LAN discovery ----------

    def _discover_cb(self, dev_id: str, event: Dict[str, str]) -> None:
        ip = event.get(CONF_IPADDRESS)
        if ip and self.ips.get(dev_id) != ip:
            log.warning("discovered %s at %s", dev_id, ip)
            self.ips[dev_id] = ip

    async def discover_round(self, wait_s: int = DISCOVER_WAIT_S) -> None:
        if self._discover is None:
            self._discover = Discover(self.login_info, self._discover_cb)
            await self._discover.try_create_broadcast()
        self._discover.start_repeat_broadcast()
        await asyncio.sleep(wait_s)

    # ---------- device connections ----------

    async def connect_all(self) -> None:
        for dev_id, dev in self.devices.items():
            ip = self.ips.get(dev_id)
            if not ip:
                print(f"  - {dev.get(CONF_NAME, dev_id)}: not found on LAN yet", flush=True)
                continue
            await self.connect_one(dev_id, ip)

    async def connect_one(self, dev_id: str, ip: str) -> bool:
        dev = self.devices[dev_id]
        try:
            dc = DeviceClient(dev, self.login_info)
            await dc.connect(ip)
            self.clients[dev_id] = dc
            print(f"  + {dev.get(CONF_NAME, dev_id)} connected ({ip})", flush=True)
            return True
        except Exception as e:  # noqa: BLE001
            log.warning("connect %s failed: %s", dev_id, e)
            return False

    async def ensure_connected(self, dev_id: str) -> DeviceClient:
        dc = self.clients.get(dev_id)
        if dc is not None and dc.status.online:
            return dc
        ip = self.ips.get(dev_id)
        if not ip:
            raise RuntimeError("light is not reachable on the home network right now")
        if await self.connect_one(dev_id, ip):
            return self.clients[dev_id]
        raise RuntimeError("could not reconnect to the light")

    async def rediscover_loop(self) -> None:
        while True:
            await asyncio.sleep(REDISCOVER_EVERY_S)
            try:
                await self.discover_round(wait_s=15)
                for dev_id, ip in self.ips.items():
                    dc = self.clients.get(dev_id)
                    if dc is None or not dc.status.online:
                        await self.connect_one(dev_id, ip)
            except Exception as e:  # noqa: BLE001
                log.warning("rediscover failed: %s", e)

    # ---------- state / control ----------

    def find(self, ident: str) -> str:
        """Resolve a light by id or (case-insensitive) name. Returns dev_id."""
        ident_l = ident.lower()
        if ident in self.devices:
            return ident
        matches = [d for d in self.devices.values()
                   if ident_l in str(d.get(CONF_NAME, "")).lower()]
        if len(matches) == 1:
            return matches[0][CONF_ID]
        if len(matches) > 1:
            names = [m.get(CONF_NAME) for m in matches]
            raise ValueError(f"ambiguous name, matches: {names}")
        raise ValueError(f"no light matching '{ident}'")

    def state_of(self, dev_id: str) -> Dict[str, Any]:
        dev = self.devices[dev_id]
        dc = self.clients.get(dev_id)
        st = dc.status if dc else None
        r, g, b, w = st.rgbw if st else (0, 0, 0, 0)
        return {
            "id": dev_id,
            "name": dev.get(CONF_NAME),
            "model": dev.get("modelId"),
            "online": bool(st and st.online),
            "on": bool(st and st.on),
            "brightness": round((st.dimming / 255) * 100) if st else 0,
            "color": {"r": r, "g": g, "b": b, "w": w},
            "temperature_k": st.cct if st else None,
        }

    async def cmd(self, dev_id: str, action: str,
                 value: Any = None) -> Dict[str, Any]:
        dc = await self.ensure_connected(dev_id)
        try:
            if action == "on":
                await dc.async_turn_on()
            elif action == "off":
                await dc.async_turn_off()
            elif action == "brightness":
                await dc.async_set_brightness(int(value * 255 / 100))
            elif action == "color":
                r, g, b = value["r"], value["g"], value["b"]
                w = int(value.get("w", 0))
                await dc.async_set_rgbw((int(r), int(g), int(b), w))
            elif action == "temperature":
                await dc.async_set_cct(int(value))
            else:
                raise ValueError(f"unknown action {action}")
        except ConnectionError:
            # one reconnect + retry
            ip = self.ips.get(dev_id)
            if ip and await self.connect_one(dev_id, ip):
                return await self.cmd(dev_id, action, value)
            raise RuntimeError("light went offline")
        await asyncio.sleep(0.4)  # let status sync
        return self.state_of(dev_id)


# ---------- HTTP API ----------

def make_app(bridge: Bridge, token: str) -> web.Application:
    app = web.Application()

    @web.middleware
    async def auth(request: web.Request, handler):
        if request.path == "/health":
            return await handler(request)
        authz = request.headers.get("Authorization", "")
        if authz != f"Bearer {token}":
            return web.json_response({"error": "unauthorized"}, status=401)
        return await handler(request)

    app.middlewares.append(auth)

    async def health(_):
        return web.json_response({"ok": True,
                                  "lights": len(bridge.devices),
                                  "online": sum(1 for c in bridge.clients.values()
                                                if c.status.online)})

    async def list_lights(_):
        return web.json_response(
            [bridge.state_of(d) for d in bridge.devices])

    async def one_light(request):
        try:
            dev_id = bridge.find(request.match_info["ident"])
        except ValueError as e:
            return web.json_response({"error": str(e)}, status=404)
        return web.json_response(bridge.state_of(dev_id))

    async def do_action(request):
        try:
            dev_id = bridge.find(request.match_info["ident"])
        except ValueError as e:
            return web.json_response({"error": str(e)}, status=404)
        action = request.match_info["action"]
        try:
            body = await request.json() if request.can_read_body else {}
        except json.JSONDecodeError:
            body = {}
        try:
            if action == "on":
                state = await bridge.cmd(dev_id, "on")
            elif action == "off":
                state = await bridge.cmd(dev_id, "off")
            elif action == "brightness":
                v = float(body.get("value", 100))
                state = await bridge.cmd(dev_id, "brightness",
                                         max(1, min(100, v)))
            elif action == "color":
                state = await bridge.cmd(dev_id, "color", {
                    "r": int(body["r"]), "g": int(body["g"]),
                    "b": int(body["b"]), "w": int(body.get("w", 0))})
            elif action == "temperature":
                state = await bridge.cmd(dev_id, "temperature",
                                         int(body["kelvin"]))
            else:
                return web.json_response(
                    {"error": f"unknown action '{action}'"}, status=400)
        except (ValueError, RuntimeError) as e:
            return web.json_response({"error": str(e)}, status=502)
        return web.json_response(state)

    app.router.add_get("/health", health)
    app.router.add_get("/lights", list_lights)
    app.router.add_get("/lights/{ident}", one_light)
    app.router.add_post("/lights/{ident}/{action}", do_action)
    return app


async def main() -> None:
    ap = argparse.ArgumentParser(description="AiDot LAN bridge")
    ap.add_argument("--email", required=True, help="AiDot account email")
    ap.add_argument("--token", required=True, help="secret token Moose uses")
    ap.add_argument("--port", type=int, default=8080)
    args = ap.parse_args()

    password = getpass.getpass("AiDot password (not stored, not shown): ")

    bridge = Bridge(args.email, password)
    await bridge.cloud_login()
    print("Discovering lights on your WiFi (takes ~20s)...", flush=True)
    await bridge.discover_round()
    await bridge.connect_all()

    asyncio.create_task(bridge.rediscover_loop())

    app = make_app(bridge, args.token)
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "0.0.0.0", args.port)
    await site.start()
    print(f"Bridge live on port {args.port}. Keep this Termux session running.",
          flush=True)
    print("In the Tailscale app, note this device's IP (100.x.x.x) and send it",
          "to Moose along with your --token value.", flush=True)
    await asyncio.Event().wait()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
