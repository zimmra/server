import asyncio
import datetime
import logging
import time
from datetime import timedelta
from http import HTTPStatus
from urllib import parse

import aiohttp
import xmltodict
from aiohttp import ClientError
from aiohttp.hdrs import CONNECTION, KEEP_ALIVE
from asyncio_throttle import Throttler

from music_assistant.common.models.enums import (
    PlayerState,
)
from music_assistant.common.models.errors import TimeoutError
from music_assistant.server import MusicAssistant

_LOGGER = logging.getLogger(__name__)

ATTR_BLUESOUND_GROUP = "bluesound_group"
ATTR_MASTER = "master"

DATA_BLUESOUND = "bluesound"
DEFAULT_PORT = 11000

NODE_OFFLINE_CHECK_TIMEOUT = 180

SYNC_STATUS_INTERVAL = timedelta(minutes=5).total_seconds()

UPDATE_CAPTURE_INTERVAL = timedelta(minutes=30).total_seconds()
UPDATE_PRESETS_INTERVAL = timedelta(minutes=30).total_seconds()
UPDATE_SERVICES_INTERVAL = timedelta(minutes=30).total_seconds()


class BluesoundPlayer:
    """Representation of a Bluesound Player."""

    _attr_volume_step = 1

    def __init__(self, host, port=None, name=None, mass: MusicAssistant = None):
        """Initialize the media player."""
        self.host = host
        self.port = port
        self.mass = mass
        self.player = None
        self._playback_started = None
        self._name = name
        self._id = None
        self._capture_items = []
        self._services_items = []
        self._preset_items = []
        self._sync_status = {}
        self._status = None
        self._last_status_update = None
        self._is_online = False
        self._retry_remove = None
        self._muted = False
        self._master = None
        self._is_master = False
        self._group_name = None
        self._group_list = []
        self._bluesound_device_name = None

        if self.port is None:
            self.port = DEFAULT_PORT

    @staticmethod
    def _try_get_index(string, search_string):
        """Get the index."""
        try:
            return string.index(search_string)
        except ValueError:
            return -1

    async def force_update_sync_status(self, on_updated_cb=None, raise_timeout=False):
        """Update the internal status."""
        resp = await self.send_bluesound_command("SyncStatus", raise_timeout, raise_timeout)

        if not resp:
            return None
        self._sync_status = resp["SyncStatus"].copy()

        if not self._name:
            self._name = self._sync_status.get("@name", self.host)
        if not self._id:
            self._id = self._sync_status.get("@id", None)
        if not self._bluesound_device_name:
            self._bluesound_device_name = self._sync_status.get("@name", self.host)

        # if (master := self._sync_status.get("master")) is not None:
        #     self._is_master = False
        #     master_host = master.get("#text")
        #     master_port = master.get("@port", "11000")
        #     master_id = f"{master_host}:{master_port}"
        #     master_device = [
        #         device for device in self._hass.data[DATA_BLUESOUND] if device.id == master_id
        #     ]

        #     if master_device and master_id != self.id:
        #         self._master = master_device[0]
        #     else:
        #         self._master = None
        #         _LOGGER.error("Master not found %s", master_id)
        # else:
        #     if self._master is not None:
        #         self._master = None
        #     slaves = self._sync_status.get("slave")
        #     self._is_master = slaves is not None

        if on_updated_cb:
            on_updated_cb()
        self._is_online = True
        return True

    async def async_init(self, triggered=None):
        """Initialize the player async."""
        try:
            await self.force_update_sync_status(None, True)
        except Exception:
            _LOGGER.exception("Unexpected when initiating error in %s:%s", self.host, self.port)
            raise

    async def async_update(self) -> None:
        """Update internal status of the entity."""
        if not self._is_online:
            return

        await self.async_update_sync_status()
        await self.async_update_status()
        self.update_attributes()

    async def send_bluesound_command(self, method, raise_timeout=False, allow_offline=False):
        """Send command to the player."""
        if not self._is_online and not allow_offline:
            return

        if method[0] == "/":
            method = method[1:]
        url = f"http://{self.host}:{self.port}/{method}"

        _LOGGER.debug("Calling URL: %s", url)
        response = None

        try:
            async with asyncio.timeout(10):
                response = await self.mass.http_session.get(url)

            if response.status == HTTPStatus.OK:
                result = await response.text()
                if result:
                    data = xmltodict.parse(result)
                else:
                    data = None
            elif response.status == 595:
                _LOGGER.info("Status 595 returned, treating as timeout")
                raise TimeoutError()
            else:
                _LOGGER.error("Error %s on %s", response.status, url)
                return None

        except (asyncio.TimeoutError, aiohttp.ClientError):
            if raise_timeout:
                _LOGGER.info("Timeout: %s:%s", self.host, self.port)
                raise
            _LOGGER.debug("Failed communicating: %s:%s", self.host, self.port)
            return None

        return data

    async def async_update_status(self):
        """Use the poll session to always get the status of the player."""
        response = None

        url = "Status"
        etag = ""
        if self._status is not None:
            etag = self._status.get("@etag", "")

        if etag != "":
            url = f"Status?etag={etag}&timeout=120.0"
        url = f"http://{self.host}:{self.port}/{url}"

        _LOGGER.debug("Calling URL: %s", url)

        try:
            async with asyncio.timeout(125):
                response = await self.mass.http_session.get(url, headers={CONNECTION: KEEP_ALIVE})

            if response.status == HTTPStatus.OK:
                result = await response.text()
                self._is_online = True
                self._last_status_update = datetime.datetime.now(datetime.UTC)
                self._status = xmltodict.parse(result)["status"].copy()

                group_name = self._status.get("groupName")
                if group_name != self._group_name:
                    _LOGGER.debug("Group name change detected on device: %s", self.id)
                    self._group_name = group_name

                    # rebuild ordered list of entity_ids that are in the group, master is first
                    self._group_list = self.rebuild_bluesound_group()

                    # the sleep is needed to make sure that the
                    # devices is synced
                    await asyncio.sleep(1)
                    await self.async_trigger_sync_on_all()
                elif self.is_grouped:
                    # when player is grouped we need to fetch volume from
                    # sync_status. We will force an update if the player is
                    # grouped this isn't a foolproof solution. A better
                    # solution would be to fetch sync_status more often when
                    # the device is playing. This would solve a lot of
                    # problems. This change will be done when the
                    # communication is moved to a separate library
                    await self.force_update_sync_status()
            elif response.status == 595:
                _LOGGER.info("Status 595 returned, treating as timeout")
                raise TimeoutError()
            else:
                _LOGGER.error("Error %s on %s. Trying one more time", response.status, url)

        except (asyncio.TimeoutError, ClientError):
            self._is_online = False
            self._last_status_update = None
            self._status = None
            self.async_write_ha_state()
            _LOGGER.info("Client connection error, marking %s as offline", self._name)
            raise

    @property
    def unique_id(self):
        """Return an unique ID."""
        return f"{self._sync_status['@mac']}-{self.port}"

    async def async_update_sync_status(self, on_updated_cb=None, raise_timeout=False):
        """Update sync status."""
        async with Throttler(rate_limit=1, period=SYNC_STATUS_INTERVAL):
            await self.force_update_sync_status(on_updated_cb, raise_timeout=False)

    @property
    def state(self) -> PlayerState:
        """Return the state of the device."""
        if self._status is None:
            return PlayerState.IDLE

        if self.is_grouped and not self.is_master:
            return PlayerState.IDLE

        status = self._status.get("state")
        if status in ("pause", "stop"):
            return PlayerState.PAUSED
        if status in ("stream", "play"):
            return PlayerState.PLAYING
        return PlayerState.IDLE

    @property
    def media_title(self):
        """Title of current playing media."""
        if self._status is None or (self.is_grouped and not self.is_master):
            return None

        return self._status.get("title1")

    @property
    def media_artist(self):
        """Artist of current playing media (Music track only)."""
        if self._status is None:
            return None

        if self.is_grouped and not self.is_master:
            return self._group_name

        if not (artist := self._status.get("artist")):
            artist = self._status.get("title2")
        return artist

    @property
    def media_album_name(self):
        """Artist of current playing media (Music track only)."""
        if self._status is None or (self.is_grouped and not self.is_master):
            return None

        if not (album := self._status.get("album")):
            album = self._status.get("title3")
        return album

    @property
    def media_image_url(self):
        """Image url of current playing media."""
        if self._status is None or (self.is_grouped and not self.is_master):
            return None

        if not (url := self._status.get("image")):
            return
        if url[0] == "/":
            url = f"http://{self.host}:{self.port}{url}"

        return url

    @property
    def media_position(self):
        """Position of current playing media in seconds."""
        if self._status is None or (self.is_grouped and not self.is_master):
            return None

        mediastate = self.state
        if self._last_status_update is None or mediastate == PlayerState.IDLE:
            return None

        if (position := self._status.get("secs")) is None:
            return None

        position = float(position)
        if mediastate == PlayerState.PLAYING:
            position += (
                datetime.datetime.now(datetime.UTC) - self._last_status_update
            ).total_seconds()

        return position

    @property
    def media_duration(self):
        """Duration of current playing media in seconds."""
        if self._status is None or (self.is_grouped and not self.is_master):
            return None

        if (duration := self._status.get("totlen")) is None:
            return None
        return float(duration)

    @property
    def media_position_updated_at(self):
        """Last time status was updated."""
        return self._last_status_update

    @property
    def volume_level(self):
        """Volume level of the media player (0..1)."""
        volume = self._status.get("volume")
        if self.is_grouped:
            volume = self._sync_status.get("@volume")

        if volume is not None:
            return int(volume) / 100
        return None

    @property
    def is_volume_muted(self):
        """Boolean if volume is currently muted."""
        mute = self._status.get("mute")
        if self.is_grouped:
            mute = self._sync_status.get("@mute")

        if mute is not None:
            mute = bool(int(mute))
        return mute

    @property
    def id(self):
        """Get id of device."""
        return self._id

    @property
    def name(self):
        """Return the name of the device."""
        return self._name

    @property
    def bluesound_device_name(self):
        """Return the device name as returned by the device."""
        return self._bluesound_device_name

    @property
    def source_list(self):
        """List of available input sources."""
        if self._status is None or (self.is_grouped and not self.is_master):
            return None

        sources = []

        for source in self._preset_items:
            sources.append(source["title"])

        for source in [
            x for x in self._services_items if x["type"] in ("LocalMusic", "RadioService")
        ]:
            sources.append(source["title"])

        for source in self._capture_items:
            sources.append(source["title"])

        return sources

    @property
    def source(self):
        """Name of the current input source."""
        if self._status is None or (self.is_grouped and not self.is_master):
            return None

        if (current_service := self._status.get("service", "")) == "":
            return ""
        stream_url = self._status.get("streamUrl", "")

        if self._status.get("is_preset", "") == "1" and stream_url != "":
            # This check doesn't work with all presets, for example playlists.
            # But it works with radio service_items will catch playlists.
            items = [
                x
                for x in self._preset_items
                if "url2" in x and parse.unquote(x["url2"]) == stream_url
            ]
            if items:
                return items[0]["title"]

        # This could be a bit difficult to detect. Bluetooth could be named
        # different things and there is not any way to match chooses in
        # capture list to current playing. It's a bit of guesswork.
        # This method will be needing some tweaking over time.
        title = self._status.get("title1", "").lower()
        if title == "bluetooth" or stream_url == "Capture:hw:2,0/44100/16/2":
            items = [x for x in self._capture_items if x["url"] == "Capture%3Abluez%3Abluetooth"]
            if items:
                return items[0]["title"]

        items = [x for x in self._capture_items if x["url"] == stream_url]
        if items:
            return items[0]["title"]

        if stream_url[:8] == "Capture:":
            stream_url = stream_url[8:]

        idx = BluesoundPlayer._try_get_index(stream_url, ":")
        if idx > 0:
            stream_url = stream_url[:idx]
            for item in self._capture_items:
                url = parse.unquote(item["url"])
                if url[:8] == "Capture:":
                    url = url[8:]
                idx = BluesoundPlayer._try_get_index(url, ":")
                if idx > 0:
                    url = url[:idx]
                if url.lower() == stream_url.lower():
                    return item["title"]

        items = [x for x in self._capture_items if x["name"] == current_service]
        if items:
            return items[0]["title"]

        items = [x for x in self._services_items if x["name"] == current_service]
        if items:
            return items[0]["title"]

        if self._status.get("streamUrl", "") != "":
            _LOGGER.debug(
                "Couldn't find source of stream URL: %s",
                self._status.get("streamUrl", ""),
            )
        return None

    @property
    def is_master(self):
        """Return true if player is a coordinator."""
        return self._is_master

    @property
    def is_grouped(self):
        """Return true if player is a coordinator."""
        return self._master is not None or self._is_master

    @property
    def shuffle(self):
        """Return true if shuffle is active."""
        return self._status.get("shuffle", "0") == "1"

    # async def async_join(self, master):
    #     """Join the player to a group."""
    #     master_device = [
    #         device for device in self.hass.data[DATA_BLUESOUND] if device.entity_id == master
    #     ]

    #     if master_device:
    #         _LOGGER.debug(
    #             "Trying to join player: %s to master: %s",
    #             self.id,
    #             master_device[0].id,
    #         )

    #         await master_device[0].async_add_slave(self)
    #     else:
    #         _LOGGER.error("Master not found %s", master_device)

    @property
    def extra_state_attributes(self):
        """List members in group."""
        attributes = {}
        if self._group_list:
            attributes = {ATTR_BLUESOUND_GROUP: self._group_list}

        attributes[ATTR_MASTER] = self._is_master

        return attributes

    def rebuild_bluesound_group(self):
        """Rebuild the list of entities in speaker group."""
        if self._group_name is None:
            return None

        bluesound_group = []

        device_group = self._group_name.split("+")

        # sorted_entities = sorted(
        #     self._hass.data[DATA_BLUESOUND],
        #     key=lambda entity: entity.is_master,
        #     reverse=True,
        # )
        # bluesound_group = [
        #     entity.name
        #     for entity in sorted_entities
        #     if entity.bluesound_device_name in device_group
        # ]

        return bluesound_group

    async def async_unjoin(self):
        """Unjoin the player from a group."""
        if self._master is None:
            return

        _LOGGER.debug("Trying to unjoin player: %s", self.id)
        await self._master.async_remove_slave(self)

    async def async_add_slave(self, slave_device):
        """Add slave to master."""
        return await self.send_bluesound_command(
            f"/AddSlave?slave={slave_device.host}&port={slave_device.port}"
        )

    async def async_remove_slave(self, slave_device):
        """Remove slave to master."""
        return await self.send_bluesound_command(
            f"/RemoveSlave?slave={slave_device.host}&port={slave_device.port}"
        )

    async def async_increase_timer(self):
        """Increase sleep time on player."""
        sleep_time = await self.send_bluesound_command("/Sleep")
        if sleep_time is None:
            _LOGGER.error("Error while increasing sleep time on player: %s", self.id)
            return 0

        return int(sleep_time.get("sleep", "0"))

    async def async_clear_timer(self):
        """Clear sleep timer on player."""
        sleep = 1
        while sleep > 0:
            sleep = await self.async_increase_timer()

    async def async_set_shuffle(self, shuffle: bool) -> None:
        """Enable or disable shuffle mode."""
        value = "1" if shuffle else "0"
        return await self.send_bluesound_command(f"/Shuffle?state={value}")

    async def async_select_source(self, source: str) -> None:
        """Select input source."""
        if self.is_grouped and not self.is_master:
            return

        items = [x for x in self._preset_items if x["title"] == source]

        if not items:
            items = [x for x in self._services_items if x["title"] == source]
        if not items:
            items = [x for x in self._capture_items if x["title"] == source]

        if not items:
            return

        selected_source = items[0]
        url = f"Play?url={selected_source['url']}&preset_id&image={selected_source['image']}"

        if "is_raw_url" in selected_source and selected_source["is_raw_url"]:
            url = selected_source["url"]

        return await self.send_bluesound_command(url)

    async def async_clear_playlist(self) -> None:
        """Clear players playlist."""
        if self.is_grouped and not self.is_master:
            return

        return await self.send_bluesound_command("Clear")

    async def async_media_next_track(self) -> None:
        """Send media_next command to media player."""
        if self.is_grouped and not self.is_master:
            return

        cmd = "Skip"
        if self._status and "actions" in self._status:
            for action in self._status["actions"]["action"]:
                if "@name" in action and "@url" in action and action["@name"] == "skip":
                    cmd = action["@url"]

        return await self.send_bluesound_command(cmd)

    async def async_media_previous_track(self) -> None:
        """Send media_previous command to media player."""
        if self.is_grouped and not self.is_master:
            return

        cmd = "Back"
        if self._status and "actions" in self._status:
            for action in self._status["actions"]["action"]:
                if "@name" in action and "@url" in action and action["@name"] == "back":
                    cmd = action["@url"]

        return await self.send_bluesound_command(cmd)

    async def async_media_play(self) -> None:
        """Send media_play command to media player."""
        if self.is_grouped and not self.is_master:
            return

        return await self.send_bluesound_command("Play")

    async def async_media_pause(self) -> None:
        """Send media_pause command to media player."""
        if self.is_grouped and not self.is_master:
            return

        return await self.send_bluesound_command("Pause")

    async def async_media_stop(self) -> None:
        """Send stop command."""
        if self.is_grouped and not self.is_master:
            return

        return await self.send_bluesound_command("Pause")

    async def async_media_seek(self, position: float) -> None:
        """Send media_seek command to media player."""
        if self.is_grouped and not self.is_master:
            return

        return await self.send_bluesound_command(f"Play?seek={float(position)}")

    async def async_play_url(self, stream_url: str) -> None:
        """Send the play_media command to the media player."""
        if self.is_grouped and not self.is_master:
            return

        url = f"Play?url={stream_url}"

        return await self.send_bluesound_command(url)

    async def async_set_volume_level(self, volume: float) -> None:
        """Send volume_up command to media player."""
        if volume < 0:
            volume = 0
        elif volume > 100:
            volume = 100
        return await self.send_bluesound_command(f"Volume?level={float(volume)}")

    async def async_mute_volume(self, mute: bool) -> None:
        """Send mute command to media player."""
        if mute:
            return await self.send_bluesound_command("Volume?mute=1")
        return await self.send_bluesound_command("Volume?mute=0")

    def update_attributes(self) -> None:
        """Update attributes of the MA Player from Bluesound"""
        now = time.time()
        # generic attributes (speaker_info)
        self.player.available = True
        # self.player.name = self.name
        self.player.volume_level = self.volume_level * 100
        self.player.volume_muted = self.is_volume_muted

        # transport info (playback state)
        self.player.state = self.state

        if self._playback_started is not None and self.state == PlayerState.IDLE:
            self._playback_started = None
        elif self._playback_started is None and self.state == PlayerState.PLAYING:
            self._playback_started = now

        # # media info (track info)
        self.player.current_url = self._status.get("streamUrl")

        self.player.elapsed_time = self.media_position

        # # zone topology (syncing/grouping) details
        # if (
        #     self.group_info
        #     and self.group_info.coordinator
        #     and self.group_info.coordinator.uid == self.player_id
        # ):
        #     # this player is the sync leader
        #     self.player.synced_to = None
        #     group_members = {x.uid for x in self.group_info.members if x.is_visible}
        #     if not group_members:
        #         # not sure about this ?!
        #         self.player.type = PlayerType.STEREO_PAIR
        #     elif group_members == {self.player_id}:
        #         self.player.group_childs = set()
        #     else:
        #         self.player.group_childs = group_members
        # elif self.group_info and self.group_info.coordinator:
        #     # player is synced to
        #     self.player.group_childs = set()
        #     self.player.synced_to = self.group_info.coordinator.uid
        # else:
