import time

from music_assistant.common.models.config_entries import (
    ConfigEntry,
    ConfigValueType,
    ProviderConfig,
)
from music_assistant.common.models.enums import (
    ConfigEntryType,
    PlayerFeature,
    PlayerState,
    PlayerType,
)
from music_assistant.common.models.player import DeviceInfo, Player
from music_assistant.common.models.provider import ProviderManifest
from music_assistant.common.models.queue_item import QueueItem
from music_assistant.server import MusicAssistant
from music_assistant.server.models import ProviderInstanceType
from music_assistant.server.models.player_provider import PlayerProvider
from music_assistant.server.providers.bluesound.bluesound_player import BluesoundPlayer

CONF_BLUESOUND_PLAYER_HOST = "bluesound_player_host"
CONF_BLUESOUND_PLAYER_CONTROL_PORT = "bluesound_player_control_port"


async def setup(
    mass: MusicAssistant, manifest: ProviderManifest, config: ProviderConfig
) -> ProviderInstanceType:
    """Initialize provider(instance) with given configuration."""
    prov = BluesoundProvider(mass, manifest, config)
    await prov.handle_setup()
    return prov


async def get_config_entries(
    mass: MusicAssistant,
    instance_id: str | None = None,
    action: str | None = None,
    values: dict[str, ConfigValueType] | None = None,
) -> tuple[ConfigEntry, ...]:
    """
    Return Config entries to setup this provider.

    instance_id: id of an existing provider instance (None if new instance setup).
    action: [optional] action key called from config entries UI.
    values: the (intermediate) raw values for config entries sent with the action.
    """
    # ruff: noqa: ARG001
    return (
        ConfigEntry(
            key=CONF_BLUESOUND_PLAYER_HOST,
            type=ConfigEntryType.STRING,
            default_value="192.168.1.162",
            label="Bluesound server ip",
            required=True,
        ),
        ConfigEntry(
            key=CONF_BLUESOUND_PLAYER_CONTROL_PORT,
            type=ConfigEntryType.INTEGER,
            default_value="11000",
            label="Bluesound control port",
            required=True,
        ),
    )


class BluesoundProvider(PlayerProvider):
    """Player provider for Bluesound based players."""

    async def handle_setup(self) -> None:
        """Handle setup of this provider."""
        self.bluesound_players: dict[str, BluesoundPlayer] = {}
        host = self.config.get_value(CONF_BLUESOUND_PLAYER_HOST)
        port = self.config.get_value(CONF_BLUESOUND_PLAYER_CONTROL_PORT)
        bluesound_player = BluesoundPlayer(host=host, port=port, mass=self.mass)
        await bluesound_player.async_init()
        bluesound_player.player = Player(
            player_id=bluesound_player.unique_id,
            provider=self.domain,
            type=PlayerType.PLAYER,
            name=host,
            available=True,
            powered=False,
            device_info=DeviceInfo(model=self.manifest.name, manufacturer="Music Assistant"),
            supported_features=(
                PlayerFeature.POWER,
                PlayerFeature.PAUSE,
                PlayerFeature.VOLUME_SET,
                PlayerFeature.VOLUME_MUTE,
                PlayerFeature.SET_MEMBERS,
            ),
        )
        self.bluesound_players[bluesound_player.unique_id] = bluesound_player
        self.mass.players.register_or_update(bluesound_player.player)

    async def cmd_stop(self, player_id: str) -> None:
        """Send STOP command to given player."""
        bluesound_player = self.bluesound_players.get(player_id)
        await bluesound_player.async_media_stop()

    async def cmd_play(self, player_id: str) -> None:
        """Send PLAY (unpause) command to given player."""
        bluesound_player = self.bluesound_players.get(player_id)
        await bluesound_player.async_media_play()

    async def cmd_pause(self, player_id: str) -> None:
        """Send PAUSE command to given player."""
        bluesound_player = self.bluesound_players.get(player_id)
        await bluesound_player.async_media_pause()

    async def cmd_volume_set(self, player_id: str, volume_level: int) -> None:
        """Send VOLUME SET command to given player."""
        bluesound_player = self.bluesound_players.get(player_id)
        await bluesound_player.async_set_volume_level(volume_level)

    async def cmd_volume_mute(self, player_id: str, mute: bool) -> None:
        """Send VOLUME MUTE command to given player."""
        bluesound_player = self.bluesound_players.get(player_id)
        await bluesound_player.async_mute_volume(mute)

    async def cmd_play_url(
        self,
        player_id: str,
        url: str,
        queue_item: QueueItem | None,
    ) -> None:
        """Send PLAY URL command to given player.

        This is called when the Queue wants the player to start playing a specific url.
        If an item from the Queue is being played, the QueueItem will be provided with
        all metadata present.

            - player_id: player_id of the player to handle the command.
            - url: the url that the player should start playing.
            - queue_item: the QueueItem that is related to the URL (None when playing direct url).
        """
        bluesound_player = self.bluesound_players.get(player_id)
        await bluesound_player.async_play_url(url)
        # optimistically set this timestamp to help figure out elapsed time later
        now = time.time()
        bluesound_player.player.elapsed_time = 0
        bluesound_player.player.state = PlayerState.PLAYING
        bluesound_player.player.elapsed_time_last_updated = now

    async def poll_player(self, player_id: str) -> None:
        bluesound_player = self.bluesound_players.get(player_id)
        await bluesound_player.async_update()
