#!/usr/bin/env python3
# -*- coding:utf-8 -*-

import asyncio
import os
import operator
from aiohttp import web
import threading
import urllib
from memory_tempfile import MemoryTempfile
import soundfile
import pyloudnorm
import io
import aiohttp
import subprocess
import gc
import shlex

from .constants import EVENT_STREAM_STARTED, EVENT_STREAM_ENDED
from .utils import LOGGER, try_parse_int, get_ip, run_async_background_task, run_periodic, get_folder_size
from .models.media_types import TrackQuality, MediaType
from .models.playerstate import PlayerState

class HTTPStreamer():
    ''' Built-in streamer using sox and webserver '''
    
    def __init__(self, mass):
        self.mass = mass
        self.local_ip = get_ip()
        self.analyze_jobs = {}
        self.stream_clients = []

    async def setup(self):
        ''' async initialize of module '''
        pass # we have nothing to initialize
        
    async def stream(self, http_request):
        ''' 
            start stream for a player
        '''
        # make sure we have valid params
        player_id = http_request.match_info.get('player_id','')
        player = await self.mass.players.get_player(player_id)
        if not player:
            return web.Response(status=404, reason="Player not found")
        if not player.queue.use_queue_stream:
            queue_item_id = http_request.match_info.get('queue_item_id')
            queue_item = await player.queue.by_item_id(queue_item_id)
            if not queue_item:
                return web.Response(status=404, reason="Invalid Queue item Id")
        # prepare headers as audio/flac content
        resp = web.StreamResponse(status=200, reason='OK', 
                headers={'Content-Type': 'audio/flac'})
        await resp.prepare(http_request)
        # run the streamer in executor to prevent the subprocess locking up our eventloop
        cancelled = threading.Event()
        if player.queue.use_queue_stream:
            bg_task = self.mass.event_loop.run_in_executor(None,
                self.__get_queue_stream, player, resp, cancelled)
        else:
            bg_task = self.mass.event_loop.run_in_executor(None,
                self.__get_queue_item_stream, player, queue_item, resp, cancelled)
        # let the streaming begin!
        try:
            await asyncio.gather(bg_task)
        except (asyncio.CancelledError, asyncio.TimeoutError):
            cancelled.set()
            raise asyncio.CancelledError()
        return resp
    
    def __get_queue_item_stream(self, player, queue_item, buffer, cancelled):
        ''' start streaming single queue track '''
        LOGGER.debug("stream single queue track started for track %s on player %s" % (queue_item.name, player.name))
        for is_last_chunk, audio_chunk in self.__get_audio_stream(player, queue_item, cancelled):
            if cancelled.is_set():
                # http session ended
                # we must consume the data to prevent hanging subprocess instances
                continue
            # put chunk in buffer
            self.mass.run_task(
                    buffer.write(audio_chunk), wait_for_result=True, 
                        ignore_exception=(BrokenPipeError,ConnectionResetError))
        # all chunks received: streaming finished
        if cancelled.is_set():
            LOGGER.debug("stream single track interrupted for track %s on player %s" % (queue_item.name, player.name))
        else:
            # indicate EOF if no more data
            self.mass.run_task(
                    buffer.write_eof(), wait_for_result=True, 
                        ignore_exception=(BrokenPipeError,ConnectionResetError))
            LOGGER.debug("stream single track finished for track %s on player %s" % (queue_item.name, player.name))

    def __get_queue_stream(self, player, buffer, cancelled):
        ''' start streaming all queue tracks '''
        sample_rate = try_parse_int(player.settings['max_sample_rate'])
        fade_length = try_parse_int(player.settings["crossfade_duration"])
        if not sample_rate or sample_rate < 44100 or sample_rate > 384000:
            sample_rate = 96000
        if fade_length:
            fade_bytes = int(sample_rate * 4 * 2 * fade_length)
        else:
            fade_bytes = int(sample_rate * 4 * 2 * 6)
        pcm_args = 'raw -b 32 -c 2 -e signed-integer -r %s' % sample_rate
        args = 'sox -t %s - -t flac -C 0 -' % pcm_args
        # start sox process
        args = shlex.split(args)
        sox_proc = subprocess.Popen(args, shell=False, 
            stdout=subprocess.PIPE, stdin=subprocess.PIPE)

        def fill_buffer():
            while True:
                chunk = sox_proc.stdout.read(128000)
                if not chunk:
                    break
                if chunk and not cancelled.is_set():
                    self.mass.run_task(buffer.write(chunk), 
                        wait_for_result=True, ignore_exception=(BrokenPipeError,ConnectionResetError))
                del chunk
            # indicate EOF if no more data
            if not cancelled.is_set():
                self.mass.run_task(buffer.write_eof(), 
                    wait_for_result=True, ignore_exception=(BrokenPipeError,ConnectionResetError))
        # start fill buffer task in background
        fill_buffer_thread = threading.Thread(target=fill_buffer)
        fill_buffer_thread.start()
        
        LOGGER.info("Start Queue Stream for player %s " %(player.name))
        is_start = True
        last_fadeout_data = b''
        while True:
            if cancelled.is_set():
                break
            # get the (next) track in queue
            if is_start:
                # report start of queue playback so we can calculate current track/duration etc.
                queue_track = asyncio.run_coroutine_threadsafe(
                    player.queue.start_queue_stream(), 
                    self.mass.event_loop).result()
                is_start = False
            else:
                queue_track = player.queue.next_item
            if not queue_track:
                LOGGER.debug("no (more) tracks left in queue")
                break
            LOGGER.debug("Start Streaming queue track: %s (%s) on player %s" % (queue_track.item_id, queue_track.name, player.name))
            fade_in_part = b''
            cur_chunk = 0
            prev_chunk = None
            bytes_written = 0
            # handle incoming audio chunks
            for is_last_chunk, chunk in self.__get_audio_stream(
                    player, queue_track, cancelled, chunksize=fade_bytes, 
                    resample=sample_rate):
                cur_chunk += 1

                ### HANDLE FIRST PART OF TRACK
                if cur_chunk <= 2 and not last_fadeout_data:
                    # no fadeout_part available so just pass it to the output directly
                    sox_proc.stdin.write(chunk)
                    bytes_written += len(chunk)
                    del chunk
                elif cur_chunk == 1 and last_fadeout_data:
                    prev_chunk = chunk
                    del chunk
                ### HANDLE CROSSFADE OF PREVIOUS TRACK FADE_OUT AND THIS TRACK FADE_IN
                elif cur_chunk == 2 and last_fadeout_data:
                    # combine the first 2 chunks and strip off silence
                    args = 'sox --ignore-length -t %s - -t %s - silence 1 0.1 1%%' % (pcm_args, pcm_args)
                    first_part, std_err = subprocess.Popen(args, shell=True,
                            stdout=subprocess.PIPE, 
                            stdin=subprocess.PIPE).communicate(prev_chunk + chunk)
                    if len(first_part) < fade_bytes:
                        # part is too short after the strip action?!
                        # so we just cut off at the fade position
                        first_part = prev_chunk+chunk
                        if len(first_part) >= fade_bytes:
                            first_part = first_part[fade_bytes:]
                    fade_in_part = first_part[:fade_bytes]
                    remaining_bytes = first_part[fade_bytes:]
                    del first_part
                    # do crossfade
                    crossfade_part = self.__crossfade_pcm_parts(fade_in_part, 
                            last_fadeout_data, pcm_args, fade_length)
                    sox_proc.stdin.write(crossfade_part)
                    bytes_written += len(crossfade_part)
                    del crossfade_part
                    del fade_in_part
                    last_fadeout_data = b''
                    # also write the leftover bytes from the strip action
                    sox_proc.stdin.write(remaining_bytes)
                    bytes_written += len(remaining_bytes)
                    del remaining_bytes
                    del chunk
                    prev_chunk = None # needed to prevent this chunk being sent again
                ### HANDLE LAST PART OF TRACK
                elif prev_chunk and is_last_chunk:
                    # last chunk received so create the last_part with the previous chunk and this chunk
                    # and strip off silence
                    args = 'sox --ignore-length -t %s - -t %s - reverse silence 1 0.1 1%% reverse' % (pcm_args, pcm_args)
                    last_part, stderr = subprocess.Popen(args, shell=True,
                            stdout=subprocess.PIPE, 
                            stdin=subprocess.PIPE).communicate(prev_chunk + chunk)
                    if len(last_part) < fade_bytes:
                        # part is too short after the strip action
                        # so we just cut off at the fade position
                        last_part = prev_chunk+chunk
                        if len(last_part) >= fade_bytes:
                            last_part = last_part[:fade_bytes]
                    if not player.queue.crossfade_enabled:
                        # crossfading is not enabled so just pass the (stripped) audio data
                        sox_proc.stdin.write(last_part)
                        bytes_written += len(last_part)
                        del last_part
                        del chunk
                    else:
                        # handle crossfading support
                        # store fade section to be picked up for next track
                        last_fadeout_data = last_part[-fade_bytes:]
                        remaining_bytes = last_part[:-fade_bytes]
                        # write remaining bytes
                        sox_proc.stdin.write(remaining_bytes)
                        bytes_written += len(remaining_bytes)
                        del last_part
                        del remaining_bytes
                        del chunk
                ### MIDDLE PARTS OF TRACK
                else:
                    # middle part of the track
                    # keep previous chunk in memory so we have enough samples to perform the crossfade
                    if prev_chunk:
                        sox_proc.stdin.write(prev_chunk)
                        bytes_written += len(prev_chunk)
                        prev_chunk = chunk
                    else:
                        prev_chunk = chunk
                    del chunk
            # end of the track reached
            if cancelled.is_set():
                # break out the loop if the http session is cancelled
                break
            else:
                # update actual duration to the queue for more accurate now playing info
                accurate_duration = bytes_written / int(sample_rate * 4 * 2)
                queue_track.duration = accurate_duration
                LOGGER.debug("Finished Streaming queue track: %s (%s) on player %s" % (queue_track.item_id, queue_track.name, player.name))
                # run garbage collect manually to avoid too much memory fragmentation 
                gc.collect()
        # end of queue reached, pass last fadeout bits to final output
        if last_fadeout_data and not cancelled.is_set():
            sox_proc.stdin.write(last_fadeout_data)
            del last_fadeout_data
        ### END OF QUEUE STREAM
        sox_proc.stdin.close()
        sox_proc.terminate()
        fill_buffer_thread.join()
        del sox_proc
        # run garbage collect manually to avoid too much memory fragmentation 
        gc.collect()
        if cancelled.is_set():
            LOGGER.info("streaming of queue for player %s interrupted" % player.name)
        else:
            LOGGER.info("streaming of queue for player %s completed" % player.name)

    def __get_audio_stream(self, player, queue_item, cancelled,
                chunksize=128000, resample=None):
        ''' get audio stream from provider and apply additional effects/processing where/if needed'''
        # get stream details from provider
        # sort by quality and check track availability
        streamdetails = None
        for prov_media in sorted(queue_item.provider_ids, 
                key=operator.itemgetter('quality'), reverse=True):
            if not prov_media['provider'] in self.mass.music.providers:
                continue
            streamdetails = self.mass.run_task(
                    self.mass.music.providers[prov_media['provider']].get_stream_details(prov_media['item_id']), 
                    wait_for_result=True)
            if streamdetails:
                streamdetails['player_id'] = player.player_id
                if not 'item_id' in streamdetails:
                    streamdetails['item_id'] = prov_media['item_id']
                if not 'provider' in streamdetails:
                    streamdetails['provider'] = prov_media['provider']
                if not 'quality' in streamdetails:
                    streamdetails['quality'] = prov_media['quality']
                queue_item.streamdetails = streamdetails
                break
        if not streamdetails:
            LOGGER.warning(f"no stream details for {queue_item.name}")
            yield (True, b'')
            return
        # get sox effects and resample options
        sox_options = self.__get_player_sox_options(player, streamdetails)
        outputfmt = 'flac -C 0'
        if resample:
            outputfmt = 'raw -b 32 -c 2 -e signed-integer'
            sox_options += ' rate -v %s' % resample
        streamdetails['sox_options'] = sox_options
        # determine how to proceed based on input file ype
        if streamdetails["content_type"] == 'aac':
            # support for AAC created with ffmpeg in between
            args = 'ffmpeg -v quiet -i "%s" -f flac - | sox -t flac - -t %s - %s' % (streamdetails["path"], outputfmt, sox_options)
            process = subprocess.Popen(args, shell=True, stdout=subprocess.PIPE)
        elif streamdetails['type'] in ['url', 'file']:
            args = 'sox -t %s "%s" -t %s - %s' % (streamdetails["content_type"], 
                    streamdetails["path"], outputfmt, sox_options)
            args = shlex.split(args)
            process = subprocess.Popen(args, shell=False, stdout=subprocess.PIPE)
        elif streamdetails['type'] == 'executable':
            args = '%s | sox -t %s - -t %s - %s' % (streamdetails["path"], 
                    streamdetails["content_type"], outputfmt, sox_options)
            process = subprocess.Popen(args, shell=True, stdout=subprocess.PIPE)
        else:
            LOGGER.warning(f"no streaming options for {queue_item.name}")
            yield (True, b'')
            return
        # fire event that streaming has started for this track
        self.mass.run_task(
                self.mass.signal_event(EVENT_STREAM_STARTED, streamdetails))
        # yield chunks from stdout
        # we keep 1 chunk behind to detect end of stream properly
        bytes_sent = 0
        while True:
            if cancelled.is_set():
                # http session ended
                # send terminate and pick up left over bytes
                process.terminate()
            # read exactly chunksize of data
            chunk = process.stdout.read(chunksize)
            if len(chunk) < chunksize:
                # last chunk
                bytes_sent += len(chunk)
                yield (True, chunk)
                break
            else:
                bytes_sent += len(chunk)
                yield (False, chunk)
        # fire event that streaming has ended
        self.mass.run_task(self.mass.signal_event(EVENT_STREAM_ENDED, streamdetails))
        # send task to background to analyse the audio
        if queue_item.media_type == MediaType.Track:
            self.mass.event_loop.run_in_executor(None, self.__analyze_audio, streamdetails)

    def __get_player_sox_options(self, player, streamdetails):
        ''' get player specific sox effect options '''
        sox_options = []
        # volume normalisation
        gain_correct = self.mass.run_task(
                self.mass.players.get_gain_correct(
                    player.player_id, streamdetails["item_id"], streamdetails["provider"]), 
                wait_for_result=True)
        if gain_correct != 0:
            sox_options.append('vol %s dB ' % gain_correct)
        # downsample if needed
        if player.settings['max_sample_rate']:
            max_sample_rate = try_parse_int(player.settings['max_sample_rate'])
            if max_sample_rate:
                quality = streamdetails["quality"]
                if quality > TrackQuality.FLAC_LOSSLESS_HI_RES_3 and max_sample_rate == 192000:
                    sox_options.append('rate -v 192000')
                elif quality > TrackQuality.FLAC_LOSSLESS_HI_RES_2 and max_sample_rate == 96000:
                    sox_options.append('rate -v 96000')
                elif quality > TrackQuality.FLAC_LOSSLESS_HI_RES_1 and max_sample_rate == 48000:
                    sox_options.append('rate -v 48000')
        if player.settings.get('sox_options'):
            sox_options.append(player.settings['sox_options'])
        return " ".join(sox_options)
        
    def __analyze_audio(self, streamdetails):
        ''' analyze track audio, for now we only calculate EBU R128 loudness '''
        item_key = '%s%s' %(streamdetails["item_id"], streamdetails["provider"])
        if item_key in self.analyze_jobs:
            return # prevent multiple analyze jobs for same track
        self.analyze_jobs[item_key] = True
        track_loudness = self.mass.run_task(self.mass.db.get_track_loudness(
                streamdetails["item_id"], streamdetails["provider"]), wait_for_result=True)
        if track_loudness == None:
            # only when needed we do the analyze stuff
            LOGGER.debug('Start analyzing track %s' % item_key)
            if streamdetails['type'] == 'url':
                import urllib
                audio_data = urllib.request.urlopen(streamdetails["path"]).read()
            elif streamdetails['type'] == 'executable':
                audio_data = subprocess.check_output(streamdetails["path"], shell=True)
            # calculate BS.1770 R128 integrated loudness
            with io.BytesIO(audio_data) as tmpfile:
                data, rate = soundfile.read(tmpfile)
            meter = pyloudnorm.Meter(rate) # create BS.1770 meter
            loudness = meter.integrated_loudness(data) # measure loudness
            del data
            self.mass.run_task(
                self.mass.db.set_track_loudness(streamdetails["item_id"], streamdetails["provider"], loudness))
            del audio_data
            LOGGER.debug("Integrated loudness of track %s is: %s" %(item_key, loudness))
        self.analyze_jobs.pop(item_key, None)
    
    def __crossfade_pcm_parts(self, fade_in_part, fade_out_part, pcm_args, fade_length):
        ''' crossfade two chunks of audio using sox '''
        # create fade-in part
        fadeinfile = MemoryTempfile(fallback=True).NamedTemporaryFile(buffering=0)
        args = 'sox --ignore-length -t %s - -t %s %s fade t %s' % (pcm_args, pcm_args, fadeinfile.name, fade_length)
        args = shlex.split(args)
        process = subprocess.Popen(args, shell=False, stdin=subprocess.PIPE)
        process.communicate(fade_in_part)
        # create fade-out part
        fadeoutfile = MemoryTempfile(fallback=True).NamedTemporaryFile(buffering=0)
        args = 'sox --ignore-length -t %s - -t %s %s reverse fade t %s reverse' % (pcm_args, pcm_args, fadeoutfile.name, fade_length)
        args = shlex.split(args)
        process = subprocess.Popen(args, shell=False,
                stdout=subprocess.PIPE, stdin=subprocess.PIPE)
        process.communicate(fade_out_part)
        # create crossfade using sox and some temp files
        # TODO: figure out how to make this less complex and without the tempfiles
        args = 'sox -m -v 1.0 -t %s %s -v 1.0 -t %s %s -t %s -' % (pcm_args, fadeoutfile.name, pcm_args, fadeinfile.name, pcm_args)
        args = shlex.split(args)
        process = subprocess.Popen(args, shell=False,
                stdout=subprocess.PIPE, stdin=subprocess.PIPE)
        crossfade_part, stderr = process.communicate()
        fadeinfile.close()
        fadeoutfile.close()
        del fadeinfile
        del fadeoutfile
        return crossfade_part
