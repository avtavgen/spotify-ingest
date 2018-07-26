import itertools
import requests
import sys
from time import sleep
from random import randint
from datetime import datetime


def batches(iterable, n=10):
    """divide a single list into a list of lists of size n """
    batchLen = len(iterable)
    for ndx in range(0, batchLen, n):
        yield list(iterable[ndx:min(ndx + n, batchLen)])


class SpotifyProcessor(object):
    def __init__(self, entity, log, retry=3):
        self.log = log
        self.retry = retry
        self.entity = entity
        self.next = None
        self.user_list = []
        self.track_list = []
        self.base_url = "https://api.spotify.com/v1/"
        self.access_token = ""
        self.headers = {'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_1) '
                                      'AppleWebKit/537.36 (KHTML, like Gecko) Chrome/39.0.2171.95 Safari/537.36'}

    def _make_request(self, url, access_token=None, next=None):
        if next:
            url = next
        if access_token:
            self.headers["Authorization"] = "Bearer " + self.access_token
        else:
            sys.exit("No credentials provided")
        retries = 0
        while retries <= self.retry:
            try:
                response = requests.get(url, headers=self.headers)
                response.raise_for_status()
                return response
            except requests.exceptions.HTTPError as e:
                self.log.info("{}".format(e))
                sleep(randint(10, 20))
                retries += 1
                if retries <= self.retry:
                    self.log.info("Trying again!")
                    continue
                else:
                    sys.exit("Max retries reached")
            except Exception as e:
                self.log.info("{}: Failed to make request on try {}".format(e, retries))
                retries += 1
                if retries <= self.retry:
                    self.log.info("Trying again!")
                    continue
                else:
                    sys.exit("Max retries reached")

    def _get_categories(self):
        self.info = []
        self.categories = []
        self.access_token = self._auth()
        while True:
            response = self._make_request(self.base_url + "browse/categories?limit=50", self.access_token, self.next)
            categories = response.json()["categories"]
            self.next = categories["next"]
            for category in categories["items"]:
                try:
                    self.categories.append(category)
                except Exception as e:
                    self.log.info("Failed to fetch categorie: {}".format(e))

            if not self.next:
                break
        self.log.info(len(self.categories))
        self.log.info(self.categories)

    def _get_playlists(self):
        for category in self.categories:
            self.log.info(category["id"])
            track_list = []
            user_list = []
            playlists_list = []
            category_data = dict()
            while True:
                response = self._make_request(self.base_url + "browse/categories/{}/playlists?limit=50".format(category["id"]), self.access_token, self.next)
                playlists = response.json()["playlists"]
                self.next = playlists["next"]
                for playlist in playlists["items"]:
                    try:
                        playlist_data = dict()
                        playlist_data["category"] = category["id"]
                        playlist_data["id"] = playlist["id"]
                        playlist_data["name"] = playlist["name"]
                        playlist_data["tracks"] = playlist["tracks"]["href"]
                        playlists_list.append(playlist_data)
                    except Exception as e:
                        self.log.info("Failed to fetch playlist: {}".format(e))
                if not self.next:
                    break
            for playlist in playlists_list:
                self.log.info(playlist["name"])
                resp = self._get_tracks(playlist)
                track_list.extend(resp[0])
                user_list.extend(resp[1])
            category_data["category_id"] = category["id"]
            category_data["category_name"] = category["name"]
            category_data["track_count"] = len(track_list)
            category_data["artist_count"] = len(user_list)
            self.log.info(category_data)
            self.entity.save(category=category_data, users=track_list, tracks=user_list)

    def _get_tracks(self, playlist):
        artist_ids = []
        track_info = []
        user_list = []
        while True:
            response = self._make_request(playlist["tracks"], self.access_token, self.next)
            tracks = response.json()["items"]
            self.next = response.json()["next"]
            for track in tracks:
                try:
                    track_data = dict()
                    album_data = dict()
                    # album_data["album_type"] = track["track"]["album"]["album_type"]
                    # album_data["id"] = track["track"]["album"]["id"]
                    # album_data["name"] = track["track"]["album"]["name"]
                    # album_data["release_date"] = track["track"]["album"]["release_date"]
                    # album_data["type"] = track["track"]["album"]["type"]
                    # album_data["uri"] = track["track"]["album"]["uri"]
                    artist_ids_ = []
                    for artist in track["track"]["artists"]:
                        # artist_data["id"] = artist["id"]
                        # artist_data["name"] = artist["name"]
                        # artist_data["type"] = artist["type"]
                        # artist_data["uri"] = artist["uri"]
                        # artist_data["external_urls"] = artist["external_urls"]
                        artist_ids_.append(artist["id"])
                        # user_list.append(self._get_user_info(artist_data_list))
                    # track_data["album_data"] = album_data
                    # track_data["artist_data"] = artist_data_list
                    artist_ids.extend(artist_ids_)
                    track_data["uri"] = "spotify␟track␟{}".format(track["track"]["id"])
                    track_data["artists_id"] = artist_ids_
                    track_data["category"] = playlist["category"]
                    track_data["playlist"] = playlist["name"]
                    track_data["added_at"] = track["added_at"]
                    track_data["disc_number"] = track["track"]["disc_number"]
                    track_data["duration_ms"] = track["track"]["duration_ms"]
                    track_data["episode"] = track["track"]["episode"]
                    track_data["explicit"] = track["track"]["explicit"]
                    track_data["id"] = track["track"]["id"]
                    track_data["is_local"] = track["track"]["is_local"]
                    track_data["album_name"] = track["track"]["album"]["name"]
                    track_data["name"] = track["track"]["name"]
                    track_data["popularity"] = track["track"]["popularity"]
                    track_data["track"] = track["track"]["track"]
                    track_data["track_number"] = track["track"]["track_number"]
                    track_data["type"] = track["track"]["type"]
                    track_info.append(track_data)
                except Exception as e:
                    self.log.info("Failed to fetch playlist: {}".format(e))
            randint(4, 6)
            if not self.next:
                break
        for users in batches(list(set(artist_ids)), 40):
            user_list.extend(self._get_user_info(users))
            randint(4, 6)
        return track_info, user_list

    def _get_user_info(self, user_ids):
        response = self._make_request(self.base_url + "artists?ids={}".format(",".join(user_ids)), self.access_token)
        raw_data = response.json()
        artist_list = []
        for artist in raw_data["artists"]:
            try:
                user_data = dict()
                user_data["uri"] = "spotify␟user␟{}".format(artist["id"])
                user_data["ingested"] = False
                user_data["date"] = datetime.now().strftime("%Y-%m-%d")
                user_data["name"] = artist["name"]
                user_data["popularity"] = artist["popularity"]
                user_data["type"] = artist["type"]
                user_data["followers"] = artist["followers"]["total"]
                user_data["genres"] = artist["genres"]
                artist_list.append(user_data)
            except Exception as e:
                self.log.info("Failed to fetch user info: {}".format(e))
        return artist_list

    def _auth(self):
        data = dict()
        headers = dict()
        data["grant_type"] = "client_credentials"
        headers["Content-Type"] = "application/x-www-form-urlencoded"
        headers["Authorization"] = "Basic ZTZiOWQ0NzE2MTk3NGRlMWJmZGViYjhmMGQwMmViMjQ6MjhjYTgxMjAzZWEzNDllZTg0MGIzNzY5MjliZDZmZjA="
        response = requests.post("https://accounts.spotify.com/api/token", data=data, headers=headers)
        if response.status_code == 200:
            return response.json()["access_token"]
        else:
            raise Exception("SF login failed to run by returning code of {}.".format(response.status_code))

    def fetch(self):
        self.log.info('Making request to Spotify for daily creators export')
        self._get_categories()
        self._get_playlists()
        return self
