import requests
import sys
from time import sleep
from random import randint
from datetime import datetime


class SpotifyProcessor(object):
    def __init__(self, entity, log, retry=3):
        self.log = log
        self.retry = retry
        self.entity = entity
        self.next = None
        self.base_url = "https://api.spotify.com/v1/"
        self.access_token = ""
        self.headers = {'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_1) '
                                      'AppleWebKit/537.36 (KHTML, like Gecko) Chrome/39.0.2171.95 Safari/537.36'}

    def _make_request(self, url, next=None, access_token=None):
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
                sleep(randint(4, 10))
                break
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
            response = self._make_request(self.base_url + "browse/categories?limit=50", self.next, self.access_token)
            categories = response.json()["categories"]
            self.next = categories["next"]
            for category in categories["items"]:
                try:
                    self.categories.append(category["id"])
                except Exception as e:
                    self.log.info("Failed to fetch categorie: {}".format(e))

            if not self.next:
                break
        self.log.info(len(self.categories))
        self.log.info(self.categories)
        # self.entity.save(users=self.info)

    def _get_playlists(self):
        self.playlists = []
        for category in self.categories:
            while True:
                response = self._make_request(self.base_url + "browse/categories/{}/playlists?limit=50".format(category), self.next, self.access_token)
                playlists = response.json()["playlists"]
                self.next = playlists["next"]
                for playlist in playlists["items"]:
                    try:
                        playlist_data = dict()
                        playlist_data["category"] = category
                        playlist_data["id"] = playlist["id"]
                        playlist_data["name"] = playlist["name"]
                        playlist_data["tracks"] = playlist["tracks"]["href"]
                        self.playlists.append(playlist_data)
                    except Exception as e:
                        self.log.info("Failed to fetch playlist: {}".format(e))
                if not self.next:
                    break
        self.log.info(len(self.playlists))
        self.log.info(self.playlists)

    def _get_tracks(self):
        self._track_info = []
        for playlist in self.playlists:
            while True:
                response = self._make_request(playlist["tracks"], self.next, self.access_token)
                tracks = response.json()["items"]
                self.next = response.json()["next"]
                for track in tracks:
                    try:
                        track_data = dict()
                        album_data = dict()
                        artist_data_list = []
                        album_data["album_type"] = track["track"]["album"]["album_type"]
                        album_data["id"] = track["track"]["album"]["id"]
                        album_data["name"] = track["track"]["album"]["name"]
                        album_data["release_date"] = track["track"]["album"]["release_date"]
                        album_data["type"] = track["track"]["album"]["type"]
                        album_data["uri"] = track["track"]["album"]["uri"]
                        for artist in track["track"]["artists"]:
                            artist_data = dict()
                            artist_data["id"] = artist["id"]
                            artist_data["name"] = artist["name"]
                            artist_data["type"] = artist["type"]
                            artist_data["uri"] = artist["uri"]
                            artist_data["external_urls"] = artist["external_urls"]
                            artist_data_list.append(artist_data)
                        track_data["album_data"] = album_data
                        track_data["artist_data"] = artist_data_list
                        track_data["category"] = playlist["category"]
                        track_data["playlist"] = playlist["name"]
                        track_data["added_at"] = track["added_at"]
                        track_data["disc_number"] = track["track"]["disc_number"]
                        track_data["duration_ms"] = track["track"]["duration_ms"]
                        track_data["episode"] = track["track"]["episode"]
                        track_data["explicit"] = track["track"]["explicit"]
                        track_data["href"] = track["track"]["href"]
                        track_data["id"] = track["track"]["id"]
                        track_data["is_local"] = track["track"]["is_local"]
                        track_data["name"] = track["track"]["name"]
                        track_data["popularity"] = track["track"]["popularity"]
                        track_data["preview_url"] = track["track"]["preview_url"]
                        track_data["track"] = track["track"]["track"]
                        track_data["track_number"] = track["track"]["track_number"]
                        track_data["type"] = track["track"]["type"]
                        track_data["uri"] = track["track"]["uri"]
                        self._track_info.append(track_data)
                        self.log.info(track_data)
                    except Exception as e:
                        self.log.info("Failed to fetch playlist: {}".format(e))
                if not self.next:
                    break
        self.log.info(len(self._track_info))

    # def _get_user_info(self, url, week):
    #     creators_list = []
    #     user_data = dict()
    #     today = datetime.now().strftime("%Y-%m-%d")
    #     response = self._make_request(url)
    #     for creator in creators:
    #
    #         self.log.info(user_data)
    #
    #     return creators_list

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
        self._get_tracks()
        return self
