from skafossdk import *
from helpers import get_logger
from social.entity import SocialStatements
from spotify.spotify_proccessor import SpotifyProcessor

# Initialize the skafos sdk
ska = Skafos()

ingest_log = get_logger('user-fetch')

if __name__ == "__main__":
    ingest_log.info('Starting job')

    ingest_log.info('Fetching spotify tracks data')
    entity = SocialStatements(ingest_log, ska.engine) # , ska.engine
    processor = SpotifyProcessor(entity, ingest_log).fetch()
