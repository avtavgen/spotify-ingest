
def batches(iterable, n=10):
    """divide a single list into a list of lists of size n """
    batchLen = len(iterable)
    for ndx in range(0, batchLen, n):
        yield list(iterable[ndx:min(ndx + n, batchLen)])


class SocialStatements:

    def __init__(self, logger, engine=None):
        self.users = []
        self.categories = []
        self.tracks = []
        self.engine = engine
        self.logger = logger
        self.relations = []

    category_schema = {
        "table_name": "categories_tmp",
        "options": {
            "primary_key": ["category_id"]
        },
        "columns": {
            "category_id": "text",
            "category_name": "text",
            "track_count": "int",
            "artist_count": "int"
        }
    }

    user_schema = {
        "table_name": "user_tmp",
        "options": {
            "primary_key": ["uri", "date"],
            "order_by": ["date desc"]
        },
        "columns": {
            "uri": "text",
            "id": "text",
            "ingested": "boolean",
            "date": "date",
            "name": "text",
            "popularity": "int",
            "type": "text",
            "followers": "int",
            "genres": "set<text>",
        }
    }

    track_schema = {
        "table_name": "tracks_tmp",
        "options": {
            "primary_key": ["uri", "date"],
            "order_by": ["date desc"]
        },
        "columns": {
            "uri": "text",
            "artists_id": "set<text>",
            "category": "text",
            "playlist": "text",
            "date": "date",
            "added_at": "text",
            "disc_number": "int",
            "duration_ms": "int",
            "episode": "boolean",
            "explicit": "boolean",
            "id": "text",
            "is_local": "boolean",
            "album_name": "text",
            "name": "text",
            "popularity": "int",
            "track": "boolean",
            "track_number": "int",
            "type": "text"
        }
    }

    def save(self, batch_size=50, categories=None, users=None, tracks=None, category_name=None):
        """Write these social statements to the data engine in the appropriate manner."""
        self.users = users
        self.categories = categories
        self.tracks = tracks

        if self.tracks:
            self.track_schema["table_name"] = "{}_tracks_temp".format(category_name)
            self.logger.info('about to send {} track statements to the data engine'.format(len(self.tracks)))
            self.logger.info(self.track_schema)
            self._write_batches(self.engine, self.logger, self.track_schema, self.tracks, batch_size)
        else:
            self.logger.info('skipping track ingest, no records in these social statements')

        if self.users:
            self.logger.info('about to send {} user statements to the data engine'.format(len(self.users)))
            self._write_batches(self.engine, self.logger, self.user_schema, self.users, batch_size)
        else:
            self.logger.info('skipping user ingest, no records in these social statements')

        if self.categories:
            self.logger.info('about to send {} user statements to the data engine'.format(len(self.categories)))
            self._write_batches(self.engine, self.logger, self.category_schema, self.categories, batch_size)
        else:
            self.logger.info('skipping category ingest, no records in these social statements')

    @staticmethod
    def _write_batches(engine, logger, schema, data, batch_size=40):
        for rows in batches(data, batch_size):
            logger.info('Rows: {}'.format(rows))
            res = engine.save(schema, list(rows)).result()
            logger.info(res)
