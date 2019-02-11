import os

import dataset
from lightsteem.client import Client


def get_db_connection():
    """A generic function to standardize the connection
    to the database.

    The default database url can be passed as an environment variable. If it's
    not passed the default is set as `sqlite:///delegations.db`. (A file in
    the relative path.)

    return (Database): dataset.Database instance
    """
    database_uri = os.getenv("DATABASE_URI", "sqlite:///delegations.db")
    db = dataset.connect(database_uri)
    return db


def get_lightsteem_client():
    """A generic function to standardize the STEEM blockchain
    interaction.

    The default node can be passed as an environment variable. If it's not
    passed the default is set as `api.steemit.com`.
    """
    nodes = os.getenv("NODE", ["https://api.steemit.com",])
    return Client(nodes=nodes)
