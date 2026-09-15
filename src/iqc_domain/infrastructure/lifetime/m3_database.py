"""An isolated M3 pool; never changes the primary DatabaseManager singleton."""

from functools import lru_cache
import os
from pathlib import Path

from dotenv import load_dotenv
from sqlalchemy import create_engine, URL
from sqlalchemy.engine import Engine

from src.iqc_domain.application.lifetime.lifetime import LifetimeSourceUnavailable


@lru_cache(maxsize=1)
def get_m3_engine() -> Engine:
    load_dotenv(Path(__file__).resolve().parents[4] / '.env')
    settings = {key: os.getenv(f'M3_DB_{key}') for key in (
        'USER', 'PASSWORD', 'HOST', 'PORT', 'DATABASE',
    )}
    if not all(settings.values()):
        raise LifetimeSourceUnavailable('LIFETIME_CONNECTION_UNAVAILABLE')
    url = URL.create(
        'postgresql+psycopg2', username=settings['USER'], password=settings['PASSWORD'],
        host=settings['HOST'], port=int(settings['PORT']), database=settings['DATABASE'],
    )
    return create_engine(
        url, pool_pre_ping=True, pool_recycle=3600, pool_timeout=15,
        connect_args={'connect_timeout': 10}, hide_parameters=True,
    )
