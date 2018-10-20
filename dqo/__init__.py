import asyncio

if not hasattr(asyncio, 'get_running_loop'):
  def get_running_loop():
    loop = asyncio.get_event_loop()
    return loop if loop.is_running() else None
  asyncio.get_running_loop = get_running_loop

from .table import table
from .column import Column
from .database import Database, Dialect

DEFAULT_SYNC_DB = None
DEFAULT_ASYNC_DB = None
