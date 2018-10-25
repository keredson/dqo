import asyncio

if not hasattr(asyncio, 'get_running_loop'):
  def get_running_loop():
    loop = asyncio.get_event_loop()
    return loop if loop.is_running() else None
  asyncio.get_running_loop = get_running_loop

from .table import TableDecorator as Table
from .column import Column, ForeignKey
from .database import Database, Dialect, EchoDatabase
from .function import sql

DB = None

from .__version__ import __version__
