import asyncio

def get_running_loop():
  loop = asyncio.get_event_loop()
  return loop if loop.is_running() else None

