from __future__ import annotations

from taskiq_redis import ListQueueBroker

from config.settings import Settings

_settings = Settings()

broker = ListQueueBroker(url=_settings.REDIS_URL, queue_name="weisiong:tasks")
