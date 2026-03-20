import os
from dotenv import load_dotenv

load_dotenv()

EVENT_HUB_CONNECTION_STRING = os.getenv("EVENT_HUB_CONNECTION_STRING")
EVENT_HUB_NAME = os.getenv("EVENT_HUB_NAME", "retail-media-events")
EVENTS_PER_SECOND = int(os.getenv("EVENTS_PER_SECOND", 10))
TOTAL_EVENTS = int(os.getenv("TOTAL_EVENTS", 1000))
