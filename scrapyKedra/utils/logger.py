import logging
import json
from datetime import datetime

logger = logging.getLogger("structured")

def log_event(event_type, **data):
    log = {
        "timestamp": datetime.utcnow().isoformat(),
        "event": event_type,
        **data
    }
    logger.info(json.dumps(log))