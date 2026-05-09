from __future__ import annotations

import logging
import os


def configure_streaming_logging(default_level: str = "INFO") -> None:
    level_name = os.getenv("STREAMING_LOG_LEVEL", default_level).upper()
    log_level = getattr(logging, level_name, logging.INFO)
    logging.basicConfig(level=log_level)

    for logger_name in ("py4j", "py4j.clientserver", "py4j.java_gateway"):
        logging.getLogger(logger_name).setLevel(logging.WARNING)
