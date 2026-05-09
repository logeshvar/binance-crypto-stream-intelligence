from __future__ import annotations

import logging

from streaming.logging_utils import configure_streaming_logging


def test_configure_streaming_logging_quiets_py4j_info():
    configure_streaming_logging()

    assert logging.getLogger("py4j.clientserver").getEffectiveLevel() == logging.WARNING
