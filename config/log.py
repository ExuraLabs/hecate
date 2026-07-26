"""Logging setup shared by the CLI and the backfill's worker processes.

Lives here rather than in the CLI because spawned workers configure their
own logging and must not import the CLI to do it.
"""

import logging

LOG_FORMAT = "%(asctime)s %(levelname)-8s %(processName)-14s %(name)s | %(message)s"
DATE_FORMAT = "%H:%M:%S"

# ogmios-python logs a multi-line record for *every block it parses*, at INFO.
# At Hecate's block rates that buries anything worth reading, so it is pinned
# to WARNING here. Raise it deliberately if you need to debug the protocol:
#   logging.getLogger("ogmios").setLevel(logging.INFO)
QUIETED_LOGGERS = ("ogmios",)


def configure_logging(level: int | str = logging.INFO) -> None:
    """Send Hecate's logs to stderr at ``level``.

    ``force=True`` so a re-entrant call (e.g. a library that already
    touched the root logger) still ends up with our handler.
    """
    logging.basicConfig(
        level=level,
        format=LOG_FORMAT,
        datefmt=DATE_FORMAT,
        force=True,
    )
    for name in QUIETED_LOGGERS:
        logging.getLogger(name).setLevel(logging.WARNING)
