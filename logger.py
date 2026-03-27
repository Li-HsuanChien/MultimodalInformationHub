import logging
import os


# ─────────────────────────────────────────────
# ANSI color codes
# ─────────────────────────────────────────────
ANSI = {
    "RED":    "\x1b[31m",
    "GREEN":  "\x1b[32m",
    "YELLOW": "\x1b[33m",
    "BLUE":   "\x1b[34m",
    "RESET":  "\x1b[0m",
}

LEVEL_COLORS = {
    logging.DEBUG:    "BLUE",
    logging.INFO:     "YELLOW",
    logging.WARNING:  "YELLOW",
    logging.ERROR:    "RED",
    logging.CRITICAL: "RED",
}


def alter_text_color(text: str, color: str, colorize: bool = True) -> str:
    """
    Wrap text in ANSI color codes.
    When colorize=False (i.e. writing to a file) returns plain text.
    """
    if not colorize:
        return text
    code = ANSI.get(color.upper(), "")
    reset = ANSI["RESET"]
    return f"{code}{text}{reset}"


# ─────────────────────────────────────────────
# Custom formatters
# ─────────────────────────────────────────────
class PlainFormatter(logging.Formatter):
    """For file handlers — no ANSI codes."""

    def format(self, record: logging.LogRecord) -> str:
        # Strip any ANSI escape sequences that may have snuck in
        import re
        msg = super().format(record)
        return re.sub(r"\x1b\[[0-9;]*m", "", msg)


class ColorFormatter(logging.Formatter):
    """For the console (StreamHandler) — colorizes the level label."""

    def format(self, record: logging.LogRecord) -> str:
        color = LEVEL_COLORS.get(record.levelno, "RESET")
        code  = ANSI[color]
        reset = ANSI["RESET"]
        original_levelname = record.levelname
        record.levelname = f"{code}{record.levelname}{reset}"
        result = super().format(record)
        record.levelname = original_levelname   # restore so other handlers aren't affected
        return result


_FMT      = "%(asctime)s [%(levelname)s] (%(name)s) %(message)s"
_DATE_FMT = "%Y-%m-%d %H:%M:%S"


# ─────────────────────────────────────────────
# Logger factory
# ─────────────────────────────────────────────
def get_logger(
    user_email: str,
    alias: str,
    log_dir: str = "logs",
    console: bool = True,
) -> logging.Logger:
    """
    Return a Logger for *user_email* that writes to:
      - <alias>/<alias>.log   (per-user, plain text)
      - logs/all.log       (full combined log, plain text)
      - stderr/stdout      (console, with ANSI color) — optional

    Safe to call multiple times: handlers are only attached once.
    """
    os.makedirs(log_dir, exist_ok=True)

    logger = logging.getLogger(user_email)
    if logger.handlers:
        return logger   # already configured

    logger.setLevel(logging.DEBUG)
    # Don't propagate to the root logger to avoid duplicate output
    logger.propagate = False

    plain_fmt = PlainFormatter(fmt=_FMT, datefmt=_DATE_FMT)
    color_fmt = ColorFormatter(fmt=_FMT, datefmt=_DATE_FMT)

    # 1. Per-user file handler
    user_path = os.path.join(f"annotation-human/version2/{alias}", f"{alias}.log")
    user_handler = logging.FileHandler(user_path, encoding="utf-8")
    user_handler.setFormatter(plain_fmt)
    logger.addHandler(user_handler)

    # 2. Full / combined file handler
    full_path = os.path.join(log_dir, "all.log")
    full_handler = logging.FileHandler(full_path, encoding="utf-8")
    full_handler.setFormatter(plain_fmt)
    logger.addHandler(full_handler)

    # 3. Console handler (color)
    if console:
        console_handler = logging.StreamHandler()
        console_handler.setFormatter(color_fmt)
        logger.addHandler(console_handler)

    return logger