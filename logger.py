import logging
import os
import re

# ── Custom SUCCESS level ──────────────────────────────────────────────────────
SUCCESS = 25
logging.addLevelName(SUCCESS, "SUCCESS")

def _success(self, message, *args, **kwargs):
    if self.isEnabledFor(SUCCESS):
        self._log(SUCCESS, message, args, **kwargs)

logging.Logger.success = _success

# ── ANSI color codes ──────────────────────────────────────────────────────────
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
    SUCCESS:          "GREEN",
    logging.ERROR:    "RED",
    logging.CRITICAL: "RED",
}

def alter_text_color(text: str, color: str, colorize: bool = True) -> str:
    if not colorize:
        return text
    code  = ANSI.get(color.upper(), "")
    reset = ANSI["RESET"]
    return f"{code}{text}{reset}"

# ── Formatters ────────────────────────────────────────────────────────────────
class PlainFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        msg = super().format(record)
        return re.sub(r"\x1b\[[0-9;]*m", "", msg)

class ColorFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        color  = LEVEL_COLORS.get(record.levelno, "RESET")
        code   = ANSI[color]
        reset  = ANSI["RESET"]
        original_levelname = record.levelname
        record.levelname   = f"{code}{record.levelname}{reset}"
        result             = super().format(record)
        record.levelname   = original_levelname
        return result

_FMT      = "%(asctime)s [%(levelname)s] (%(name)s) %(message)s"
_DATE_FMT = "%Y-%m-%d %H:%M:%S"

# ── Logger factory ────────────────────────────────────────────────────────────
def get_logger(
    user_email: str,
    alias: str,
    log_dir: str = "logs",
    console: bool = True,
) -> logging.Logger:
    os.makedirs(log_dir, exist_ok=True)

    logger = logging.getLogger(user_email)
    if logger.handlers:
        return logger

    logger.setLevel(logging.DEBUG)
    logger.propagate = False

    plain_fmt = PlainFormatter(fmt=_FMT, datefmt=_DATE_FMT)
    color_fmt = ColorFormatter(fmt=_FMT, datefmt=_DATE_FMT)

    # 1. Per-user file
    user_path = os.path.join(f"annotation-human/version2/{alias}", f"{alias}.log")
    os.makedirs(os.path.dirname(user_path), exist_ok=True)
    user_handler = logging.FileHandler(user_path, encoding="utf-8")
    user_handler.setFormatter(plain_fmt)
    logger.addHandler(user_handler)

    # 2. Full combined file
    full_path = os.path.join(log_dir, "all.log")
    full_handler = logging.FileHandler(full_path, encoding="utf-8")
    full_handler.setFormatter(plain_fmt)
    logger.addHandler(full_handler)

    # 3. Console (color)
    if console:
        console_handler = logging.StreamHandler()
        console_handler.setFormatter(color_fmt)
        logger.addHandler(console_handler)

    return logger