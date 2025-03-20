"""
There are two logging environments: local and Glue.

**Local Logger**: This logger configures the logging settings for the ETL job in the local environment. A shared logging configuration is applied across all ETL processes and tasks, ensuring that logs are written to the same file and formatted consistently.

*Note*: You cannot instantiate a logger instance and share it across processes, as loggers are not picklable. Each process creates its own logger instance using the shared configuration.

**Glue Logger**: In the Glue environment, logging is controlled, and instead of managing the complexities of Glue's logger, `print` is used for logging. Since logs are sent to CloudWatch and incur costs, the logging level is set to a minimum of `INFO`. Enabling `DEBUG` can result in excessive verbosity, pulling in logs from all dependencies, which can overwhelm CloudWatch and become costly.
"""

import logging
import os
import sys
from datetime import datetime

import structlog
from structlog.stdlib import ProcessorFormatter

# Default values for logging
DEFAULT_LOGGING_LEVEL = "DEBUG"
DEFAULT_LOG_DIR = "logs"
DEFAULT_LOG_FILENAME = "etl.log"
DEFAULT_LOG_FILE_PREFIX = f"{datetime.now().strftime('%Y-%m-%d-%H-%M')}"

logging_level = os.getenv("LOGGING_LEVEL", DEFAULT_LOGGING_LEVEL)
log_filename = os.getenv("LOG_FILENAME", DEFAULT_LOG_FILENAME)
log_dir = os.getenv("LOG_DIR", DEFAULT_LOG_DIR)
log_file_prefix = os.getenv("LOG_FILE_PREFIX", DEFAULT_LOG_FILE_PREFIX)
print(f"Logging level: {logging_level}")
log_level = getattr(logging, logging_level.upper(), logging.INFO)

# log dir is env dependent : Check if running in AWS Glue
if os.getenv("ETL_EXECUTION_ENV") == "AWS_GLUE":
    ETL_DIR = "/tmp/etl"
else:
    ETL_DIR = os.path.normpath(os.path.join(os.path.dirname(__file__), "../../etl"))

log_file_path = os.path.normpath(
    os.path.join(ETL_DIR, log_dir, f"{log_file_prefix}_{log_filename}")
)

# Ensure the log directory exists
os.makedirs(os.path.dirname(log_file_path), exist_ok=True)


def get_log_level_numeric(level_name):
    log_levels = {
        "DEBUG": logging.DEBUG,
        "INFO": logging.INFO,
        "WARNING": logging.WARNING,
        "ERROR": logging.ERROR,
        "CRITICAL": logging.CRITICAL,
    }
    return log_levels.get(level_name.upper(), logging.INFO)


class GluePrintLogger:
    """
    Glue environment: Uses print() for logging instead of structlog.

    - Converts DEBUG to INFO in Glue.
    - Ensures minimal cost and avoids excessive verbosity in CloudWatch.
    """

    def __init__(self, job_name):
        level = os.getenv("LOGGING_LEVEL", "INFO")
        self.level = get_log_level_numeric(level)
        self.job_name = job_name  # Job name passed as argument

    def _log(self, level_name, message, *args):
        formatted_message = f"[{self.job_name}] {message} " + " ".join(
            map(str, args)
        )
        level_name = level_name.upper()
        print(f"{level_name}: {formatted_message}")

    def debug(self, message, *args, **kwargs):
        if self.level <= logging.INFO:
            self._log("INFO", message, *args, **kwargs)

    def info(self, message, *args, **kwargs):
        if self.level <= logging.INFO:
            self._log("INFO", message, *args, **kwargs)

    def warning(self, message, *args, **kwargs):
        if self.level <= logging.WARNING:
            self._log("WARNING", message, *args, **kwargs)

    def error(self, message, *args, **kwargs):
        if self.level <= logging.ERROR:
            self._log("ERROR", message, *args, **kwargs)

    def critical(self, message, *args, **kwargs):
        if self.level <= logging.CRITICAL:
            self._log("CRITICAL", message, *args, **kwargs)



# Define the formatter
formatter = ProcessorFormatter(
    processor=structlog.dev.ConsoleRenderer(colors=False),
    foreign_pre_chain=[
        structlog.stdlib.add_log_level,
        structlog.processors.TimeStamper(fmt="iso"),
    ],
)

# File handler for logging to file
file_handler = logging.FileHandler(log_file_path, mode="a")
file_handler.setLevel(log_level)
file_handler.setFormatter(formatter)

# Console handler for critical messages only
console_handler = logging.StreamHandler(sys.stdout)
console_handler.setLevel(logging.CRITICAL)
console_handler.setFormatter(formatter)

# Logging config
logging.basicConfig(
    level=log_level,
    handlers=[file_handler, console_handler],
)

# Structlog configuration
structlog.configure(
    processors=[
        structlog.stdlib.filter_by_level,
        structlog.stdlib.add_logger_name,
        structlog.stdlib.add_log_level,
        structlog.processors.TimeStamper(fmt="%Y-%m-%dT%H:%M:%S.%f", utc=True),
        structlog.stdlib.PositionalArgumentsFormatter(),
        structlog.processors.StackInfoRenderer(),
        structlog.processors.format_exc_info,
        ProcessorFormatter.wrap_for_formatter,
    ],
    logger_factory=structlog.stdlib.LoggerFactory(),
    wrapper_class=structlog.make_filtering_bound_logger(log_level),
    cache_logger_on_first_use=True,
)


def get_logger(job_name):
    """
    Get a configured logger instance.

    Args:
        job_name (str): Unique job identifier.

    Returns:
        GluePrintLogger or structlog.BoundLogger depending on the environment.
    """
    if os.getenv("ETL_EXECUTION_ENV") == "AWS_GLUE":
        return GluePrintLogger(job_name)
    else:
        return structlog.get_logger().bind(Job=job_name)
