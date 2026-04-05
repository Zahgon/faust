"""Worker.

A "worker" starts a single instance of a Faust application.

See Also:
    :ref:`app-starting`: for more information.
"""
import asyncio
import logging
import os
import sys

from collections import defaultdict
from itertools import chain
from pathlib import Path
from typing import Any, Dict, IO, Iterable, Mapping, Optional, Set, Union

import mode
from aiokafka.structs import TopicPartition
from mode import ServiceT, get_logger
from mode.utils.logging import Severity, formatter2

from .types import AppT, SensorT, TP, TopicT
from .types._env import CONSOLE_PORT, DEBUG
from .utils import terminal
from .utils.functional import consecutive_numbers

try:  # pragma: no cover
    # if installed we use this to set ps.name (argv[0])
    from setproctitle import setproctitle
except ImportError:  # pragma: no cover
    def setproctitle(title: str) -> None: ...  # noqa

__all__ = ['Worker']

#: Name prefix of process in ps/top listings.
PSIDENT = '[Faust:Worker]'

TP_TYPES = (TP, TopicPartition)

logger = get_logger(__name__)


@formatter2
def format_log_arguments(
        arg: Any, record: logging.LogRecord) -> Any:  # pragma: no cover
    # This adds custom formatting to certain log messages.

    pass


def _partition_set_logtable(arg: Iterable[TP]) -> str:
    pass


def _repr_partition_set(s: Set[int]) -> str:
    """Convert set of partition numbers to human readable form.

    This will consolidate ranges of partitions to make them easier
    to read.

    Example:
        >>> partitions = {1, 2, 3, 7, 8, 9, 10, 34, 35, 36, 37, 38, 50}
        >>> _repr_partition_set(partitions)
        '{1-3, 7-10, 34-38, 50}'
    """
    pass


def _iter_consecutive_numbers(s: Iterable[int]) -> Iterable[str]:
    """Find consecutive number ranges from an iterable of integers.

    The number ranges are represented as strings (e.g. ``"3-14"``)

    Example:
        >>> numbers = {1, 2, 3, 7, 8, 9, 10, 34, 35, 36, 37, 38, 50}
        >>> list(_iter_consecutive_numbers(numbers))
        [1-3, 7-10, 34-38, 50]
    """
    pass


class Worker(mode.Worker):
    """Worker.

    See Also:
        This is a subclass of :class:`mode.Worker`.

    Usage:
        You can start a worker using:

            1) the :program:`faust worker` program.

            2) instantiating Worker programmatically and calling
               `execute_from_commandline()`::

                    >>> worker = Worker(app)
                    >>> worker.execute_from_commandline()

            3) or if you already have an event loop, calling ``await start``,
               but in that case *you are responsible for gracefully shutting
               down the event loop*::

                    async def start_worker(worker: Worker) -> None:
                        await worker.start()

                    def manage_loop():
                        loop = asyncio.get_event_loop()
                        worker = Worker(app, loop=loop)
                        try:
                            loop.run_until_complete(start_worker(worker)
                        finally:
                            worker.stop_and_shutdown_loop()

    Arguments:
        app: The Faust app to start.
        *services: Services to start with worker.
            This includes application instances to start.

        sensors (Iterable[SensorT]): List of sensors to include.
        debug (bool): Enables debugging mode [disabled by default].
        quiet (bool): Do not output anything to console [disabled by default].
        loglevel (Union[str, int]): Level to use for logging, can be string
            (one of: CRIT|ERROR|WARN|INFO|DEBUG), or integer.
        logfile (Union[str, IO]): Name of file or a stream to log to.
        stdout (IO): Standard out stream.
        stderr (IO): Standard err stream.
        blocking_timeout (float): When :attr:`debug` is enabled this
            sets the timeout for detecting that the event loop is blocked.
        workdir (Union[str, Path]): Custom working directory for the process
            that the worker will change into when started.
            This working directory change is permanent for the process,
            or until something else changes the working directory again.
        loop (asyncio.AbstractEventLoop): Custom event loop object.
    """

    logger = logger

    #: The Faust app started by this worker.
    app: AppT

    #: Additional sensors to add to the Faust app.
    sensors: Set[SensorT]

    #: Current working directory.
    #: Note that if passed as an argument to Worker, the worker
    #: will change to this directory when started.
    workdir: Path

    #: Class that displays a terminal progress spinner (see :pypi:`progress`).
    spinner: Optional[terminal.Spinner]

    #: Set by signal to avoid printing an OK status.
    _shutdown_immediately: bool = False

    def __init__(self,
                 app: AppT,
                 *services: ServiceT,
                 sensors: Iterable[SensorT] = None,
                 debug: bool = DEBUG,
                 quiet: bool = False,
                 loglevel: Union[str, int] = None,
                 logfile: Union[str, IO] = None,
                 stdout: IO = sys.stdout,
                 stderr: IO = sys.stderr,
                 blocking_timeout: float = None,
                 workdir: Union[Path, str] = None,
                 console_port: int = CONSOLE_PORT,
                 loop: asyncio.AbstractEventLoop = None,
                 redirect_stdouts: bool = None,
                 redirect_stdouts_level: Severity = None,
                 logging_config: Dict = None,
                 **kwargs: Any) -> None:
        self.app = app
        self.sensors = set(sensors or [])
        self.workdir = Path(workdir or Path.cwd())
        conf = app.conf
        if redirect_stdouts is None:
            redirect_stdouts = conf.worker_redirect_stdouts
        if redirect_stdouts_level is None:
            redirect_stdouts_level = (
                conf.worker_redirect_stdouts_level or logging.INFO)
        if logging_config is None and app.conf.logging_config:
            logging_config = dict(app.conf.logging_config)
        super().__init__(
            *services,
            debug=debug,
            quiet=quiet,
            loglevel=loglevel,
            logfile=logfile,
            loghandlers=app.conf.loghandlers,
            stdout=stdout,
            stderr=stderr,
            blocking_timeout=blocking_timeout or 0.0,
            console_port=console_port,
            redirect_stdouts=redirect_stdouts,
            redirect_stdouts_level=redirect_stdouts_level,
            logging_config=logging_config,
            loop=loop,
            **kwargs)
        self.spinner = terminal.Spinner(file=self.stdout)

    async def on_start(self) -> None:
        """Signal called every time the worker starts."""
        pass

    def _on_sigint(self) -> None:
        pass

    def _on_sigterm(self) -> None:
        pass

    def _flag_as_shutdown_by_signal(self) -> None:
        pass

    async def maybe_start_blockdetection(self) -> None:
        """Start blocking detector service if enabled."""
        pass

    async def on_startup_finished(self) -> None:
        """Signal called when worker has started."""
        pass

    def _on_startup_end_spinner(self) -> None:
        pass

    def _on_shutdown_immediately(self) -> None:
        pass

    def on_init_dependencies(self) -> Iterable[ServiceT]:
        """Return service dependencies that must start with the worker."""
        pass

    async def on_first_start(self) -> None:
        """Signal called the first time the worker starts.

        First time, means this callback is not called if the
        worker is restarted by an exception being raised.
        """
        pass

    def change_workdir(self, path: Path) -> None:
        """Change the current working directory (CWD)."""
        pass

    def autodiscover(self) -> None:
        """Autodiscover modules and files to find @agent decorators, etc."""
        pass

    def _setproctitle(self, info: str, *, ident: str = PSIDENT) -> None:
        pass

    def _proc_ident(self) -> str:
        pass

    def _proc_web_ident(self) -> str:
        pass

    async def on_execute(self) -> None:
        """Signal called when the worker is about to start."""
        pass

    def on_worker_shutdown(self) -> None:
        """Signal called before the worker is shutting down."""
        pass

    def on_setup_root_logger(self, logger: logging.Logger, level: int) -> None:
        """Signal called when the root logger is being configured."""
        pass

    def _disable_spinner_if_level_below_WARN(self, level: int) -> None:
        pass

    def _setup_spinner_handler(
            self, logger: logging.Logger, level: int) -> None:
        pass
