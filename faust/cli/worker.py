"""Program ``faust worker`` used to start application from console."""
import asyncio
import os
import platform
import socket
from typing import Any, List, Optional, Tuple, Type, cast

from mode import ServiceT, Worker
from mode.utils.imports import symbol_by_name
from mode.utils.logging import level_name
from yarl import URL

from faust.worker import Worker as FaustWorker
from faust.types import AppT
from faust.types._env import WEB_BIND, WEB_PORT, WEB_TRANSPORT
from faust.utils.terminal.tables import TableDataT

from . import params
from .base import AppCommand, now_builtin_worker_options, option

__all__ = ['worker']

FAUST = 'ƒaµS†'

# XXX mypy borks if we do `from faust import __version`.
faust_version: str = symbol_by_name('faust:__version__')


class worker(AppCommand):
    """Start worker instance for given app."""

    daemon = True
    redirect_stdouts = True

    worker_options = [
        option('--with-web/--without-web',
               default=True,
               help='Enable/disable web server and related components.'),
        option('--web-port', '-p',
               default=None, type=params.TCPPort(),
               help=f'Port to run web server on (default: {WEB_PORT})'),
        option('--web-transport',
               default=None, type=params.URLParam(),
               help=f'Web server transport (default: {WEB_TRANSPORT})'),
        option('--web-bind', '-b', type=str),
        option('--web-host', '-h',
               default=socket.gethostname(), type=str,
               help=f'Canonical host name for the web server '
                    f'(default: {WEB_BIND})'),
    ]

    options = (cast(List, worker_options) +
               cast(List, now_builtin_worker_options))

    def on_worker_created(self, worker: Worker) -> None:
        """Print banner when worker starts."""
        pass

    def as_service(self, loop: asyncio.AbstractEventLoop,
                   *args: Any, **kwargs: Any) -> ServiceT:
        """Return the service this command should execute.

        For the worker we simply start the application itself.

        Note:
            The application will be started using a :class:`faust.Worker`.
        """
        pass

    def _init_worker_options(self,
                             *args: Any,
                             with_web: bool,
                             web_port: Optional[int],
                             web_bind: Optional[str],
                             web_host: str,
                             web_transport: URL,
                             **kwargs: Any) -> None:
        pass

    @property
    def _Worker(self) -> Type[Worker]:
        # using Faust worker to start the app, not command code.
        pass

    def banner(self, worker: Worker) -> str:
        """Generate the text banner emitted before the worker starts."""
        pass

    def _format_banner_table(self, data: TableDataT) -> str:
        pass

    def _banner_title(self) -> str:
        pass

    def _banner_data(self, worker: Worker) -> TableDataT:
        pass

    def _human_cython_info(self) -> Optional[Tuple[str, str]]:
        pass

    def _human_transport_info(self, loop: Any) -> str:
        # uvloop didn't leave us with any way to identify itself,
        # and also there's no uvloop.__version__ attribute.
        pass

    def _driver_versions(self, app: AppT) -> List[str]:
        pass

    def faust_ident(self) -> str:
        """Return Faust version information as ANSI string."""
        pass

    def platform(self) -> str:
        """Return platform identifier as ANSI string."""
        pass
