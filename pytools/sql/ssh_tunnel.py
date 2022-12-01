import os
import subprocess
from typing import NamedTuple, Optional, Sequence


class PortForward(NamedTuple):
    local_port: int
    host: str
    port: int
    bind_address: Optional[str] = None

    def __str__(self) -> str:
        if self.bind_address:
            return f"{self.bind_address}:{self.local_port}:{self.host}:{self.port}"
        return f"{self.local_port}:{self.host}:{self.port}"


class SSHTunnel(subprocess.Popen):
    def __init__(
        self,
        *,
        bastion_host: Optional[str] = None,
        port_forwards: Optional[Sequence[PortForward]] = None,
        compress: bool = True,
        connect_timeout: int = 5,
    ) -> None:
        """
        Opens a SSH tunnel to a bastion host and forwards ports.
        Allows multiple port forwards to be specified.
        """
        self.compress = compress
        self.port_forwards = list(port_forwards) if port_forwards else []
        self.bastion_host = bastion_host or os.environ["BASTION_HOST"]
        self.connect_timeout = connect_timeout

        port_forward_args = []
        for port_forward in self.port_forwards:
            port_forward_args.extend(["-L", str(port_forward)])

        args = [
            "/usr/bin/ssh",
            *(["-C"] if self.compress else []),
            "-f",  # go to background
            "-q",  # quiet
            "-o",
            "ExitOnForwardFailure yes",
            *port_forward_args,
            self.bastion_host,
            "sleep",
            str(self.connect_timeout),
        ]

        super().__init__(args)  # type: ignore
        # Ignore a bug in mypy thinking `args` will be passed to `object.__init__`,
        # which takes zero arguments. Evidently this code works at run-time.
        # <https://github.com/python/mypy/issues/4335>
        # <https://github.com/python/mypy/issues/5887>
