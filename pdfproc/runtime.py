import asyncio
import logging
import signal


def configure_logging(level: str) -> None:
    logging.basicConfig(
        level=level.upper(),
        format="%(asctime)s %(levelname)-7s %(name)s: %(message)s",
    )
    logging.getLogger("aiokafka").setLevel(logging.WARNING)
    logging.getLogger("elastic_transport").setLevel(logging.WARNING)


def install_signal_handlers() -> asyncio.Event:
    """Devuelve un Event que se activa con SIGINT/SIGTERM para un apagado ordenado."""
    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, stop.set)
    return stop
