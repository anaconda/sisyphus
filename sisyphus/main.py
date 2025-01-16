import click
import logging
import os

from .build import Build
from .host import Host


HELP_CONTEXT = dict(help_option_names=["-h", "--help"])


def setup_logging(log_level):
    """
    Setup logging for the whole application.
    """
    # All we want to see is the message level (DEBUG, INFO, etc...) and the actual message
    format = "%(levelname)s %(message)s"

    # Except when in DEBUG mode, then want to prefix that with a timestamp
    if log_level == "debug":
        format = "%(asctime)s " + format

    # Set the loggin level based on command-line option
    if log_level == "error":
        level = logging.ERROR
    elif log_level == "warning":
        level = logging.WARNING
    elif log_level == "info":
        level = logging.INFO
    elif log_level == "debug":
        level = logging.DEBUG
    logging.basicConfig(level=level, format=format)


@click.group(context_settings=HELP_CONTEXT)
def cli():
    pass


@cli.command(context_settings=HELP_CONTEXT)
@click.option("-H", "--host", required=True, help="IP or FQDN of the build host.")
@click.option("-l", "--log-level", type=click.Choice(["error", "warning", "info", "debug"], case_sensitive=False),
              default="info", show_default=True, help="Logging level.")
def prepare(host, log_level):
    """
    Prepare the host for building.
    """
    setup_logging(log_level)

    # Establish communication with the host
    h = Host(host)

    # Create work directories, setup conda, install CUDA if necessary, etc...
    h.prepare()

    # Wait for prepare to finish if necesary
    h.watch_prepare()


@cli.command(context_settings=HELP_CONTEXT)
@click.option("-H", "--host", required=True, help="IP or FQDN of the build host.")
@click.option("-P", "--package", required=True, help="Name of the package to build.")
@click.option("-B", "--branch", help="Branch to build from in the feedstock's repository.")
@click.option("-l", "--log-level", type=click.Choice(["error", "warning", "info", "debug"], case_sensitive=False),
              default="info", show_default=True, help="Logging level.")
def build(package, branch, host, log_level):
    """
    Build a package on the host.
    """
    setup_logging(log_level)

    # Establish communication with the host
    h = Host(host)

    # Prepare the host for building, it will automatically figure out if it has already run or not
    h.prepare()

    # Prepare and upload the data to the host
    b = Build(package, branch)
    b.upload_data(h)
    workdir = h.topdir + h.separator + package
    tarfile = h.topdir + h.separator + b.tarfile
    # Start from a blank slate, untar the data and cleanup
    h.rm(workdir)
    h.untar(tarfile, workdir)
    h.rm(tarfile)
    logging.info("Data ready on host")

    # Wait for prepare to finish if necesary
    h.watch_prepare()

    # Create a build directory, and build the package
    h.build(workdir)

    # Start watching the build process
    h.watch_build(workdir)


@cli.command(context_settings=HELP_CONTEXT)
@click.option("-H", "--host", required=True, help="IP or FQDN of the build host.")
@click.option("-P", "--package", help="Name of the package being built.")
@click.option("-l", "--log-level", type=click.Choice(["error", "warning", "info", "debug"], case_sensitive=False),
              default="info", show_default=True, help="Logging level.")
def watch(host, package, log_level):
    """
    Watch build in real-time if a package name is passed, otherwise watch the prepare process.
    Set exit code on error.
    """
    setup_logging(log_level)

    h = Host(host)
    if package:
        h.watch_build(h.topdir + h.separator + package)
    else:
        h.watch_prepare()


@cli.command(context_settings=HELP_CONTEXT)
@click.option("-H", "--host", required=True, help="IP or FQDN of the build host.")
@click.option("-P", "--package", required=True, help="Name of the package being built.")
@click.option("-C", "--channel", required=True, help="Target channel on anaconda.org to upload the packages.")
@click.option("-T", "--token", required=True, help="Token for the target channel on anaconda.org.")
@click.option("-l", "--log-level", type=click.Choice(["error", "warning", "info", "debug"], case_sensitive=False),
              default="info", show_default=True, help="Logging level.")
def upload(host, package, channel, token, log_level):
    """
    Upload built packages to anaconda.org.
    """
    setup_logging(log_level)

    h = Host(host)
    h.upload(package, channel, token)


@cli.command(context_settings=HELP_CONTEXT)
@click.option("-H", "--host", required=True, help="IP or FQDN of the build host.")
@click.option("-P", "--package", required=True, help="Name of the package being built.")
@click.option("-l", "--log-level", type=click.Choice(["error", "warning", "info", "debug"], case_sensitive=False),
              default="info", show_default=True, help="Logging level.")
def log(host, package, log_level):
    """
    Print build log to standard output (does not update in real-time).
    """
    setup_logging(log_level)

    h = Host(host)
    h.log(package)


@cli.command(context_settings=HELP_CONTEXT)
@click.option("-H", "--host", required=True, help="IP or FQDN of the build host.")
@click.option("-P", "--package", required=True, help="Name of the package being built.")
@click.option("-d", "--destination", help="Destination directory.")
@click.option("-a", "--all", is_flag=True, help="Download the whole work directory for debugging.")
@click.option("-l", "--log-level", type=click.Choice(["error", "warning", "info", "debug"], case_sensitive=False),
              default="info", show_default=True, help="Logging level.")
def download(host, package, destination, all, log_level):
    """
    Download built tarballs.
    """
    setup_logging(log_level)

    # The default desitination is the current working directory
    if not destination:
        destination = os.getcwd()

    h = Host(host)
    h.download(package, destination, all)


@cli.command(context_settings=HELP_CONTEXT)
@click.option("-H", "--host", required=True, help="IP or FQDN of the build host.")
@click.option("-P", "--package", required=True, help="Name of the package being built.")
@click.option("-l", "--log-level", type=click.Choice(["error", "warning", "info", "debug"], case_sensitive=False),
              default="info", show_default=True, help="Logging level.")
def transmute(host, package, log_level):
    """
    Transmute .tar.bz2 packages to .conda packages.
    """
    setup_logging(log_level)

    h = Host(host)
    h.transmute(package)


if __name__ == "__main__":
    cli()
