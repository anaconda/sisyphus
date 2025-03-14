import click
import logging
import os
import shutil

from .build import Build
from .host import Host
from .util import create_gpu_instance, stop_instance


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

    logging.getLogger("httpx").setLevel(logging.WARNING)
    logging.getLogger("paramiko").setLevel(logging.WARNING)


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


def prepare_and_build(host, package, branch):
    """
    Prepare a host and build a package.
    """
    workdir = host.path(package)

    # Prepare the host for building
    host.prepare()

    # Prepare and upload the data to the host
    b = Build(package, branch)
    b.upload_data(host)
    tarfile = host.path(b.tarfile)

    # Start from a blank slate, untar the data and cleanup
    host.rm(workdir)
    host.untar(tarfile, workdir)
    host.rm(tarfile)
    logging.info("Data ready on host")

    # Wait for prepare to finish if necessary
    host.watch_prepare()

    # Create a build directory, and build the package
    host.build(workdir)


@cli.command(context_settings=HELP_CONTEXT)
@click.option("-H", "--host", required=True, help="IP or FQDN of the build host.")
@click.option("-P", "--package", required=True, help="Name of the package to build.")
@click.option("-B", "--branch", help="Branch to build from in the feedstock's repository.")
@click.option("--no-watch", is_flag=True, default=False, help="Don't watch the build process after it starts.")
@click.option("-l", "--log-level", type=click.Choice(["error", "warning", "info", "debug"], case_sensitive=False),
              default="info", show_default=True, help="Logging level.")
def build(package, branch, host, no_watch, log_level):
    """
    Build a package on the host.
    """
    setup_logging(log_level)

    # Establish communication with the host
    h = Host(host)

    prepare_and_build(h, package, branch)

    # Start watching the build process if not disabled
    if not no_watch:
        h.watch_build(h.path(package))


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
        h.watch_build(h.path(package))
    else:
        h.watch_prepare()


@cli.command(context_settings=HELP_CONTEXT)
@click.option("-H", "--host", required=True, help="IP or FQDN of the build host.")
@click.option("-P", "--package", required=True, help="Name of the package being built.")
@click.option("-C", "--channel", required=True, help="Target channel on anaconda.org to upload the packages.")
@click.option("-t", "--token", required=True, help="Token for the target channel on anaconda.org.")
@click.option("-l", "--log-level", type=click.Choice(["error", "warning", "info", "debug"], case_sensitive=False),
              default="info", show_default=True, help="Logging level.")
def upload(host, package, channel, token, log_level):
    """
    Upload built packages on the remote host to anaconda.org.
    """
    setup_logging(log_level)

    h = Host(host)
    h.upload(package, channel, token)


@cli.command(context_settings=HELP_CONTEXT)
@click.option("-H", "--host", required=True, help="IP or FQDN of the build host.")
@click.option("-P", "--package", required=True, help="Name of the package being built.")
@click.option("--no-wait", is_flag=True, default=False, help="Don't wait for the build to finish before printing the log.")
@click.option("-l", "--log-level", type=click.Choice(["error", "warning", "info", "debug"], case_sensitive=False),
              default="info", show_default=True, help="Logging level.")
def log(host, package, no_wait, log_level):
    """
    Print the build log to standard output (does not update in real-time).
    """
    setup_logging(log_level)

    h = Host(host)
    # Wait for the build to finish unless no_wait is specified
    if not no_wait:
        h.wait(package)

    print(h.log(package))


@cli.command(context_settings=HELP_CONTEXT)
@click.option("-H", "--host", required=True, help="IP or FQDN of the build host.")
@click.option("-P", "--package", required=True, help="Name of the package being built.")
@click.option("-d", "--destination", help="Destination directory.")
@click.option("-a", "--all", is_flag=True, help="Download the whole work directory for debugging.")
@click.option("-l", "--log-level", type=click.Choice(["error", "warning", "info", "debug"], case_sensitive=False),
              default="info", show_default=True, help="Logging level.")
def download(host, package, destination, all, log_level):
    """
    Download built packages from the remote host.
    """
    setup_logging(log_level)

    h = Host(host)
    # Wait for the build to finish
    h.wait(package)

    # The default desitination is the current working directory
    if not destination:
        destination = os.getcwd()

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
    h.wait(package)
    h.transmute(package)


@cli.command(context_settings=HELP_CONTEXT)
@click.option("-H", "--host", required=True, help="IP or FQDN of the build host.")
@click.option("-P", "--package", required=True, help="Name of the package being built.")
@click.option("-l", "--log-level", type=click.Choice(["error", "warning", "info", "debug"], case_sensitive=False),
              default="info", show_default=True, help="Logging level.")
def status(host, package, log_level):
    """
    Print the build status.
    """
    setup_logging(log_level)

    h = Host(host)
    print(h.status(package))


@cli.command(context_settings=HELP_CONTEXT)
@click.option("-H", "--host", required=True, help="IP or FQDN of the build host.")
@click.option("-P", "--package", required=True, help="Name of the package being built.")
@click.option("-l", "--log-level", type=click.Choice(["error", "warning", "info", "debug"], case_sensitive=False),
              default="info", show_default=True, help="Logging level.")
def wait(host, package, log_level):
    """
    Wait for the build to finish and set exit code based on result.
    """
    setup_logging(log_level)

    h = Host(host)
    if not h.wait(package):
        raise SystemExit(1)


@cli.command(context_settings=HELP_CONTEXT)
@click.option("--linux", is_flag=True, help="Create a Linux GPU instance.")
@click.option("--windows", is_flag=True, help="Create a Windows GPU instance.")
@click.option("-t", "--instance-type", type=click.Choice(["g4dn.4xlarge", "p3.2xlarge"]),
              default="g4dn.4xlarge", show_default=True, help="EC2 GPU instance type.")
@click.option("--lifetime", default="24", show_default=True,
              help="Hours before instance termination.")
@click.option("--token", help="GitHub token (defaults to GITHUB_TOKEN environment variable).")
@click.option("-l", "--log-level", type=click.Choice(["error", "warning", "info", "debug"], case_sensitive=False),
              default="info", show_default=True, help="Logging level.")
def start_host(linux, windows, instance_type, lifetime, token, log_level):
    """
    Create a Linux or Windows GPU instance using rocket-platform.
    """
    setup_logging(log_level)

    if not linux and not windows:
        raise click.UsageError("Either --linux or --windows must be specified")
    if linux and windows:
        raise click.UsageError("Only one of --linux or --windows can be specified")

    create_gpu_instance(token, linux, instance_type, lifetime)


@cli.command(context_settings=HELP_CONTEXT)
@click.argument("id_or_ip")
@click.option("--token", help="GitHub token (defaults to GITHUB_TOKEN environment variable).")
@click.option("-l", "--log-level", type=click.Choice(["error", "warning", "info", "debug"], case_sensitive=False),
              default="info", show_default=True, help="Logging level.")
def stop_host(id_or_ip, token, log_level):
    """
    Stop a GPU instance by ID or IP using rocket-platform.
    """
    setup_logging(log_level)

    stop_instance(token, id_or_ip)


@cli.command(context_settings=HELP_CONTEXT)
@click.argument("package")
@click.option("-H", "--host", help="IP or FQDN of the build host.")
@click.option("-B", "--branch", help="Branch to build from in the feedstock's repository.")
@click.option("-d", "--destination", help="Destination directory for downloaded packages and logs.")
@click.option("--linux", is_flag=True, help="Automatically create a Linux GPU instance.")
@click.option("--windows", is_flag=True, help="Automatically create a Windows GPU instance.")
@click.option("--do-not-stop-host", is_flag=True, help="Do not stop the host at the end of the process.")
@click.option("-t", "--instance-type", type=click.Choice(["g4dn.4xlarge", "p3.2xlarge"]),
              default="g4dn.4xlarge", show_default=True, help="EC2 GPU instance type.")
@click.option("--lifetime", default="24", show_default=True,
              help="Hours before instance termination.")
@click.option("--token", help="GitHub token (defaults to GITHUB_TOKEN environment variable).")
@click.option("-l", "--log-level", type=click.Choice(["error", "warning", "info", "debug"], case_sensitive=False),
              default="info", show_default=True, help="Logging level.")
def auto(host, package, branch, destination, linux, windows, do_not_stop_host, instance_type, lifetime, token, log_level):
    """
    Create a build host if needed, build the package, and download build log and artifacts.
    """
    setup_logging(log_level)

    # If no host is provided, create a new one
    if not host:
        if not linux and not windows:
            raise click.UsageError("Either --linux or --windows must be specified when no host is provided")
        if linux and windows:
            raise click.UsageError("Only one of --linux or --windows can be specified")
        # Create the GPU instance
        h = create_gpu_instance(token, linux, instance_type, lifetime)
    else:
        if linux or windows:
            raise click.UsageError("Either --linux or --windows can't be specified when a host is provided")
        # Establish communication with the host
        h = Host(host)

    # The default destination is the current working directory
    if not destination:
        destination = os.getcwd()
    # Create the destination subdirectory for the log in case there are no build artifacts and it doesn't get created
    local_pkgdir = os.path.join(os.path.abspath(destination), package, h.pkgdir)
    shutil.rmtree(local_pkgdir, ignore_errors=True)
    os.makedirs(local_pkgdir)

    prepare_and_build(h, package, branch)
    logging.info("Build started, you can watch it live in another terminal with:")
    logging.info("    sisyphus watch -H %s -P %s", h.host, package)

    # Wait for the build to finish and capture the success status (to be used later)
    build_success = h.wait(package)

    # Download the built artifacts
    h.download(package, destination, False)

    # Get the build log
    log_content = h.log(package)
    # Save the log to a file
    log_file_path = os.path.join(local_pkgdir, "build.log")
    with open(log_file_path, 'w') as log_file:
        log_file.write(log_content)

    if build_success:
        logging.info("Build successful, artifacts and log are in %s", local_pkgdir)
        if host:
            logging.info("A host was specified manually, so we won't stop it automatically")
        elif do_not_stop_host:
            logging.info("Not stopping the host, as requested")
        else:
            stop_instance(token, h.host)
            raise SystemExit(0)
    else:
        logging.error("Build failed, you can ssh onto the host with:")
        logging.error("    ssh %s@%s", h.user, h.host)
        logging.error("The work directory is at %s", h.path(package))

    logging.warning("Please don't forget to stop the instance once you're done with it:")
    logging.warning("    sisyphus stop-host %s", h.host)

if __name__ == "__main__":
    cli()
