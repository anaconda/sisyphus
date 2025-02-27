import json
import logging
import sys
import urllib.request
import urllib.error

from .host import Host
from pushbutan.src.pushbutan.pushbutan import Pushbutan


def download(url, path):
    """
    Download a file and save it.
    """
    logging.debug("Downloading '%s'", url)
    try:
        with urllib.request.urlopen(url) as response, open(path, 'wb') as out_file:
            out_file.write(response.read())
    except urllib.error.HTTPError as e:
        logging.error("HTTP Error: %s - %s", e.code, e.reason)
        raise SystemExit(1)
    except urllib.error.URLError as e:
        logging.error("URL Error: %s", e.reason)
        raise SystemExit(1)
    # TODO More exceptions (file)?


def query_api(url):
    """
    Query an API and return the JSON structure.
    """
    logging.debug("Querying '%s'", url)
    try:
        with urllib.request.urlopen(url) as response:
            return json.loads(response.read().decode("utf-8"))
    except urllib.error.HTTPError as e:
        logging.error("HTTP Error: %s - %s", e.code, e.reason)
        raise SystemExit(1)
    except urllib.error.URLError as e:
        logging.error("URL Error: %s", e.reason)
        raise SystemExit(1)


def create_gpu_instance(token, linux, instance_type, lifetime):
    """
    Create a GPU instance using rocket-platform.

    Args:
        token: str, GitHub token for authentication
        linux: bool, True for Linux instance, False for Windows
        instance_type: str, EC2 instance type
        lifetime: str, hours before instance termination

    Returns:
        str: IP address of the created instance

    Raises:
        SystemExit: If instance creation fails
    """
    try:
        pb = Pushbutan(token)
        if linux:
            logging.info(f"Creating Linux GPU instance ({instance_type})...")
            result = pb.trigger_linux_gpu_instance(
                instance_type=instance_type,
                lifetime=lifetime
            )
        else:
            logging.info(f"Creating Windows GPU instance ({instance_type})...")
            result = pb.trigger_windows_gpu_instance(
                instance_type=instance_type,
                lifetime=lifetime
            )

        instance = pb.wait_for_instance(result["run_id"])
        ip = instance['ip_address']
        id = instance['instance_id']
        h = Host(ip)
        h.run(f"echo {id} > {h.path('instance_id')}")
        logging.info(f"Instance ready at: {ip} (ID: {id})")
        return h

    except Exception as e:
        logging.error(f"Failed to create instance: {e}")
        raise SystemExit(1)


def stop_instance(token, id):
    """
    Stop a GPU instance using rocket-platform.

    Args:
        token: str, GitHub token for authentication
        instance_identifier: str, either IP address or instance ID

    Raises:
        SystemExit: If instance termination fails
    """
    try:
        pb = Pushbutan(token)

        # If the identifier looks like an IP address, we need to get the ID first
        if '.' in id:  # Simple check for IP address format
            # Read the instance ID from the remote file
            h = Host(id)
            try:
                id = h.run(f"{h.cat} {h.path('instance_id')}").strip()
                logging.info(f"Instance ID: {id}")
            except Exception as e:
                logging.error(f"Failed to get instance ID from IP {id}: {e}")
                raise SystemExit(1)

        pb.stop_instance(id)
        logging.info("Instance %s stopped successfully", id)

    except Exception as e:
        logging.error(f"Failed to stop instance: {e}")
        raise SystemExit(1)
