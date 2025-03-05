import logging
import os
import tarfile
import tempfile
import zipfile

from . import util


CBC_URL = "https://raw.githubusercontent.com/AnacondaRecipes/aggregate/master/conda_build_config.yaml"
GITHUB_API="https://api.github.com/repos/AnacondaRecipes/"
FEEDSTOCK_PREFIX="https://github.com/AnacondaRecipes/"
FEEDSTOCK_SUFFIX="-feedstock"
CBC_YAML = "conda_build_config.yaml"


class Build:
    """
    Create a build object, prepare and upload the data, etc...
    """
    def __init__(self, package, branch):
        """
        Initialize variables
        """
        self.package = package
        logging.info("Package: %s", self.package)

        # If the feedstock branch isn't set we need to figure out what is the default for this repository
        if branch:
            self.branch = branch
        else:
            logging.warning("Feedstock branch isn't set, using default for this repository")
            self.branch = util.query_api(GITHUB_API + self.package + FEEDSTOCK_SUFFIX)["default_branch"]
        logging.info("Branch: %s", self.branch)


    def __patch_cbc(self):
        """
        Patch the Conda build config.
        """
        cbc_path = os.path.join(self.workdir, CBC_YAML)
        logging.debug("Patching '%s'", cbc_path)

        # Load the file
        with open(cbc_path, "r") as f:
            cbc = f.read()

        # Patch it
        cbc = cbc.replace("vs2019", "vs2022")
        # Add anything else here as needed

        # Rewrite it in place
        with open(cbc_path, "w") as f:
            f.write(cbc)
        logging.info("Patched '%s'", CBC_YAML)


    def upload_data(self, host):
        """
        Prepare the data locally instead of doing that on the host, which is inconvenient especially on Windows.
        """
        initial_dir = os.getcwd()
        tmpdir = tempfile.TemporaryDirectory()
        self.workdir = tmpdir.name
        logging.debug("Local work directory: %s", self.workdir)

        # Download and patch the Conda build config
        util.download(CBC_URL, os.path.join(self.workdir, CBC_YAML))
        logging.info("Downloaded '%s'", CBC_YAML)
        self.__patch_cbc()

        # We download an archive so we don't need to have git installed and shell out to it (which is ugly)
        feedstock_url = FEEDSTOCK_PREFIX + self.package + FEEDSTOCK_SUFFIX + "/archive/refs/heads/" + self.branch + ".zip"
        zip_file_path = os.path.join(self.workdir, self.package + "_" + self.branch + ".zip")

        # Save the archive as a file because we don't want to clobber RAM
        util.download(feedstock_url, zip_file_path)
        with zipfile.ZipFile(zip_file_path, 'r') as zip_ref:
            zip_ref.extractall(self.workdir)
        logging.info("Downloaded feedstock")

        # Rename the feedstock directory to 'feedstock' so that the name is known after we upload everything to the host
        with zipfile.ZipFile(zip_file_path, 'r') as zip_file:
            feedstock_dir_name = zip_file.namelist()[0].split("/")[0]
        os.rename(os.path.join(self.workdir, feedstock_dir_name), os.path.join(self.workdir, "feedstock"))

        # tar the data because uploading recursively to a Windows host is a major pain
        self.tarfile = self.package + ".tar"
        os.chdir(self.workdir)
        with tarfile.open(self.tarfile, "a") as tf:
            tf.add(CBC_YAML)
            tf.add("feedstock")
        logging.info("Data archive ready to upload")

        # Finally upload the data to the host
        # We're doing it from within this method because the temporary directory is very volatile
        host.put(self.tarfile, host.sisyphus_dir)
        logging.info("Data archive uploaded")

        os.chdir(initial_dir)
