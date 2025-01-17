import fabric
import logging
import paramiko
import os
import shutil
import tarfile
import time


LINUX_TYPE = "linux"
WINDOWS_TYPE = "windows"
LINUX_USER = "ec2-user"
WINDOWS_USER = "dev-admin"
LINUX_TOPDIR = "/tmp/sisyphus"
WINDOWS_TOPDIR = "\\sisyphus"
CONDA_PACKAGES = "conda-build distro-tooling::anaconda-linter git anaconda-client conda-package-handling"
BUILD_OPTIONS = "--error-overlinking -c ai-staging"
ACTIVATE = "conda activate sisyphus &&"


class Host:
    def __init__(self, host):
        """
        Detect the remote host type and initialize the instance.
        """
        self.host = host

        if self.__test_connection(LINUX_USER, "uname -a", LINUX_TYPE):
            self.type = LINUX_TYPE
            self.user = LINUX_USER
            self.separator = "/"
            self.topdir = LINUX_TOPDIR
            self.touch = "touch"
            self.run("conda init")
        elif self.__test_connection(WINDOWS_USER, "ver", WINDOWS_TYPE):
            self.type = WINDOWS_TYPE
            self.user = WINDOWS_USER
            self.separator = "\\"
            self.topdir = WINDOWS_TOPDIR
            self.touch = "copy nul"
            self.run("C:\\miniconda3\\Scripts\\conda.exe init")
        else:
            logging.error("Couldn't connect to host '%s' or figure out what type it is", self.host)
            raise SystemExit(1)
        self.mkdir(self.topdir)


    def __test_connection(self, user, cmd, type):
        """
        Verify we can connect and run a test command in order to try and identify the host type.
        """
        logging.debug("Attempting to connect to '%s' assuming it's %s")
        self.connection = fabric.Connection(user=user, connect_timeout=10, host=self.host)
        try:
            r = self.connection.run(cmd, hide=True)
        except:
            logging.debug("Couldn't connect to host '%s' or it isn't '%s'", self.host, type.capitalize())
            self.connection.close()
            return False
        else:
            logging.debug(r.stdout.lstrip().rstrip())
            logging.info("'%s' is a %s host", self.host, type.capitalize())
            return True


    def run(self, cmd, quiet=False):
        """
        Wrapper to run a command on the remote host, log automatically, and report errors if any.
        """
        try:
            r = self.connection.run(cmd, hide=True)
        except Exception as e:
            if not quiet:
                logging.error("%s", e)
                raise SystemExit(1)
        else:
            logging.debug("Running '%s'", cmd)
            stdout = r.stdout.lstrip().rstrip()
            for line in stdout.splitlines():
                logging.debug(line)
            return stdout


    def run_async(self, cmd):
        """
        Launch a background command on the remote host, no error reporting since we're not waiting for exit.
        """
        logging.debug("Running asynchronously '%s'", cmd)
        self.connection.run(cmd, asynchronous=True)


    def exists(self, path):
        """
        Check if remote file or directory exists
        """
        if self.type == LINUX_TYPE:
            # Using single-quotes for the variable to avoid expansion
            r = self.run(f"if [[ -e '{path}' ]]; then echo Yes; fi")
        elif self.type == WINDOWS_TYPE:
            # Windows wants double-quotes for the variable
            r = self.run(f'if exist "{path}" echo Yes')
        if r == "Yes":
            logging.debug("'%s' exists", path)
            return True
        else:
            logging.debug("'%s' doesn't exist", path)
            return False


    def isdir(self, path):
        """
        Check if a remote path is a directory.
        """
        if self.type == LINUX_TYPE:
            r = self.run(f"if [[ -d '{path}' ]]; then echo Yes; fi")
        elif self.type == WINDOWS_TYPE:
            r = self.run(f'if exist "{path}\\*" echo Yes')
        if r == "Yes":
            logging.debug("'%s' is a directory", path)
            return True
        else:
            logging.debug("'%s' isn't a directory", path)
            return False


    def mkdir(self, path):
        """
        Create a remote directory.
        """
        if self.exists(path):
            if self.isdir(path):
                logging.debug("Directory '%s' already exists")
                return
            else:
                logging.error("'%s' already exists and is a file, can't create directory")
                raise SystemExit(1)
        if self.type == LINUX_TYPE:
            self.run(f"mkdir -p {path}")
        elif self.type == WINDOWS_TYPE:
            self.run(f'mkdir "{path}"')


    def ls(self, path):
        """
        Outputs a simple list of the contents of a remote directory.
        """
        if self.type == LINUX_TYPE:
            out = self.run(f"ls -1A {path}")
        elif self.type == WINDOWS_TYPE:
            out = self.run(f'dir /b "{path}"')
        return out.splitlines()


    def rm(self, path):
        """
        Delete a remote file or directory.
        """
        if self.exists(path):
            if self.type == LINUX_TYPE:
                self.run(f"rm -rf {path}")
            elif self.type == WINDOWS_TYPE:
                if self.isdir(path):
                    self.run(f'rd /s /q "{path}"')
                else:
                    self.run(f'del "{path}"')


    def untar(self, filepath, dest):
        """
        Untar a remote file into a remote directory.
        """
        # Create the destination directory in case it doesn't exist
        self.mkdir(dest)
        self.run(f"tar -x -f {filepath} -C {dest}")


    def prepare(self):
        """
        Prepare the remote host for building.
        """
        # Create the top-level work directory
        self.mkdir(self.topdir)

        # Does the sisyphus environment exist?
        found = False
        touch = f"{self.touch} {self.topdir}{self.separator}conda."
        r = self.run("conda env list")
        for line in r.splitlines():
            if line.startswith("sisyphus "):
                found = True
                break
        if found:
            logging.info("Environment 'sisyphus' already exists")
            self.run(f"{touch}ready")
        else:
            # It doesn't, so let's create it
            conda_cmd = f"conda create -y -n sisyphus {CONDA_PACKAGES}"
            redirect = f"{self.topdir}{self.separator}conda.log 2>&1"
            self.run_async(f"{conda_cmd} > {redirect} && {touch}ready || {touch}failed")
            logging.info("Environment 'sisyphus' is being created")

        # Windows hosts need to have CUDA installed by the user
        if self.type == WINDOWS_TYPE:
            if self.exists(f"{self.topdir}{self.separator}cuda_driver.log") or self.exists(f"{self.topdir}{self.separator}cuda_12.3.0.log"):
                logging.info("CUDA is already installed or being installed")
            else:
                # Using multiple powershell calls from cmd because the && operator doesn't exist in the old version we're using
                start = "powershell -ExecutionPolicy ByPass -File \\prefect\\install_"
                middle = f".ps1 > {self.topdir}\\"
                end = ".log 2>&1"
                cuda_driver = f"{start}cuda_driver{middle}cuda_driver{end}"
                cuda_12_3_0 = f"{start}cuda_12.3.0{middle}cuda_12.3.0{end}"
                touch = f"{self.touch} {self.topdir}{self.separator}cuda."
                self.run_async(f"{cuda_driver} && {cuda_12_3_0} && {touch}ready || {touch}failed")
                logging.info("CUDA is being installed")


    def put(self, source, dest):
        """
        Upload a local file to a remote directory.
        """
        # fabric won't handle backslashes and volume names in paths, so don't use the latter and replace the former
        if self.type == WINDOWS_TYPE:
            dest = dest.replace("\\", "/")
        logging.debug("Uploading '%s' to '%s'", source, dest)
        self.connection.put(source, dest)


    def build(self, workdir):
        """
        Build a feedstock with the conda config both in a remote directory.
        """
        builddir = f"{workdir}{self.separator}build"
        cbc = f"{workdir}{self.separator}conda_build_config.yaml"
        feedstock = f"{workdir}{self.separator}feedstock"
        logfile = f"{workdir}{self.separator}build.log"
        cmd = f"conda build {BUILD_OPTIONS} -e {cbc} --croot={builddir} {feedstock}"
        touch = f"{self.touch} {workdir}{self.separator}build."
        self.mkdir(builddir)
        self.run_async(f"{ACTIVATE} {cmd} > {logfile} 2>&1 && {touch}ready || {touch}failed")
        logging.info("Build is running")


    def watch_build(self, workdir):
        """
        Show the build process in real-time.
        """
        # Set the wait time between updates in seconds
        wait = 3
        # Avoid overflowing the fabric connection
        max_lines = 1000

        logfile = f"{workdir}{self.separator}build.log"
        if self.type == LINUX_TYPE:
            skip_pre = 'tail -n +'
            skip_post = f' "{logfile}" | tail -n {max_lines}'
        elif self.type == WINDOWS_TYPE:
            skip_pre = f'powershell -Command "Get-Content {logfile} | Select-Object -Skip '
            skip_post = f' | Select-Object -Last {max_lines}"'

        lines_read = 0
        while True:
            r = self.run(f'{skip_pre}{lines_read}{skip_post}')
            lines = r.splitlines()
            for line in lines:
                logging.info(line)
            lines_read += len(lines)
            # Quit watching when the build.ready or build.failed files show up
            if self.exists(f"{workdir}{self.separator}build.ready"):
                logging.info("Build complete")
                break
            if self.exists(f"{workdir}{self.separator}build.failed"):
                logging.error("Build Failed")
                raise SystemExit(1)
            time.sleep(wait)


    def watch_prepare(self):
        """
        Watch the prepare process.
        """
        # Set the wait time between updates in seconds
        wait = 3

        error = False
        messaged = False
        while True:
            if self.exists(self.topdir + self.separator + "conda.ready"):
                logging.info("Conda is ready")
                break
            elif self.exists(self.topdir + self.separator + "conda.failed"):
                logging.warning("Conda setup failed")
                error = True
                break
            if not messaged:
                logging.info("Waiting for Conda setup to complete")
                messaged = True
            time.sleep(wait)

        if self.type == WINDOWS_TYPE:
            messaged = False
            while True:
                if self.exists(self.topdir + "\\cuda.ready"):
                    logging.info("CUDA is ready")
                    break
                elif self.exists(self.topdir + "\\cuda.failed"):
                    logging.warning("CUDA installation failed")
                    error = True
                    break
                if not messaged:
                    logging.info("Waiting for CUDA installation to complete")
                    messaged = True
                time.sleep(wait)

        if error:
            raise SystemExit(1)


    def upload(self, package, channel, token):
        """
        Upload build packages to anaconda.org.
        """
        pkgdir = f"{self.topdir}{self.separator}{package}{self.separator}build{self.separator}"
        if self.type == LINUX_TYPE:
            pkgdir = f"{pkgdir}linux-64"
        elif self.type == WINDOWS_TYPE:
            pkgdir = f"{pkgdir}win-64"
        logging.info("Uploading packages in: %s", pkgdir)
        logging.info("To channel: %s", channel)
        r = self.connection.run(f"{ACTIVATE} anaconda -t {token} upload -c {channel} --force {pkgdir}{self.separator}*.tar.bz2")
        logging.info("Done")


    def log(self, package):
        """
        Watch the build if a package name is passed, otherwise watch the prepare process.
        """
        logfile = f"{self.topdir}{self.separator}{package}{self.separator}build.log"
        if self.type == LINUX_TYPE:
            cat= "cat"
        elif self.type == WINDOWS_TYPE:
            cat= "type"
        r = self.run(f"{cat} {logfile}")
        print(r)


    def download(self, package, destination, all=False):
        """
        Download build tarballs from the remote host.
        """
        # Wait for the build to finish
        wait = 60
        failed = False
        while True:
            if self.exists(f"{self.topdir}{self.separator}{package}{self.separator}build.ready"):
                logging.info("Build complete")
                break
            if self.exists(f"{self.topdir}{self.separator}{package}{self.separator}build.failed"):
                logging.info("Build Failed")
                failed = True
                break
            logging.info("Waiting for the build to finish")
            # Close the connection because over a very long time it can silently die
            self.connection.close()
            time.sleep(wait)
            # Re-open the connection
            self.connection = fabric.Connection(user=self.user, connect_timeout=10, host=self.host)

        builddir = f"{self.topdir}{self.separator}{package}{self.separator}build"
        if self.type == LINUX_TYPE:
            pkgdir = "linux-64"
            tf = "/tmp/sisyphus.tar"
        elif self.type == WINDOWS_TYPE:
            pkgdir = "win-64"
            tf = "\\sisyphus.tar"
        logging.info("Downloading package tarballs in '%s%s%s'", builddir, self.separator, pkgdir)

        # Create a tarball containing either just packages or the whole build directory
        if all:
            self.run(f"cd {self.topdir} && cd .. && tar -cf {tf} sisyphus")
        else:
            if self.type == LINUX_TYPE:
                self.run(f"cd {builddir} && tar -cf {tf} {pkgdir}/*.conda {pkgdir}/*.tar.bz2 2>/dev/null || true")
            elif self.type == WINDOWS_TYPE:
                self.run(f'cd {builddir} && tar -cf {tf} $(dir /s {pkgdir}\\*.conda {pkgdir}\\*.tar.bz2 2>nul )', quiet=True)

        # Download and untar it
        dest = os.path.join(destination, package)
        # Create the local destination directory if it doesn't exist
        try:
            os.makedirs(dest)
        except:
            pass
        # Delete the previous builds for the same package if any
        try:
            if all:
                shutil.rmtree(os.path.join(dest, "sisyphus"))
            else:
                shutil.rmtree(os.path.join(dest, pkgdir))
        except:
            pass
        os.chdir(dest)
        self.connection.get(tf.replace("\\", "/"))
        with tarfile.open("sisyphus.tar", "r") as tar:
            tar.extractall()

        # Cleanup
        os.remove("sisyphus.tar")
        self.rm(tf)

        if failed:
            logging.warning("Packages downloaded but the build failed")
        logging.info("Done")