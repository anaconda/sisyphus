import fabric
import logging
import os
import re
import shutil
import sys
import tarfile
import time


LINUX_TYPE = "linux"
WINDOWS_TYPE = "windows"
LINUX_USER = "ec2-user"
WINDOWS_USER = "dev-admin"
LINUX_TOPDIR = "/tmp"       # These two have to be a directory,
WINDOWS_TOPDIR = "\\tmp"    # not the root of a device like / or \\
CONDA_PACKAGES = "conda-build distro-tooling::anaconda-linter git anaconda-client conda-package-handling"
BUILD_OPTIONS = "--error-overlinking -c ai-staging"
ACTIVATE = "conda activate sisyphus &&"


def print_dot():
    sys.stdout.write(".")
    sys.stdout.flush()


def print_newline():
    sys.stdout.write("\n")
    sys.stdout.flush()


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
            self.cat = "cat"
            self.run("conda init")
            self.pkgdir = "linux-64"
        elif self.__test_connection(WINDOWS_USER, "ver", WINDOWS_TYPE):
            self.type = WINDOWS_TYPE
            self.user = WINDOWS_USER
            self.separator = "\\"
            self.topdir = WINDOWS_TOPDIR
            self.touch = "copy nul"
            self.cat = "type"
            self.run("C:\\miniconda3\\Scripts\\conda.exe init")
            self.pkgdir = "win-64"
        else:
            logging.error("Couldn't connect to host '%s' or figure out what type it is", self.host)
            raise SystemExit(1)
        self.sisyphus_dir = self.path_join(self.topdir, "sisyphus")
        self.mkdir(self.sisyphus_dir)


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


    def path_join(self, *paths):
        """
        Join paths with the host separator.
        """
        path = self.separator.join(list(paths))
        # We need to deduplicate separators but, sadly, re.sub doesn't work here (because, you guessed it, Windows)
        cleaned_path = ""
        for c in path:
            if len(cleaned_path) > 0 and c == self.separator and cleaned_path[-1] == self.separator:
                continue
            cleaned_path += c
        return cleaned_path


    def path(self, *paths):
        """
        Build a path on the host from the Sisyphus directory and the given paths.
        """
        return self.path_join(self.sisyphus_dir, *paths)


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


    def reset_connection(self, wait=0):
        """
        Close the connection and open a new one after optionally waiting for a while.
        """
        self.connection.close()
        time.sleep(wait)
        self.connection = fabric.Connection(user=self.user, connect_timeout=10, host=self.host)


    def prepare(self):
        """
        Prepare the remote host for building.
        """
        # Create the top-level work directory
        self.mkdir(self.sisyphus_dir)

        # Does the sisyphus environment exist?
        found = False
        touch = f"{self.touch} {self.path("conda.")}"
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
            redirect = f"{self.path("conda.log")} 2>&1"
            self.run_async(f"{conda_cmd} > {redirect} && {touch}ready || {touch}failed")
            logging.info("Environment 'sisyphus' is being created")

        # Windows hosts need to have CUDA installed by the user
        if self.type == WINDOWS_TYPE:
            if self.exists(self.path("cuda_driver.log")) or self.exists(self.path("cuda_12.3.0.log")):
                logging.info("CUDA is already installed or being installed")
            else:
                # Using multiple powershell calls from cmd because the && operator doesn't exist in the old version we're using
                start = "powershell -ExecutionPolicy ByPass -File \\prefect\\install_"
                middle = f".ps1 > {self.sisyphus_dir}\\"
                end = ".log 2>&1"
                cuda_driver = f"{start}cuda_driver{middle}cuda_driver{end}"
                cuda_12_3_0 = f"{start}cuda_12.3.0{middle}cuda_12.3.0{end}"
                touch = f"{self.touch} {self.path("cuda.")}"
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
        builddir = self.path_join(workdir, "build")
        cbc = self.path_join(workdir, "conda_build_config.yaml")
        feedstock = self.path_join(workdir, "feedstock")
        logfile = self.path_join(workdir, "build.log")
        cmd = f"conda build {BUILD_OPTIONS} -e {cbc} --croot={builddir} {feedstock}"
        touch = f"{self.touch} {self.path_join(workdir, "build.")}"
        self.mkdir(builddir)
        self.run_async(f"{ACTIVATE} {cmd} > {logfile} 2>&1 && {touch}ready || {touch}failed")
        logging.info("Build is starting")


    def watch_build(self, workdir):
        """
        Show the build process in real-time.
        """
        # Avoid overflowing the fabric connection
        max_lines = 1000

        logfile = self.path_join(workdir, "build.log")
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
            if self.exists(self.path_join(workdir, "build.ready")):
                logging.info("Build complete")
                break
            if self.exists(self.path_join(workdir, "build.failed")):
                logging.error("Build Failed")
                raise SystemExit(1)
            self.reset_connection(10)


    def watch_prepare(self):
        """
        Watch the prepare process.
        """
        error = False
        logging.info("Waiting for Conda setup to finish")
        print_dot()
        while True:
            if self.exists(self.path("conda.ready")):
                print_newline()
                logging.info("Conda is ready")
                break
            elif self.exists(self.path("conda.failed")):
                print_newline()
                logging.warning("Conda setup failed")
                error = True
                break
            print_dot()
            self.reset_connection(10)

        if self.type == WINDOWS_TYPE:
            logging.info("Waiting for CUDA installation to finish")
            print_dot()
            while True:
                if self.exists(self.path("cuda.ready")):
                    print_newline()
                    logging.info("CUDA is ready")
                    break
                elif self.exists(self.path("cuda.failed")):
                    print_newline()
                    logging.warning("CUDA installation failed")
                    error = True
                    break
                print_dot()
                self.reset_connection(30)

        if error:
            raise SystemExit(1)

        self.reset_connection()


    def upload(self, package, channel, token):
        """
        Upload build packages to anaconda.org.
        """
        pkgdir = self.path(package, "build", self.pkgdir)
        logging.info("Uploading packages in: %s", pkgdir)
        logging.info("To channel: %s", channel)
        r = self.connection.run(f"{ACTIVATE} anaconda -t {token} upload -c {channel} --force {pkgdir}{self.separator}*.tar.bz2")
        logging.info("Done")
        self.reset_connection()


    def status(self, package):
        """
        Print the build status.
        """
        files = self.ls(self.path(package))
        if "build.ready" in files:
            return "Complete"
        if "build.failed" in files:
            return "Failed"
        if "build.log" in files:
            return "Building"
        return "Not started"


    def wait(self, package):
        """
        Wait for the build to finish and set the return value accordingly.
        """
        waiting_to_start = False
        waiting_to_finish = False
        while True:
            status = self.status(package)
            if status == "Complete":
                print_newline()
                logging.info("Build complete")
                return True
            if status == "Failed":
                print_newline()
                logging.error("Build failed")
                return False
            if status == "Not started":
                if not waiting_to_start:
                    logging.info("Waiting for build to start")
                    waiting_to_start = True
                print_dot()
            if status == "Building":
                if waiting_to_start:
                    print_newline()
                    waiting_to_start = False
                if not waiting_to_finish:
                    logging.info("Waiting for build to finish")
                    waiting_to_finish = True
                print_dot()
            self.reset_connection(60)
        self.reset_connection()


    def log(self, package):
        """
        Return the build log.
        """
        logging.info("Downloading the build log")
        logfile = self.path(package, "build.log")
        r = self.run(f"{self.cat} {logfile}")
        self.reset_connection()
        return r


    def download(self, package, destination, all=False):
        """
        Download build artifacts from the remote host.
        """
        # Transmute packages if needed
        self.transmute(package)

        # Check whether there are packages to download, if not bail out
        builddir = self.path(package, "build")
        pkgdir = self.path_join(builddir, self.pkgdir)
        files = [self.path_join(self.pkgdir, f) for f in self.ls(pkgdir) if f.endswith('.tar.bz2') or f.endswith('.conda')]
        if not files:
            logging.warning("No build artifacts to download")
            return

        tf_name = f"sisyphus_{package}_{self.type}.tar"
        tf = self.path_join(self.topdir, tf_name)


        # Create a tarball containing either just packages or the whole build directory
        try:
            if all:
                logging.info("Downloading complete Sisyphus data at '%s'", self.sisyphus_dir)
                self.run(f"cd {self.topdir} && tar -cf {tf} sisyphus")
            else:
                logging.info("Downloading %d build artifacts in '%s'", len(files), pkgdir)
                if self.type == LINUX_TYPE:
                    self.run(f"cd {builddir} && tar -cf {tf} {" ".join(files)} 2>/dev/null || true")
                elif self.type == WINDOWS_TYPE:
                    self.run(f'cd {builddir} && tar -cf {tf} {" ".join(files)} 2>nul )', quiet=True)

        except Exception as e:
            logging.error(f"Failed to create tar file: {str(e)}")
            raise SystemExit(1)

        # Verify the tar file was created
        if not self.exists(tf):
            logging.error("Tar file '%s' is missing", tf)
            raise SystemExit(1)

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
                shutil.rmtree(os.path.join(dest, self.pkgdir))
        except:
            pass
        os.chdir(dest)

        logging.debug(f"Attempting to download from remote path: {tf}")
        self.reset_connection()
        try:
            self.connection.get(tf.replace("\\", "/"))
        except Exception as e:
            logging.error(f"Download failed for {tf}: {str(e)}")
            raise SystemExit(1)
        with tarfile.open(tf_name, "r") as tar:
            tar.extractall()
        self.reset_connection()

        # Cleanup
        os.remove(tf_name)
        self.rm(tf)

        logging.info("Done")


    def transmute(self, package):
        """
        Transmute .tar.bz2 packages to .conda packages and vice-versa.
        """
        pkgdir = self.path(package, "build", self.pkgdir)
        all_bz2_pkgs = [p for p in self.ls(pkgdir) if p.endswith(".tar.bz2")]
        all_conda_pkgs = [p for p in self.ls(pkgdir) if p.endswith(".conda")]
        bz2_pkgs = [p for p in all_bz2_pkgs if re.sub("tar.bz2$", "conda", p) not in all_conda_pkgs]
        conda_pkgs = [p for p in all_conda_pkgs if re.sub("conda$", "tar.bz2", p) not in all_bz2_pkgs]
        for bz2_pkg in bz2_pkgs:
            logging.info("Transmuting %s to .conda", bz2_pkg)
            self.reset_connection()
            self.run(f"{ACTIVATE} cd {pkgdir} && cph t {bz2_pkg} .conda")
        for conda_pkg in conda_pkgs:
            logging.info("Transmuting %s to .tar.bz2", conda_pkg)
            self.reset_connection()
            self.run(f"{ACTIVATE} cd {pkgdir} && cph t {conda_pkg} .tar.bz2")

        self.reset_connection()
