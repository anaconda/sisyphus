# Sisyphus - GPU Package Build Automation Tool

<img src="sisyphus.png" alt="Sisyphus" width="40%" height="40%"/>

Sisyphus is a command-line automation tool that streamlines the process of building GPU (CUDA) enabled and other packages across Linux, Windows and other platforms. Sisyphus integrates with Anaconda's existing rocket-platform infrastructure to eliminate manual toil from the GPU package building process.

Key features:
- Automated environment and host preparation including CUDA setup.
- Remote build process management across Linux (x64, aarch64), Windows and other platforms.
- Real-time build monitoring and logging.
- Asynchronous operations to protect against local system, network or vpn issues resulting in lost work and time.
- Automated package upload and distribution handling to specific channels.
- Seamless integration with existing Anaconda build infrastructure

Sisyphus currently supports building the following packages with more planned:

- llama.cpp: CPU, CUDA, and hardware optimized versions


## Prerequisites

To use Sisyphus, you need:
- Access to rocket-platform dev instances ([documentation][1], TL:DR: run the [workflow][2], choose `linux-64` or `win-64`, select `g4dn.4xlarge` for the instance type, do not set a CUDA toolkit version)
- Active Anaconda VPN connection
- Unix-like system (Linux, MacOS) for running the tool

Sisyphus automates the build processes documented in:
- [Building GPU packages][3]
- [Updating the Llama.cpp Conda Package][4]


## Setup & Usage

Sisyphus uses [`conda-project`](https://github.com/conda-incubator/conda-project).
You will need to have `conda` and `conda-project` installed.

```
conda install -c conda-forge conda-project
```

Activating the conda project environment and installing the sisyphus package:

```
cd sisyphus
conda project activate
pip install -e . --no-deps
```

### TL;DR (full example)

Build `llama.cpp` on host `1.2.3.4`:

```
sisyphus build -H 1.2.3.4 -P llama.cpp
```

This will connect to the host, determine if it's Linux or Windows, prepare it to run CUDA builds, prepare the build,
run it, then show the progress in real-time. When the build completes, you can retrieve the built packages like this:

```
sisyphus download -H 1.2.3.4 -P llama.cpp
```


### Getting help

```
> sisyphus --help
Usage: sisyphus [OPTIONS] COMMAND [ARGS]...

Options:
  -h, --help  Show this message and exit.

Commands:
  build     Build a package on the host.
  download  Download built tarballs.
  log       Print build log to standard output (does not update in real-time).
  prepare   Prepare the host for building.
  upload    Upload built packages to anaconda.org.
  watch     Watch build in real-time if a package name is passed, otherwise watch the prepare process.
```

### Preparing the host

The remote host can optionally be prepared manually to run CUDA builds.
This is done automatically by the `build` subcommand, so in a normal workflow it isn't needed.

The `prepare` subcommand will create a work directory, set up Conda, and create a Conda environment.
On Windows, it will also install CUDA software and drivers, which takes more time.
This only needs to be done once.

```
> sisyphus prepare --help
Usage: sisyphus prepare [OPTIONS]

  Prepare the host for building.

Options:
  -H, --host TEXT                 IP or FQDN of the build host  [required]
  -l, --log-level [error|warning|info|debug]
                                  Logging level  [default: info]
  -h, --help                      Show this message and exit.
```

To prepare the host, run:

```
> sisyphus prepare -H <host>
```

Where `<host>` is the IP address or FQDN of your remote host.

Remember to make sure your SSH key is correctly configured (see [rocket platform dev instance docs][1]).

Notes:
- You do not need to define the host type, Sisyphus will automatically detect if the remote host is Linux or Windows.
- It will immediately disconnect from the host but the preparation will continue on the host asynchronously protecting against local system, network or vpn issues.

Logs for the Conda and, on Windows, both CUDA jobs are saved on the remote host in the work directory.
On Linux this is at `/tmp/sisyphus`, and on Windows it's at `C:\sisyphus`.
When `ssh`ing to the host for checking these logs, remember you should login with the `ec2-user` name on Linux and `dev-admin` on Windows.


### Watching the preparation progress

This command is useful in case you lose the connection to the host during the prepare process (unlikely but not impossible).

```
> sisyphus watch --help
Usage: sisyphus watch [OPTIONS]

  Watch build in real-time if a package name is passed, otherwise watch the
  prepare process. Set exit code on error.

Options:
  -H, --host TEXT                 IP or FQDN of the build host  [required]
  -P, --package TEXT              Name of the package being built
  -l, --log-level [error|warning|info|debug]
                                  Logging level  [default: info]
  -h, --help                      Show this message and exit.
```

To watch the above preparation process, do:

```
> sisyphus watch -H <host>
```

On the default logging level (info), the output will inform in real-time of the status.
In case of failure, an error exit code will be returned for use in automation, or even a Shell script or one-liner.


### Building the package

```
> sisyphus build --help
Usage: sisyphus build [OPTIONS]

  Build a package on the host.

Options:
  -H, --host TEXT                 IP or FQDN of the build host  [required]
  -P, --package TEXT              Name of the package to build  [required]
  -B, --branch TEXT               Branch to build from in the feedstock's
                                  repository
  -l, --log-level [error|warning|info|debug]
                                  Logging level  [default: info]
  -h, --help                      Show this message and exit.
```

Start the build with:

```
> sisyphus build -H <host> -P <package>
```

`<package>` is the package name as written in the URL for the feedstock.
For example, if the URL is `https://github.com/AnacondaRecipes/llama.cpp-feedstock`, then `<package>` is `llama.cpp`.

Sisyphus will prepare the host to run CUDA builds if needed, prepare all the data locally, upload it to the host, start the build, then show the build process in real-time.

If you lose connection to the host during the build process, which isn't unusual, you can use the `watch` command like bellow to resume watching the build process. Losing the connection will never interrupt builds.


### Watching the build process

This command is useful in case you lose the connection to the host during the build process, which is a common occurrence.

It's the same as above for the preparation, except this time we pass the package name.

```
> sisyphus watch -H <host> -P <package>
```

On the default logging level sisyphus will show the build output in real-time.
Here too, an exit code will be returned at the end for use in automation.


### Print or download the build log

```
❯ sisyphus log --help
Usage: sisyphus log [OPTIONS]

  Print build log to standard output (does not update in real-time).

Options:
  -H, --host TEXT                 IP or FQDN of the build host.  [required]
  -P, --package TEXT              Name of the package being built.  [required]
  -l, --log-level [error|warning|info|debug]
                                  Logging level.  [default: info]
  -h, --help                      Show this message and exit.
```

This will print the build log in your terminal.
The output can be piped to a pager like `less` or be redirected to a file to save it.


### Download built packages

```
❯ sisyphus download --help
Usage: sisyphus download [OPTIONS]

  Download built tarballs.

Options:
  -H, --host TEXT                 IP or FQDN of the build host.  [required]
  -P, --package TEXT              Name of the package being built.  [required]
  -d, --destination TEXT          Destination directory.
  -a, --all                       Download the whole work directory for
                                  debugging.
  -l, --log-level [error|warning|info|debug]
                                  Logging level.  [default: info]
  -h, --help                      Show this message and exit.
```

Download packages with:

```
sisyphus download -H <host> -P <package>
```


### Uploading packages to anaconda.org

```
> sisyphus upload --help
Usage: sisyphus upload [OPTIONS]

  Upload build packages to anaconda.org.

Options:
  -H, --host TEXT                 IP or FQDN of the build host.  [required]
  -P, --package TEXT              Name of the package being built.  [required]
  -C, --channel TEXT              Target channel on anaconda.org to upload the
                                  packages.  [required]
  -T, --token TEXT                Token for the target channel on
                                  anaconda.org.  [required]
  -l, --log-level [error|warning|info|debug]
                                  Logging level.  [default: info]
  -h, --help                      Show this message and exit.
```

Upload packages with:

```
sisyphus upload -H <host> -P <package> -C <channel> -T <token>
```

> [!IMPORTANT]
> Windows packages need to be signed first.
> Upload the packages to a temporary channel, then run the code signing action at
> https://github.com/anaconda-distribution/rocket-platform/actions/workflows/codesign-windows.yml


### Transmute packages

```
> sisyphus transmute --help
Usage: sisyphus transmute [OPTIONS]

  Transmute .tar.bz2 packages to .conda packages.

Options:
  -H, --host TEXT                 IP or FQDN of the build host.  [required]
  -P, --package TEXT              Name of the package being built.  [required]
  -l, --log-level [error|warning|info|debug]
                                  Logging level.  [default: info]
  -h, --help                      Show this message and exit.
```

Transmute packages with:

```
sisyphus transmute -H <host> -P <package>
```


[1]: https://github.com/anaconda-distribution/rocket-platform/tree/main/machine-images#dev-instances
[2]: https://github.com/anaconda-distribution/rocket-platform/actions/workflows/start.yml
[3]: https://github.com/anaconda-distribution/perseverance-skills/blob/main/sections/02_Package_building/01_How_tos/Building_GPU_packages.md
[4]: https://anaconda.atlassian.net/wiki/spaces/~7120206a3789e73a844699b3e4eb79b01a8c23/pages/3889627143/Updating+the+Llama.cpp+Conda+Package
