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
- Access to rocket-platform dev instances ([documentation][1])
- Active Anaconda VPN connection
- Unix-like system (Linux, MacOS) for running the tool

Sisyphus automates the build processes documented in:
- [Building GPU packages][3]
- [Updating the Llama.cpp Conda Package][4]


## Setup & Usage

You will need to have `conda` and `git` installed.

```
conda create -n sisyphus -c conda-forge python pip
conda activate sisyphus
git clone git@github.com:anaconda/sisyphus.git
cd sisyphus
git submodule update --init
pip install -e .
```


### TL;DR (full example)

```
export GITHUB_TOKEN=<your SSO-activated token with workflow scope>
sisyphus auto --linux llama.cpp
```

This will create a host, build the package, download the build artifacts and log, then stop the host if the build is successful.
Pay attention to suggestions telling you how to watch the live build process in a different terminal, or how to ssh onto the host for debugging.
You will get a reminder to stop the host in situations where it isn't stopped automatically, and instructions on how to do so.


### Getting help

Get general help with:

```
> sisyphus --help
```

Get help for a specific command with:

```
> sisyphus <command> --help
```

For example:

```
> sisyphus auto --help
```

Commands often have options not discussed here for the sake of brevity.


### Automated end-to-end build process

Run a complete end-to-end build with:

```
> sisyphus auto [--linux|--windows] [-d <destination>] <package>
```

or

```
> sisyphus auto -H <host> [-d <destination>] <package>
```

The `auto` command combines the functionality of `start-host`, `build`, `download`, and optionally `stop-host` into a single command.

If no host is provided with `-H`, it will automatically:
1. Create a new host based on the `--linux` or `--windows` flag
2. Prepare the host for building
3. Build the package
4. Download the build artifacts and log file to the specified destination (or current directory if not specified)
5. Stop the host if the build was successful

If a host is provided with `-H`, it will use that host for building and downloading but won't stop it when finished.

This is ideal for unattended builds where you want to start a process and come back later to the completed artifacts.

Example:
```
> sisyphus auto --linux -d ~/builds llama.cpp
```

This will create a Linux host, build the llama.cpp package, download all artifacts to ~/builds, and stop the host when complete.


### Start a new host

Start a new linux host with:

```
sisyphus start-host --linux
```

Start a new windows host with:

```
sisyphus start-host --windows
```

This will create a new GPU instance using rocket-platform and return its IP address. By default, it creates a Linux `g4dn.4xlarge` instance with a 24-hour lifetime.

You will need to provide a GitHub token for authentication (`workflow` scope, SSO authenticated). Either set the `GITHUB_TOKEN` environment variable or pass the `--token` option.

> [!NOTE]
> The system may sometimes fail to retrieve the workflow run from rocket-platform. This is an infrequent but known bug which we haven't been able to resolve yet.
> When that happens, you will have to go to https://github.com/anaconda-distribution/rocket-platform/actions/workflows/start.yml to locate the workflow (look for your user name and the triggered time).
> Select it, click on `Start`, `Start Instance`, then locate the `INSTANCE_IDS` variable which will list your instance ID.
> Finally go to https://github.com/anaconda-distribution/rocket-platform/actions/workflows/stop.yml, and use this ID to run a workflow to stop the instance.
> Then run the `start-host` command again.


### Prepare the host

This step is optional. The `build` command will automatically prepare the host if needed.

The `prepare` command will create a work directory, set up Conda, and create a Conda environment.
On Windows, it will also install CUDA software and drivers, which takes more time.
This only needs to be done once.

To prepare the host, run:

```
> sisyphus prepare -H <host>
```

Where `<host>` is the IP address or FQDN of your remote host.

Remember to make sure your SSH key is correctly configured (see [rocket platform dev instance docs][1]).

> [!NOTE]
> You do not need to define the host type, Sisyphus will automatically detect if the remote host is Linux or Windows.
> It will immediately disconnect from the host but the preparation will continue on the host asynchronously protecting against local system, network or vpn issues.

Logs for the Conda and, on Windows, both CUDA jobs are saved on the remote host in the work directory.
On Linux this is at `/tmp/sisyphus`, and on Windows it's at `C:\tmp\sisyphus`.
When `ssh`ing to the host for checking these logs, remember you should login with the `ec2-user` name on Linux and `dev-admin` on Windows.


### Watch the preparation progress

This step is for when you run the `prepare` command manually, and only useful in case you lose the connection to the host during the prepare process (unlikely but not impossible).

To watch the preparation process, do:

```
> sisyphus watch -H <host>
```

On the default logging level (info), the output will inform in real-time of the status.
In case of failure, an error exit code will be returned for use in automation, or even a Shell script or one-liner.


### Build the package

Start a build with:

```
> sisyphus build -H <host> -P <package>
```

`<package>` is the package name as written in the URL for the feedstock.
For example, if the URL is `https://github.com/AnacondaRecipes/llama.cpp-feedstock`, then `<package>` is `llama.cpp`.

Sisyphus will prepare the host to run CUDA builds if needed, prepare all the data locally, upload it to the host, start the build, then show the build process in real-time (unless `--no-watch` is specified).

If you lose connection to the host during the build process, which isn't unusual, you can use the `watch` command like below to resume watching the build process. Losing the connection will never interrupt builds.


### Watch the build process

This command is useful in case you lose the connection to the host during the build process, which is a common occurrence.

It's the same as above for the preparation, except this time we pass the package name.

```
> sisyphus watch -H <host> -P <package>
```

On the default logging level, sisyphus will show the build output in real-time.
Here too, an exit code will be returned at the end for use in automation.


### Check the build status

```
> sisyphus status -H <host> -P <package>
```

This will print one of the following statuses:
- `Not started`: The build hasn't started yet
- `Building`: The build is currently running
- `Complete`: The build finished successfully
- `Failed`: The build failed

The command returns immediately without waiting for the build to finish.


### Wait for build completion

```
> sisyphus wait -H <host> -P <package>
```

Wait for the build to finish and return an exit code of 0 if the build succeeded, or 1 if it failed.

This is useful for automation and scripting.


### Print or download the build log

```
> sisyphus log -H <host> -P <package>
```

Print the build log in your terminal. By default, it will wait for the build to finish before printing the log, unless `--no-wait` is specified.
The output can, and probably should, be piped to a pager like `less` or be redirected to a file to save it.


### Transmute packages

This step is optional. The `download` command will automatically transmute packages as needed before downloading them.

Transmute build artifacts with:

```
sisyphus transmute -H <host> -P <package>
```

Sisyphus will automaticaly convert all `.tar.bz2` packages to `.conda` packages, and vice-versa, as needed.


### Download build artifacts

Download build artifacts with:

```
sisyphus download -H <host> -P <package>
```

Sisyphus will automatically transmute packages as needed before downloading them.


### Uploading packages to anaconda.org

Upload packages to anaconda.org with:

```
sisyphus upload -H <host> -P <package> -C <channel> -t <token>
```

> [!IMPORTANT]
> Windows packages need to be signed first.
> Upload the packages to a temporary channel, then run the code signing action at
> https://github.com/anaconda-distribution/rocket-platform/actions/workflows/codesign-windows.yml


### Stop the host

Don't forget to stop the host when you're done. Hosts cost money per hour they run.

Stop a host with:

```
sisyphus stop-host  <host>
```

Where `<host>` can be either the IP address or the instance ID. If using the IP address, the tool will automatically retrieve the instance ID from the host before stopping it.

You will need to provide a GitHub token for authentication. Either set the `GITHUB_TOKEN` environment variable or pass the `--token` option.


[1]: https://github.com/anaconda-distribution/rocket-platform/tree/main/machine-images#dev-instances
[2]: https://github.com/anaconda-distribution/rocket-platform/actions/workflows/start.yml
[3]: https://github.com/anaconda-distribution/perseverance-skills/blob/main/sections/02_Package_building/01_How_tos/Building_GPU_packages.md
[4]: https://anaconda.atlassian.net/wiki/spaces/~7120206a3789e73a844699b3e4eb79b01a8c23/pages/3889627143/Updating+the+Llama.cpp+Conda+Package
