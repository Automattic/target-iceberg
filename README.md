# target-iceberg

`target-iceberg` is a Singer target for iceberg.

Build with the [Meltano Target SDK](https://sdk.meltano.com).

## Installation

Install locally:

```bash
pip install -e .
```

`constraints.txt` generation:

```bash
$ pip-compile --generate-hashes --output-file=constraints.txt
```
and replace hashed with tagged version in `constraints.txt`:
`target-iceberg @ git+https://github.com/Automattic/target-iceberg.git@v1.20.0 ; python_version >= "3.9" and python_version < "4"`

## Usage

You can easily run `target-iceberg` by itself or in a pipeline using [Meltano](https://meltano.com/).

### Executing the Target Directly

```bash
target-iceberg --version
target-iceberg --help
# Test using the "Smoke Test" tap:
tap-smoke-test | target-iceberg --config /path/to/target-iceberg-config.json
```

## Developer Resources

Follow these instructions to contribute to this project.

### Initialize your Development Environment

Prerequisites:

- Python 3.9+
- [uv](https://docs.astral.sh/uv/)

```bash
uv sync
```

### Create and Run Tests

Create tests within the `tests` subfolder and
  then run:

```bash
uv run pytest
```

You can also test the `target-iceberg` CLI interface directly using `uv run`:

```bash
uv run target-iceberg --help
```

### Testing with [Meltano](https://meltano.com/)

_**Note:** This target will work in any Singer environment and does not require Meltano.
Examples here are for convenience and to streamline end-to-end orchestration scenarios._

<!--
Developer TODO:
Your project comes with a custom `meltano.yml` project file already created. Open the `meltano.yml` and follow any "TODO" items listed in
the file.
-->

Next, install Meltano (if you haven't already) and any needed plugins:

```bash
# Install meltano
pipx install meltano
# Initialize meltano within this directory
cd target-iceberg
meltano install
```

Now you can test and orchestrate using Meltano:

```bash
# Test invocation:
meltano invoke target-iceberg --version

# OR run a test ELT pipeline with the Smoke Test sample tap:
meltano run tap-smoke-test target-iceberg
```

### SDK Dev Guide

See the [dev guide](https://sdk.meltano.com/en/latest/dev_guide.html) for more instructions on how to use the Meltano Singer SDK to
develop your own Singer taps and targets.
