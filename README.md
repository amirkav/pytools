![Code style](https://github.com/amirkav/pytools/workflows/Code%20style/badge.svg)
![Unit tests](https://github.com/amirkav/pytools/workflows/Unit%20tests/badge.svg)
![Integration tests](https://github.com/amirkav/pytools/workflows/Integration%20tests/badge.svg)

A collection of Python common tools and facilities for connecting to databases, interacting with cloud resourecs, working with Python objects, etc.

- [Tools](#tools)
  - [Installation](#installation)
  - [Environment variables](#environment-variables)
  - [Development](#development)
    - [IDE and environment](#ide-and-environment)
    - [Using GitHub Actions CI](#using-github-actions-ci)
  - [Versioning](#versioning)
  - [Latest changes](#latest-changes)

### Installation

**NOTE**: Mac users will need the `pg_config` binary installed via the `postgresql` package in Homebrew:

```bash
brew install postgresql
```

Add the following to requirements file of dependent projects (`pyproject.toml`, `Pipfile`, `requirements.txt`, etc.):

```
pytools = { path = "../pytools/", develop = true }
```

Or, if you have an internal PyPi server:

```
pip install -i "https://${INTERNAL_PYPI_USERNAME}:${INTERNAL_PYPI_PASS}@${INTERNAL_PYPI_URL}" pytools
```

### Required dependencies

```
# packages for file_inspect parsers for AlpineLinux

sudo apk add antiword

# packages for file_inspect parsers for Debian Linux

sudo apt install build-essential antiword

# packages for file_inspect parsers for MacOS

brew install antiword

```

### Environment variables

These env variables affect `tools` modules:

- `AWS_REGION` - Region for `Boto3Connect` classes
- `ENV` - Environment for `Boto3Connect` classes
- `DEBUG_MODE` - Sets default log level to DEBUG if set to `true`

## Development

### Setup [repo_checker](https://github.com/amirkav/repo_checker)

```bash
# for fellow VSCode users: setup formatting
pre_commit -c vscode --fix

# install pre-commit hook
pre_commit -c pre_commit --fix

# run pre_commit once to make sure that your env is ready
pre_commit
```

### Using GitHub Actions CI

- Use GitHub releases to create a Release PR that bumps version and updates `CHANGELOG.md`
- Follow [keep a changelog](https://keepachangelog.com/en/1.0.0/) format in release and PR notes
- Do not update `<package>/version.txt` and `CHANGELOG.md` manually to avoid PR conflicts
- Run `repo_checker` after you edit `pyproject.toml` `[tool.repo_checker]` section

## Versioning

`pytools` version follows [PEP 440](https://www.python.org/dev/peps/pep-0440/).

## Latest changes

Full changelog can be found in [Changelog](./CHANGELOG.md).
Release notes can be found in [Releases](https://github.com/amirkav/pytools/releases).
