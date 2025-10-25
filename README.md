[![ Logo OpenStudioLandscapes ](https://github.com/michimussato/OpenStudioLandscapes/raw/main/media/images/logo128.png)](https://github.com/michimussato/OpenStudioLandscapes)

***

1. [Feature: OpenStudioLandscapes-Template](#feature-openstudiolandscapes-template)
   1. [Brief](#brief)
   2. [Requirements](#requirements)
   3. [Install](#install)
      1. [This Feature](#this-feature)
   4. [Add to OpenStudioLandscapes](#add-to-openstudiolandscapes)
   5. [Testing](#testing)
      1. [pre-commit](#pre-commit)
      2. [nox](#nox)
   6. [Variables](#variables)
      1. [Feature Configs](#feature-configs)
2. [Community](#community)
3. [Create new Feature from this Template](#create-new-feature-from-this-template)
   1. [Create a new repository from this Template](#create-a-new-repository-from-this-template)
   2. [Clone new Feature to your local drive](#clone-new-feature-to-your-local-drive)
   3. [Replace `Template` occurrences in `OpenStudioLandscapes-NewFeature`](#replace-template-occurrences-in-openstudiolandscapes-newfeature)
   4. [Commit your initial Setup](#commit-your-initial-setup)
   5. [Tag `OpenStudioLandscapesUtil` Repos](#tag-openstudiolandscapesutil-repos)
   6. [Enable OpenStudioLandscapes-NewFeature in the Engine](#enable-openstudiolandscapes-newfeature-in-the-engine)
   7. [Known Issues](#known-issues)
      1. [`no command specified`](#no-command-specified)

***

This `README.md` was dynamically created with [OpenStudioLandscapesUtil-ReadmeGenerator](https://github.com/michimussato/OpenStudioLandscapesUtil-ReadmeGenerator).

***

# Feature: OpenStudioLandscapes-Template

## Brief

This is an extension to the OpenStudioLandscapes ecosystem. The full documentation of OpenStudioLandscapes is available [here](https://github.com/michimussato/OpenStudioLandscapes).

You feel like writing your own Feature? Go and check out the [OpenStudioLandscapes-Template](https://github.com/michimussato/OpenStudioLandscapes-Template).

## Requirements

- `python-3.11`
- `OpenStudioLandscapes`

## Install

### This Feature

Clone this repository into `OpenStudioLandscapes/.features`:

```shell
# cd .features
git clone https://github.com/michimussato/OpenStudioLandscapes-Template.git
```

Create `venv`:

```shell
# cd .features/OpenStudioLandscapes-Template
python3.11 -m venv .venv
source .venv/bin/activate
python -m pip install --upgrade pip setuptools
```

Configure `venv`:

```shell
# cd .features/OpenStudioLandscapes-Template
pip install -e "../../[dev]"
pip install -e ".[dev]"
```

For more info see [VCS Support of pip](https://pip.pypa.io/en/stable/topics/vcs-support/).

## Add to OpenStudioLandscapes

Add the following code to `OpenStudioLandscapes.engine.features.FEATURES`:

```python
FEATURES.update(
    "OpenStudioLandscapes-Template": {
        "enabled": True|False,
        # - from ENVIRONMENT VARIABLE (.env):
        #   "enabled": get_bool_env("ENV_VAR")
        # - combined:
        #   "enabled": True|False or get_bool_env(
        #       "OPENSTUDIOLANDSCAPES__ENABLE_FEATURE_OPENSTUDIOLANDSCAPES_TEMPLATE"
        #   )
        "module": "OpenStudioLandscapes.Template.definitions",
        "compose_scope": ComposeScope.DEFAULT,
        "feature_config": OpenStudioLandscapesConfig.DEFAULT,
    }
)
```

## Testing

### pre-commit

- https://pre-commit.com
- https://pre-commit.com/hooks.html

```shell
pre-commit install
```

### nox

#### Generate Report

```shell
nox --no-error-on-missing-interpreters --report .nox/nox-report.json
```

#### Re-Generate this README

```shell
nox -v --add-timestamp --session readme
```

#### Generate Sphinx Documentation

```shell
nox -v --add-timestamp --session docs
```

#### pylint

```shell
nox -v --add-timestamp --session lint
```

##### pylint: disable=redefined-outer-name

- [`W0621`](https://pylint.pycqa.org/en/latest/user_guide/messages/warning/redefined-outer-name.html): Due to Dagsters way of piping arguments into assets.

#### SBOM

Acronym for Software Bill of Materials

```shell
nox -v --add-timestamp --session sbom
```

We create the following SBOMs:

- [`cyclonedx-bom`](https://pypi.org/project/cyclonedx-bom/)
- [`pipdeptree`](https://pypi.org/project/pipdeptree/) (Dot)
- [`pipdeptree`](https://pypi.org/project/pipdeptree/) (Mermaid)

SBOMs for the different Python interpreters defined in [`.noxfile.VERSIONS`](https://github.com/michimussato/OpenStudioLandscapes-Template/tree/main/noxfile.py) will be created in the [`.sbom`](https://github.com/michimussato/OpenStudioLandscapes-Template/tree/main/.sbom) directory of this repository.

- `cyclone-dx`
- `pipdeptree` (Dot)
- `pipdeptree` (Mermaid)

Currently, the following Python interpreters are enabled for testing:

- `python3.11`

## Variables

The following variables are being declared in `OpenStudioLandscapes.Template.constants` and are accessible throughout the [`OpenStudioLandscapes-Template`](https://github.com/michimussato/OpenStudioLandscapes-Template/tree/main/src/OpenStudioLandscapes/Template/constants.py) package.

| Variable           | Type   |
| :----------------- | :----- |
| `DOCKER_USE_CACHE` | `bool` |
| `ASSET_HEADER`     | `dict` |
| `FEATURE_CONFIGS`  | `dict` |

### Feature Configs

#### Feature Config: default

| Variable                    | Type   | Value                                                                  |
| :-------------------------- | :----- | :--------------------------------------------------------------------- |
| `DOCKER_USE_CACHE`          | `bool` | `False`                                                                |
| `HOSTNAME`                  | `str`  | `template`                                                             |
| `TELEPORT_ENTRY_POINT_HOST` | `str`  | `{{HOSTNAME}}`                                                         |
| `TELEPORT_ENTRY_POINT_PORT` | `str`  | `{{ENV_VAR_PORT_HOST}}`                                                |
| `ENV_VAR_PORT_HOST`         | `str`  | `1234`                                                                 |
| `ENV_VAR_PORT_CONTAINER`    | `str`  | `4321`                                                                 |
| `EXTRA_FILE`                | `str`  | `{DOT_FEATURES}/OpenStudioLandscapes-Template/.payload/bin/extra.file` |
| `TEMPLATE_VOLUME`           | `str`  | `{DOT_LANDSCAPES}/{LANDSCAPE}/Template__Template/data`                 |

# Community

| Feature                             | GitHub                                                                                                                                     | Discord                                                                |
| ----------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------ | ---------------------------------------------------------------------- |
| OpenStudioLandscapes                | [https://github.com/michimussato/OpenStudioLandscapes](https://github.com/michimussato/OpenStudioLandscapes)                               | [# openstudiolandscapes-general](https://discord.gg/F6bDRWsHac)        |
| OpenStudioLandscapes-Ayon           | [https://github.com/michimussato/OpenStudioLandscapes-Ayon](https://github.com/michimussato/OpenStudioLandscapes-Ayon)                     | [# openstudiolandscapes-ayon](https://discord.gg/gd6etWAF3v)           |
| OpenStudioLandscapes-Dagster        | [https://github.com/michimussato/OpenStudioLandscapes-Dagster](https://github.com/michimussato/OpenStudioLandscapes-Dagster)               | [# openstudiolandscapes-dagster](https://discord.gg/jwB3DwmKvs)        |
| OpenStudioLandscapes-Kitsu          | [https://github.com/michimussato/OpenStudioLandscapes-Kitsu](https://github.com/michimussato/OpenStudioLandscapes-Kitsu)                   | [# openstudiolandscapes-kitsu](https://discord.gg/6cc6mkReJ7)          |
| OpenStudioLandscapes-RustDeskServer | [https://github.com/michimussato/OpenStudioLandscapes-RustDeskServer](https://github.com/michimussato/OpenStudioLandscapes-RustDeskServer) | [# openstudiolandscapes-rustdeskserver](https://discord.gg/nJ8Ffd2xY3) |
| OpenStudioLandscapes-Template       | [https://github.com/michimussato/OpenStudioLandscapes-Template](https://github.com/michimussato/OpenStudioLandscapes-Template)             | [# openstudiolandscapes-template](https://discord.gg/J59GYp3Wpy)       |
| OpenStudioLandscapes-Twingate       | [https://github.com/michimussato/OpenStudioLandscapes-Twingate](https://github.com/michimussato/OpenStudioLandscapes-Twingate)             | [# openstudiolandscapes-twingate](https://discord.gg/tREYa6UNJf)       |
| OpenStudioLandscapes-VERT           | [https://github.com/michimussato/OpenStudioLandscapes-VERT](https://github.com/michimussato/OpenStudioLandscapes-VERT)                     | [# openstudiolandscapes-twingate](https://discord.gg/FYaFRUwbYr)       |

To follow up on the previous LinkedIn publications, visit:

- [OpenStudioLandscapes on LinkedIn](https://www.linkedin.com/company/106731439/).
- [Search for tag #OpenStudioLandscapes on LinkedIn](https://www.linkedin.com/search/results/all/?keywords=%23openstudiolandscapes).

***

# Create new Feature from this Template

[![Logo OpenStudioLandscapes ](https://github.com/michimussato/OpenStudioLandscapes/raw/main/media/images/logo128.png)](https://www.url.com)

## Create a new repository from this Template

Click `Use this template` and select `Create a new repository`

![Create a new repository ](media/images/use_template.png)

And fill in information as needed by specifying the `Repository name *` of the OpenStudioLandscapes Feature (i.e. `OpenStudioLandscapes-NewFeature`):

![Create a new repository ](media/images/create_repository.png)

## Clone new Feature to your local drive

Clone the new Feature into the `.features` directory of your local `OpenStudioLandscapes` clone:

```generic
cd /to/your/git/repos/OpenStudioLandscapes/.features
git clone <GIT_REPOSITORY_URL>
```

## Replace `Template` occurrences in `OpenStudioLandscapes-NewFeature`

Rename the package directory from `Template` to `NewFeature`:

```generic
NEW_FEATURE="NewFeature"

cd /to/your/git/repos/OpenStudioLandscapes/.features/OpenStudioLandscapes-${NEW_FEATURE}
mv src/OpenStudioLandscapes/Template src/OpenStudioLandscapes/${NEW_FEATURE}
```

Rename all occurrences of `template` in your new Feature with the correct name in the following files:

- update [`./pyproject.toml`](./pyproject.toml)
- update `./src/OpenStudioLandscapes/${NEW_FEATURE}/__init__.py`
- update `./src/OpenStudioLandscapes/${NEW_FEATURE}/assets.py`
- update `./src/OpenStudioLandscapes/${NEW_FEATURE}/constants.py`
- update `./src/OpenStudioLandscapes/${NEW_FEATURE}/definitions.py`
- update `./src/OpenStudioLandscapes/${NEW_FEATURE}/readme_feature.py` [`snakemd` Documentation](https://www.snakemd.io/en/latest/)
- update `/.coveragerc`
- remove media `rm ./media/images/*.*`
- remove nox reports `rm ./.nox/*.*`
- remove sbom reports `rm ./.sbom/*.*`

## Commit your initial Setup

Commit all changes to Git:

```generic
git add *
git commit -m "Initial Setup"
git push
```

## Tag `OpenStudioLandscapesUtil` Repos

- [OpenStudioLandscapesUtil-HarborCLI](https://github.com/michimussato/OpenStudioLandscapesUtil-HarborCLI?tab=readme-ov-file#tagging)
- [OpenStudioLandscapesUtil-ReadmeGenerator](https://github.com/michimussato/OpenStudioLandscapesUtil-ReadmeGenerator?tab=readme-ov-file#tagging)

## Enable OpenStudioLandscapes-NewFeature in the Engine

Commit all changes to Git:

```generic
cd /to/your/git/repos/OpenStudioLandscapes
source .venv/bin/activate
pip install --editable .features/OpenStudioLandscapes-${NEW_FEATURE}[dev]
pip install --editable .[dev]
```

Edit the `OpenStudioLandscapes.engine` to use your new Feature:

- update `OpenStudioLandscapes/.env`
- update `OpenStudioLandscapes/src/OpenStudioLandscapes/engine/features.py`
- update `OpenStudioLandscapes/README.md#current-feature-statuses`

## Known Issues

### `no command specified`

`OpenStudioLandscapes-Template` can't be launched as a Feature in a Landscape. If you do, this is the error message you will be presented with:

```shell
$ /home/michael/git/repos/OpenStudioLandscapes/.landscapes/2025-10-20-12-51-39-68351d36801042cb943f1675e611e3c0/ComposeScope_default__ComposeScope_default/ComposeScope_default__DOCKER_COMPOSE/docker_compose/docker_compose_up.sh
~/git/repos/OpenStudioLandscapes/.landscapes/2025-10-20-12-51-39-68351d36801042cb943f1675e611e3c0/ComposeScope_default__ComposeScope_default/ComposeScope_default__DOCKER_COMPOSE/docker_compose ~
Working Directory: /home/michael/git/repos/OpenStudioLandscapes/.landscapes/2025-10-20-12-51-39-68351d36801042cb943f1675e611e3c0/ComposeScope_default__ComposeScope_default/ComposeScope_default__DOCKER_COMPOSE/docker_compose
Sourcing ../../../../2025-10-20-12-51-39-68351d36801042cb943f1675e611e3c0/.overrides file...
Sourced successfully.
 Container hbbs--2025-10-20-12-51-39-68351d36801042cb943f1675e611e3c0  Creating
 Container dagster--2025-10-20-12-51-39-68351d36801042cb943f1675e611e3c0  Creating
 Container template--2025-10-20-12-51-39-68351d36801042cb943f1675e611e3c0  Creating
 Container mongo-express-10-2--2025-10-20-12-51-39-68351d36801042cb943f1675e611e3c0  Creating
 Container repository-installer-10-2--2025-10-20-12-51-39-68351d36801042cb943f1675e611e3c0  Creating
 Container ayon-server--2025-10-20-12-51-39-68351d36801042cb943f1675e611e3c0  Creating
 Container opencue-flyway--2025-10-20-12-51-39-68351d36801042cb943f1675e611e3c0  Creating
 Container kitsu--2025-10-20-12-51-39-68351d36801042cb943f1675e611e3c0  Creating
 Container template--2025-10-20-12-51-39-68351d36801042cb943f1675e611e3c0  Error response from daemon: no command specified
Error response from daemon: no command specified
~
```