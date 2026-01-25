import copy
import enum
import pathlib
import shutil
import textwrap
from typing import Any, Dict, Generator, List, Union

import yaml
from dagster import (
    AssetExecutionContext,
    AssetIn,
    AssetKey,
    AssetMaterialization,
    AssetsDefinition,
    MetadataValue,
    Output,
    asset,
)
from OpenStudioLandscapes.engine.common_assets.compose import get_compose
from OpenStudioLandscapes.engine.common_assets.docker_compose_graph import (
    get_docker_compose_graph,
)
from OpenStudioLandscapes.engine.common_assets.feature import get_feature__CONFIG
from OpenStudioLandscapes.engine.common_assets.feature_out import get_feature_out_v2
from OpenStudioLandscapes.engine.common_assets.group_in import (
    get_feature_in,
    get_feature_in_parent,
)
from OpenStudioLandscapes.engine.common_assets.group_out import get_group_out
from OpenStudioLandscapes.engine.config.models import ConfigEngine
from OpenStudioLandscapes.engine.constants import ASSET_HEADER_BASE
from OpenStudioLandscapes.engine.enums import *
from OpenStudioLandscapes.engine.link.models import OpenStudioLandscapesFeatureIn
from OpenStudioLandscapes.engine.utils import *
from OpenStudioLandscapes.engine.utils.docker.compose_dicts import *

# Override default ConfigParent
from OpenStudioLandscapes.Flamenco.config.models import Config as ConfigParent
from OpenStudioLandscapes.Flamenco.constants import (
    ASSET_HEADER as ASSET_HEADER_FEATURE_IN,
)

from OpenStudioLandscapes.Flamenco_Worker import dist
from OpenStudioLandscapes.Flamenco_Worker.config.models import CONFIG_STR, Config
from OpenStudioLandscapes.Flamenco_Worker.constants import *

# https://github.com/yaml/pyyaml/issues/722#issuecomment-1969292770
yaml.SafeDumper.add_multi_representer(
    data_type=enum.Enum,
    representer=yaml.representer.SafeRepresenter.represent_str,
)


# Overridden locally
# cmd: AssetsDefinition = get_feature__cmd(
#     ASSET_HEADER=ASSET_HEADER,
# )


CONFIG: AssetsDefinition = get_feature__CONFIG(
    ASSET_HEADER=ASSET_HEADER,
    CONFIG_STR=CONFIG_STR,
    search_model_of_type=Config,
)

feature_in: AssetsDefinition = get_feature_in(
    ASSET_HEADER=ASSET_HEADER,
    ASSET_HEADER_BASE=ASSET_HEADER_BASE,
    ASSET_HEADER_FEATURE_IN=ASSET_HEADER_FEATURE_IN,
)

group_out: AssetsDefinition = get_group_out(
    ASSET_HEADER=ASSET_HEADER,
)


docker_compose_graph: AssetsDefinition = get_docker_compose_graph(
    ASSET_HEADER=ASSET_HEADER,
)


compose: AssetsDefinition = get_compose(
    ASSET_HEADER=ASSET_HEADER,
)

feature_out_v2: AssetsDefinition = get_feature_out_v2(
    ASSET_HEADER=ASSET_HEADER,
)


# Produces
# - feature_in_parent
# - CONFIG_PARENT
# if ConfigParent is of type FeatureBaseModel
feature_in_parent: Union[AssetsDefinition, None] = get_feature_in_parent(
    ASSET_HEADER=ASSET_HEADER,
    config_parent=ConfigParent,
)


@asset(
    **ASSET_HEADER,
    ins={
        "feature_in": AssetIn(
            AssetKey([*ASSET_HEADER["key_prefix"], "feature_in"]),
        ),
    },
)
def compose_networks(
    context: AssetExecutionContext,
    feature_in: OpenStudioLandscapesFeatureIn,  # pylint: disable=redefined-outer-name
) -> Generator[
    Output[Dict[str, Dict[str, Dict[str, str]]]] | AssetMaterialization, None, None
]:

    env: Dict = feature_in.openstudiolandscapes_base.env

    compose_network_mode = DockerComposePolicies.NETWORK_MODE.HOST

    docker_dict = get_network_dicts(
        context=context,
        compose_network_mode=compose_network_mode,
        env=env,
    )

    docker_yaml = yaml.dump(docker_dict)

    yield Output(docker_dict)

    yield AssetMaterialization(
        asset_key=context.asset_key,
        metadata={
            "__".join(context.asset_key.path): MetadataValue.json(docker_dict),
            "compose_network_mode": MetadataValue.text(compose_network_mode.value),
            "docker_yaml": MetadataValue.md(f"```yaml\n{docker_yaml}\n```"),
        },
    )


@asset(
    **ASSET_HEADER,
    ins={
        "CONFIG": AssetIn(
            AssetKey([*ASSET_HEADER["key_prefix"], "CONFIG"]),
        ),
        "CONFIG_PARENT": AssetIn(
            AssetKey([*ASSET_HEADER["key_prefix"], "CONFIG_PARENT"]),
        ),
    },
    description=textwrap.dedent("""
        Help on `flamenco-worker.yaml`:
        - [Variables](https://flamenco.blender.org/usage/variables/)
        - [Manager Configuration](https://flamenco.blender.org/usage/worker-configuration/)
        """),
)
def flamenco_worker_yaml(
    context: AssetExecutionContext,
    CONFIG: Config,
    CONFIG_PARENT: ConfigParent,
) -> Generator[Output[pathlib.Path] | AssetMaterialization | Any, None, None]:

    env: Dict = CONFIG.env
    # env_parent: Dict = CONFIG_PARENT.env

    config_engine: ConfigEngine = CONFIG.config_engine

    flamenco_manager_yaml_path = pathlib.Path(
        env["DOT_LANDSCAPES"],
        env.get("LANDSCAPE", "default"),
        f"{dist.name}",
        "config",
        "flamenco-worker.yaml",
    ).expanduser()

    flamenco_manager_yaml_str = textwrap.dedent("""\
        # Configuration file for Flamenco.
        # For an explanation of the fields, refer to flamenco-manager-example.yaml
        #
        # NOTE: this file will be overwritten by Flamenco Manager's web-based configuration system.
        #
        # This file was written on 2025-10-29 13:10:37 +01:00 by Flamenco 3.7

        manager_url: {manager_url}
        task_types: [blender, ffmpeg, file-management, misc]
        restart_exit_code: 47
        
        # Optional advanced option, available on Linux only:
        oom_score_adjust: 500\
        """).format(
        manager_url=f"http://flamenco-manager.{config_engine.openstudiolandscapes__domain_lan}:{CONFIG_PARENT.flamenco_manager_port_host}",
        **env,
    )

    docker_yaml = yaml.safe_load(flamenco_manager_yaml_str)

    flamenco_yaml_obj = yaml.safe_dump(docker_yaml, indent=2)

    flamenco_manager_yaml_path.parent.mkdir(parents=True, exist_ok=True)

    with open(flamenco_manager_yaml_path, "w") as fw:
        fw.write(flamenco_yaml_obj)

    yield Output(flamenco_manager_yaml_path)

    yield AssetMaterialization(
        asset_key=context.asset_key,
        metadata={
            "__".join(context.asset_key.path): MetadataValue.path(
                flamenco_manager_yaml_path
            ),
            "docker_yaml": MetadataValue.md(
                f"```yaml\n{yaml.safe_dump(docker_yaml, indent=2)}\n```"
            ),
        },
    )


@asset(
    **ASSET_HEADER,
    ins={
        "CONFIG": AssetIn(
            AssetKey([*ASSET_HEADER["key_prefix"], "CONFIG"]),
        ),
        "build": AssetIn(
            AssetKey([*ASSET_HEADER_FEATURE_IN["key_prefix"], "build_docker_image"]),
        ),
        "compose_networks": AssetIn(
            AssetKey([*ASSET_HEADER["key_prefix"], "compose_networks"]),
        ),
        "flamenco_worker_yaml": AssetIn(
            AssetKey([*ASSET_HEADER["key_prefix"], "flamenco_worker_yaml"]),
        ),
    },
)
def compose_flamenco_worker(
    context: AssetExecutionContext,
    build: Dict,  # pylint: disable=redefined-outer-name
    CONFIG: Config,  # pylint: disable=redefined-outer-name
    compose_networks: Dict,  # pylint: disable=redefined-outer-name
    flamenco_worker_yaml: pathlib.Path,  # pylint: disable=redefined-outer-name
) -> Generator[Output[Dict] | AssetMaterialization, None, None]:
    """ """

    env: Dict = CONFIG.env

    config_engine: ConfigEngine = CONFIG.config_engine

    service_name_base = "flamenco-worker"

    docker_dict = {"services": {}}

    for i in range(CONFIG.flamenco_worker_NUM_SERVICES):

        if CONFIG.flamenco_worker_NUM_SERVICES == 1:
            # Ignore incrementation
            service_name = f"{service_name_base}"

        else:
            service_name = (
                f"{service_name_base}-{str(i+1).zfill(CONFIG.flamenco_worker_PADDING)}"
            )

        container_name, _ = get_docker_compose_names(
            context=context,
            service_name=service_name,
            landscape_id=env.get("LANDSCAPE", "default"),
            domain_lan=config_engine.openstudiolandscapes__domain_lan,
        )
        # container_name = "--".join([service_name, env.get("LANDSCAPE", "default")])
        # host_name = ".".join([service_name, env["OPENSTUDIOLANDSCAPES__DOMAIN_LAN"]])

        network_dict = {}
        ports_dict = {}

        if "networks" in compose_networks:
            network_dict = {
                "networks": list(compose_networks.get("networks", {}).keys())
            }
            ports_dict = {"ports": []}
        elif "network_mode" in compose_networks:
            network_dict = {"network_mode": compose_networks["network_mode"]}

        storage_root: pathlib.Path = CONFIG.flamenco_worker_storage_expanded
        storage_root.mkdir(parents=True, exist_ok=True)

        # storage = pathlib.Path(CONFIG.flamenco_worker_storage_expanded).joinpath(
        #     service_name
        # )
        # storage.mkdir(parents=True, exist_ok=True)

        volumes_dict = {
            "volumes": [
                f"{flamenco_worker_yaml.as_posix()}:/app/flamenco-worker.yaml:ro",
                # "%s${HOSTNAME:+-S{HOSTNAME}}:/app/flamenco-worker-files:rw" % (storage_root.as_posix()),
                # "%s/${HOSTNAME}:/app/flamenco-worker-files:rw" % (storage_root.as_posix()),
            ],
        }

        # For portability, convert absolute volume paths to relative paths

        _volume_relative = []

        for v in volumes_dict["volumes"]:

            host, container = v.split(":", maxsplit=1)

            volume_dir_host_rel_path = get_relative_path_via_common_root(
                context=context,
                path_src=CONFIG.docker_compose_expanded,
                path_dst=pathlib.Path(host),
                path_common_root=pathlib.Path(env["DOT_LANDSCAPES"]),
            )

            _volume_relative.append(
                f"{volume_dir_host_rel_path.as_posix()}:{container}",
            )

        volumes_dict = {
            "volumes": list(
                {
                    # Add named volume for workers
                    # This is necessary because we cannot specify dynamic host mount
                    # points using environment variable specified inside the container
                    # (not yet at least). So, named volumes are an easy workaround
                    # to create container specific, persistent volumes.
                    # Data in here is probably not that important anyway - just
                    # work data for the worker. The results of computations will
                    # end up in the mounted bind volume.
                    "flamenco-worker-files:/app/flamenco-worker-files:rw",
                    *_volume_relative,
                    *config_engine.global_bind_volumes,
                    *CONFIG.local_bind_volumes,
                }
            ),
        }

        command = [
            "/app/flamenco-worker",
            "-trace",
            # "-manager",
            # f"{env_parent['HOSTNAME']}.{env_parent['OPENSTUDIOLANDSCAPES__DOMAIN_LAN']}:{env_parent['FLAMENCO_MANAGER_PORT_HOST']}"
        ]

        # FLAMENCO_WORKER_NAME = ${HOSTNAME:+S{HOSTNAME}-}

        service = {
            "container_name": container_name,
            # To have a unique, dynamic hostname, we simply must not
            # specify it.
            # https://forums.docker.com/t/docker-compose-set-container-name-and-hostname-dynamicaly/138259/2
            # https://shantanoo-desai.github.io/posts/technology/hostname-docker-container/
            # "hostname": host_name,
            "domainname": config_engine.openstudiolandscapes__domain_lan,
            # "mac_address": ":".join(re.findall(r"..", env["HOST_ID"])),
            "restart": DockerComposePolicies.RESTART_POLICY.ALWAYS.value,
            "image": "%s%s:%s"
            % (build["image_prefixes"], build["image_name"], build["image_tags"][0]),
            "environment": {
                # https://www.codestudy.net/blog/how-can-i-use-environment-variables-in-docker-compose/
                # https://docs.docker.com/reference/compose-file/interpolation/
                "FLAMENCO_HOME": "/app/flamenco-worker-files",
                # "FLAMENCO_WORKER_NAME": f"${HOSTNAME}-{host_name}",
                # https://stackoverflow.com/a/16296466/2207196
                "FLAMENCO_WORKER_NAME": "${HOSTNAME}${HOSTNAME:+-}%s.%s"
                % (CONFIG.compose_scope, container_name),
                **config_engine.global_environment_variables,
                **CONFIG.local_environment_variables,
            },
            **copy.deepcopy(volumes_dict),
            **copy.deepcopy(network_dict),
            **copy.deepcopy(ports_dict),
            # "environment": {
            # },
            # "healthcheck": {
            # },
            "command": command,
        }

        docker_dict["services"][service_name] = copy.deepcopy(service)

    # https://docs.docker.com/engine/storage/volumes/#use-a-volume-with-docker-compose
    docker_dict["volumes"] = {
        "flamenco-worker-files": {
            "external": False,
        },
    }

    docker_yaml = yaml.dump(docker_dict)

    yield Output(docker_dict)

    yield AssetMaterialization(
        asset_key=context.asset_key,
        metadata={
            "__".join(context.asset_key.path): MetadataValue.json(docker_dict),
            "docker_yaml": MetadataValue.md(f"```yaml\n{docker_yaml}\n```"),
        },
    )


@asset(
    **ASSET_HEADER,
    ins={
        "compose_flamenco_worker": AssetIn(
            AssetKey([*ASSET_HEADER["key_prefix"], "compose_flamenco_worker"]),
        ),
    },
)
def compose_maps(
    context: AssetExecutionContext,
    **kwargs,  # pylint: disable=redefined-outer-name
) -> Generator[Output[List[Dict]] | AssetMaterialization, None, None]:

    ret = list(kwargs.values())

    context.log.info(ret)

    yield Output(ret)

    yield AssetMaterialization(
        asset_key=context.asset_key,
        metadata={
            "__".join(context.asset_key.path): MetadataValue.json(ret),
        },
    )


@asset(
    **ASSET_HEADER,
    ins={},
)
def cmd_extend(
    context: AssetExecutionContext,
) -> Generator[Output[List[Any]] | AssetMaterialization | Any, Any, None]:

    ret = ["--detach"]

    yield Output(ret)

    yield AssetMaterialization(
        asset_key=context.asset_key,
        metadata={
            "__".join(context.asset_key.path): MetadataValue.json(ret),
        },
    )


@asset(
    **ASSET_HEADER,
    ins={
        "CONFIG": AssetIn(
            AssetKey([*ASSET_HEADER["key_prefix"], "CONFIG"]),
        ),
        "compose": AssetIn(
            AssetKey([*ASSET_HEADER["key_prefix"], "compose"]),
        ),
    },
)
def cmd_append(
    context: AssetExecutionContext,
    CONFIG: Config,  # pylint: disable=redefined-outer-name
    compose: Dict,  # pylint: disable=redefined-outer-name,
) -> Generator[Output[Dict[str, List[Any]]] | AssetMaterialization | Any, Any, None]:

    env: Dict = CONFIG.env

    ret = {"cmd": [], "exclude_from_quote": []}

    compose_services = list(compose["services"].keys())

    # Example cmd:
    # /usr/bin/docker compose --file /home/michael/git/repos/OpenStudioLandscapes/.landscapes/2025-04-08-10-45-09-df78673952cc4499a80407d91bd404f4/Deadline_10_2_Worker__Deadline_10_2_Worker/Deadline_10_2_Worker__group_out/docker_compose/docker-compose.yml --project-name 2025-04-08-10-45-09-df78673952cc4499a80407d91bd404f4-worker up --detach --remove-orphans && sudo nsenter --target $(docker inspect -f '{{ .State.Pid }}' deadline-10-2-worker-001) --uts hostname "$(hostname -f)-nice-hack"

    # cmd_docker_compose_up.extend(
    #     [
    #         # needs to be detached in order to get to do sudo
    #         "--detach",
    #     ]
    # )

    exclude_from_quote = []

    cmd_docker_compose_set_dynamic_hostnames = []

    # Transform container hostnames
    # - deadline-10-2-worker-001...nnn
    # - deadline-10-2-pulse-worker-001...nnn
    # into
    # - ${HOSTNAME}-deadline-10-2-worker-001...nnn
    # - ${HOSTNAME}-deadline-10-2-pulse-worker-001...nnn
    #
    # We do this because this worker might be running on
    # a machine which hostname we don't know at build time
    # so the machine name needs to be extracted and forwarded
    # to the Docker container.
    # Note: $HOSTNAME is not defined (at least on some OSs)
    # so we have to set it in the "up"-scripts
    for service_name in compose_services:

        target_worker = (
            "\"$($(which docker) inspect -f '{{ .State.Pid }}' %s)\""
            % ".".join([service_name, env.get("LANDSCAPE", "default")])
        )
        hostname_worker = f"${{HOSTNAME}}-{service_name}"

        exclude_from_quote.extend(
            [
                target_worker,
                hostname_worker,
            ]
        )

        cmd_docker_compose_set_dynamic_hostname_worker = [
            shutil.which("sudo"),
            "--stdin",
            shutil.which("nsenter"),
            "--target",
            target_worker,
            "--uts",
            "hostname",
            hostname_worker,
        ]

        # Reference:
        # /usr/bin/docker --config /home/michael/git/repos/OpenStudioLandscapes/.landscapes/2025-07-23-00-51-15-1afae50517c5453b95c518ee0cd8e0aa/OpenStudioLandscapes_Base__OpenStudioLandscapes_Base/OpenStudioLandscapes_Base__docker_config_json compose --progress plain --file /home/michael/git/repos/OpenStudioLandscapes/.landscapes/2025-07-23-00-51-15-1afae50517c5453b95c518ee0cd8e0aa/Deadline_10_2_Worker__Deadline_10_2_Worker/Deadline_10_2_Worker__DOCKER_COMPOSE/docker_compose/docker-compose.yml --project-name 2025-07-23-00-51-15-1afae50517c5453b95c518ee0cd8e0aa-worker up --remove-orphans --detach && /usr/bin/sudo /usr/bin/nsenter --target $(docker inspect -f '{{ .State.Pid }}' deadline-10-2-worker-001--2025-07-23-00-51-15-1afae50517c5453b95c518ee0cd8e0aa) --uts hostname $(hostname)-deadline-10-2-worker-001 && /usr/bin/sudo /usr/bin/nsenter --target $(docker inspect -f '{{ .State.Pid }}' deadline-10-2-pulse-worker-001--2025-07-23-00-51-15-1afae50517c5453b95c518ee0cd8e0aa) --uts hostname $(hostname)-deadline-10-2-pulse-worker-001 \
        #     && /usr/bin/docker --config /home/michael/git/repos/OpenStudioLandscapes/.landscapes/2025-07-23-00-51-15-1afae50517c5453b95c518ee0cd8e0aa/OpenStudioLandscapes_Base__OpenStudioLandscapes_Base/OpenStudioLandscapes_Base__docker_config_json compose --progress plain --file /home/michael/git/repos/OpenStudioLandscapes/.landscapes/2025-07-23-00-51-15-1afae50517c5453b95c518ee0cd8e0aa/Deadline_10_2_Worker__Deadline_10_2_Worker/Deadline_10_2_Worker__DOCKER_COMPOSE/docker_compose/docker-compose.yml --project-name 2025-07-23-00-51-15-1afae50517c5453b95c518ee0cd8e0aa-worker logs --follow
        # Current:
        # Pre
        # /usr/bin/docker --config /home/michael/git/repos/OpenStudioLandscapes/.landscapes/2025-07-23-00-51-15-1afae50517c5453b95c518ee0cd8e0aa/OpenStudioLandscapes_Base__OpenStudioLandscapes_Base/OpenStudioLandscapes_Base__docker_config_json compose --progress plain --file /home/michael/git/repos/OpenStudioLandscapes/.landscapes/2025-07-23-00-51-15-1afae50517c5453b95c518ee0cd8e0aa/Deadline_10_2_Worker__Deadline_10_2_Worker/Deadline_10_2_Worker__DOCKER_COMPOSE/docker_compose/docker-compose.yml --project-name 2025-07-23-00-51-15-1afae50517c5453b95c518ee0cd8e0aa-worker up --remove-orphans --detach && /usr/bin/sudo /usr/bin/nsenter --target '$(docker inspect -f '"'"'{{ .State.Pid }}'"'"' deadline-10-2-pulse-worker-001--2025-07-23-00-51-15-1afae50517c5453b95c518ee0cd8e0aa)' --uts hostname '$(hostname)-deadline-10-2-pulse-worker-001' && /usr/bin/sudo /usr/bin/nsenter --target '$(docker inspect -f '"'"'{{ .State.Pid }}'"'"' deadline-10-2-worker-001--2025-07-23-00-51-15-1afae50517c5453b95c518ee0cd8e0aa)' --uts hostname '$(hostname)-deadline-10-2-worker-001'
        # Post
        #                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                   && /usr/bin/sudo /usr/bin/nsenter --target $(docker inspect -f '{{ .State.Pid }}' deadline-10-2-pulse-worker-001--2025-07-23-00-51-15-1afae50517c5453b95c518ee0cd8e0aa) --uts hostname $(hostname)-deadline-10-2-pulse-worker-001 && /usr/bin/sudo /usr/bin/nsenter --target $(docker inspect -f '{{ .State.Pid }}' deadline-10-2-worker-001--2025-07-23-00-51-15-1afae50517c5453b95c518ee0cd8e0aa) --uts hostname $(hostname)-deadline-10-2-worker-001

        cmd_docker_compose_set_dynamic_hostnames.extend(
            [
                "&&",
                *cmd_docker_compose_set_dynamic_hostname_worker,
            ]
        )

    ret["cmd"].extend(cmd_docker_compose_set_dynamic_hostnames)
    ret["exclude_from_quote"].extend(
        [
            "$(which docker)",
            "&&",
            ";",
            *exclude_from_quote,
        ]
    )

    yield Output(ret)

    yield AssetMaterialization(
        asset_key=context.asset_key,
        metadata={
            "__".join(context.asset_key.path): MetadataValue.json(ret),
        },
    )
