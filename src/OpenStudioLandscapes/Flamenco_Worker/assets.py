import copy
import enum
import pathlib
import textwrap
from typing import Any, Generator, Dict, List, Union

import yaml
from dagster import (
    AssetExecutionContext,
    AssetIn,
    AssetKey,
    AssetMaterialization,
    MetadataValue,
    Output,
    asset, AssetsDefinition,
)
from OpenStudioLandscapes.engine.common_assets.compose import get_compose

from OpenStudioLandscapes.engine.common_assets.compose_scope import get_compose_scope_group__cmd
from OpenStudioLandscapes.engine.common_assets.docker_compose_graph import (
    get_docker_compose_graph,
)

from OpenStudioLandscapes.engine.common_assets.feature import get_feature__CONFIG
from OpenStudioLandscapes.engine.common_assets.feature_out import get_feature_out_v2
from OpenStudioLandscapes.engine.common_assets.group_in import get_feature_in, get_feature_in_parent
from OpenStudioLandscapes.engine.common_assets.group_out import get_group_out
from OpenStudioLandscapes.engine.config.models import ConfigEngine
from OpenStudioLandscapes.engine.constants import ASSET_HEADER_BASE
from OpenStudioLandscapes.engine.enums import *
from OpenStudioLandscapes.engine.link.models import OpenStudioLandscapesFeatureIn
from OpenStudioLandscapes.engine.utils import *
from OpenStudioLandscapes.engine.utils.docker.compose_dicts import *

from OpenStudioLandscapes.Flamenco_Worker import dist
from OpenStudioLandscapes.Flamenco_Worker.config.models import CONFIG_STR, Config
from OpenStudioLandscapes.Flamenco_Worker.constants import *

# Override default ConfigParent
from OpenStudioLandscapes.Flamenco.config.models import Config as ConfigParent
from OpenStudioLandscapes.Flamenco.constants import ASSET_HEADER as ASSET_HEADER_FEATURE_IN


# https://github.com/yaml/pyyaml/issues/722#issuecomment-1969292770
yaml.SafeDumper.add_multi_representer(
    data_type=enum.Enum,
    representer=yaml.representer.SafeRepresenter.represent_str,
)


compose_scope_group__cmd: AssetsDefinition = get_compose_scope_group__cmd(
    ASSET_HEADER=ASSET_HEADER,
)


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
# if ConfigParent is or type FeatureBaseModel
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

    flamenco_manager_yaml_str = textwrap.dedent(
        """\
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
        """
    ).format(
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
    padding = 3

    docker_dict = {"services": {}}

    for i in range(CONFIG.flamenco_worker_NUM_SERVICES):
        service_name = f"{service_name_base}-{str(i+1).zfill(padding)}"
        container_name, host_name = get_docker_compose_names(
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
            ports_dict = {
                "ports": []
            }
        elif "network_mode" in compose_networks:
            network_dict = {"network_mode": compose_networks["network_mode"]}

        storage = pathlib.Path(CONFIG.flamenco_worker_storage_expanded).joinpath(host_name)
        storage.mkdir(parents=True, exist_ok=True)

        volumes_dict = {
            "volumes": [
                f"{flamenco_worker_yaml.as_posix()}:/app/flamenco-worker.yaml:ro",
                f"{storage.as_posix()}:/app/flamenco-worker-files:rw",
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
            "volumes": [
                *_volume_relative,
            ],
        }

        command = [
            "/app/flamenco-worker",
            "-trace",
            # "-manager",
            # f"{env_parent['HOSTNAME']}.{env_parent['OPENSTUDIOLANDSCAPES__DOMAIN_LAN']}:{env_parent['FLAMENCO_MANAGER_PORT_HOST']}"
        ]

        # service_name = "flamenco-worker"
        # container_name = "--".join([service_name, env.get("LANDSCAPE", "default")])
        # host_name = ".".join(
        #     [env["HOSTNAME"] or service_name, env["OPENSTUDIOLANDSCAPES__DOMAIN_LAN"]]
        # )
        #
        # docker_dict = {
        #     "services": {}
        # }
        #
        # service_name_base = "flamenco-worker"
        # padding = 3
        #
        # docker_dict = {"services": {}}
        #
        # for i in range(NUM_SERVICES):
        #     service_name = f"{service_name_base}-{str(i+1).zfill(padding)}"
        #     container_name = "--".join([service_name, env.get("LANDSCAPE", "default")])
        #     host_name = ".".join([service_name, env["OPENSTUDIOLANDSCAPES__DOMAIN_LAN"]])

        # deadline_command_compose_worker_runner_10_2.extend(["-name", str(service_name)])

        service = {
            "container_name": container_name,
            # To have a unique, dynamic hostname, we simply must not
            # specify it.
            # https://forums.docker.com/t/docker-compose-set-container-name-and-hostname-dynamicaly/138259/2
            # https://shantanoo-desai.github.io/posts/technology/hostname-docker-container/
            "hostname": host_name,
            "domainname": config_engine.openstudiolandscapes__domain_lan,
            # "mac_address": ":".join(re.findall(r"..", env["HOST_ID"])),
            "restart": DockerComposePolicies.RESTART_POLICY.ALWAYS.value,
            # "image": "${DOT_OVERRIDES_REGISTRY_NAMESPACE:-docker.io/openstudiolandscapes}/%s:%s"
            # % (build["image_name"], build["image_tags"][0]),
            "image": "%s%s:%s"
            % (build["image_prefixes"], build["image_name"], build["image_tags"][0]),
            "environment": {
                "FLAMENCO_HOME": "/app/flamenco-worker-files",
                "FLAMENCO_WORKER_NAME": host_name,
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
