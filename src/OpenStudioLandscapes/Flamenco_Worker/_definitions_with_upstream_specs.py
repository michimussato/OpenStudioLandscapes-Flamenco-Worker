from dagster import Definitions
from OpenStudioLandscapes.engine.base.assets import group_out_base
from OpenStudioLandscapes.Flamenco.assets import (
    build_docker_image,
    feature_out_v2,
)

from OpenStudioLandscapes.Flamenco_Worker.definitions import assets_base

assets_external = []
assets_external.extend(group_out_base.specs)
assets_external.extend(build_docker_image.specs)
assets_external.extend(feature_out_v2.specs)


defs = Definitions(
    assets=[
        *assets_base,
        *assets_external,
    ],
)
