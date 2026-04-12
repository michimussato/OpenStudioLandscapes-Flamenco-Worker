from dagster import Definitions
from OpenStudioLandscapes.engine.base.assets import group_out_base

from OpenStudioLandscapes.Flamenco_Worker.definitions import assets_base


from OpenStudioLandscapes.Flamenco.assets import (
    feature_out_v2,
    build_docker_image,
)

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
