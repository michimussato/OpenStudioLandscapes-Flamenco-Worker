from dagster import Definitions
from OpenStudioLandscapes.engine.base.assets import group_out_base_spec

from OpenStudioLandscapes.Flamenco_Worker.definitions import assets_base


from OpenStudioLandscapes.Flamenco.assets import (
    feature_out_v2,
    build_docker_image,
    build_docker_image_spec,
)

# The visualized DAG is cleaner when using `build_docker_image_spec`
# instead of `build_docker_image.specs` - yet they should be
# equivalent

assets_external = []
assets_external.append(group_out_base_spec)
# assets_external.extend(build_docker_image.specs)
assets_external.append(build_docker_image_spec)
assets_external.extend(feature_out_v2.specs)


defs = Definitions(
    assets=[
        *assets_base,
        *assets_external,
    ],
)
