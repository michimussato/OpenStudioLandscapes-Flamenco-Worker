from dagster import (
    Definitions,
    load_assets_from_modules,
)
# from OpenStudioLandscapes.engine.features.upstream_asset_specs import assets_external

import OpenStudioLandscapes.Flamenco_Worker.assets

assets_base = load_assets_from_modules(
    modules=[OpenStudioLandscapes.Flamenco_Worker.assets],
)


defs = Definitions(
    assets=[
        *assets_base,
        # *assets_external,
    ],
)
