from dagster import (
    Definitions,
    load_assets_from_modules,
)

import OpenStudioLandscapes.Flamenco_Worker.assets

assets = load_assets_from_modules(
    modules=[OpenStudioLandscapes.Flamenco_Worker.assets],
)


defs = Definitions(
    assets=[
        *assets,
    ],
)
