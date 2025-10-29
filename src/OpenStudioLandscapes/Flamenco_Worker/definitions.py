from dagster import (
    Definitions,
    load_assets_from_modules,
)

import OpenStudioLandscapes.Flamenco_Worker.assets
import OpenStudioLandscapes.Flamenco_Worker.constants

assets = load_assets_from_modules(
    modules=[OpenStudioLandscapes.Flamenco_Worker.assets],
)

constants = load_assets_from_modules(
    modules=[OpenStudioLandscapes.Flamenco_Worker.constants],
)


defs = Definitions(
    assets=[
        *assets,
        *constants,
    ],
)
