from dagster import (
    Definitions,
    load_assets_from_modules,
)

import OpenStudioLandscapes.Flamenco_Worker.assets
from OpenStudioLandscapes.Flamenco_Worker import *

LOGGER.info(f"Loading {dist.name} assets...")

assets_base = load_assets_from_modules(
    modules=[OpenStudioLandscapes.Flamenco_Worker.assets],
)


defs = Definitions(
    assets=[
        *assets_base,
    ],
)
