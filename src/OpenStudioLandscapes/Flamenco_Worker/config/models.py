import pathlib
from typing import List

from dagster import get_dagster_logger
from pydantic import Field, PositiveInt, field_validator
from pydantic_core import PydanticCustomError

LOGGER = get_dagster_logger(__name__)

from OpenStudioLandscapes.engine.config.models import FeatureBaseModel

from OpenStudioLandscapes.Flamenco_Worker import constants, dist


class Config(FeatureBaseModel):
    feature_name: str = dist.name

    group_name: str = constants.ASSET_HEADER["group_name"]

    key_prefixes: List[str] = constants.ASSET_HEADER["key_prefix"]

    compose_scope: str = "worker"

    flamenco_worker_PADDING: PositiveInt = Field(
        default=3,
    )

    flamenco_worker_NUM_SERVICES: PositiveInt = Field(
        default=1,
        description="Number of workers to simulate in parallel.",
    )

    flamenco_worker_storage: pathlib.Path = Field(
        default=pathlib.Path("{DOT_LANDSCAPES}/{LANDSCAPE}/{FEATURE}/storage"),
    )

    @field_validator("flamenco_worker_NUM_SERVICES", mode="before")
    @classmethod
    def validate_flamenco_worker_NUM_SERVICES(cls, v: int) -> int:
        if v < 1:
            raise PydanticCustomError(
                "OneOrMoreError",
                "{number} must be 1 or more!",
                {"number": v},
            )
        return v

    # EXPANDABLE PATHS
    @property
    def flamenco_worker_storage_expanded(self) -> pathlib.Path:
        LOGGER.debug(f"{self.env = }")
        if self.env is None:
            raise KeyError("`env` is `None`.")
        LOGGER.debug(f"Expanding {self.flamenco_worker_storage}...")
        ret = pathlib.Path(
            self.flamenco_worker_storage.expanduser()  # pylint: disable=E1101
            .as_posix()
            .format(
                **{
                    "FEATURE": self.feature_name,
                    **self.env,
                }
            )
        )
        return ret


CONFIG_STR = Config.get_docs()
