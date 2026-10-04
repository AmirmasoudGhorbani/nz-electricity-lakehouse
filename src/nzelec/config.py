"""Where everything lives. Override per environment with notebook widgets or job parameters."""
from dataclasses import dataclass


@dataclass(frozen=True)
class Config:
    catalog: str = "workspace"
    schema: str = "nz_electricity"
    volume: str = "raw"
    raw_path: str = ""          # set only for local runs, to read files from disk

    @property
    def raw(self) -> str:
        """Landing zone: files are uploaded (or copied by a job) into this volume."""
        return self.raw_path or f"/Volumes/{self.catalog}/{self.schema}/{self.volume}"

    @property
    def checkpoints(self) -> str:
        return f"{self.raw}/_checkpoints"

    def table(self, name: str) -> str:
        return f"{self.catalog}.{self.schema}.{name}"


# Reference nodes: the two points the market itself uses as benchmarks
REFERENCE_NODES = {"OTA2201": "Otahuhu (Auckland)", "BEN2201": "Benmore (South Island)"}

# A half-hour above this is treated as a price spike (NZD per MWh)
SPIKE_THRESHOLD = 500.0
