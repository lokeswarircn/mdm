from dataclasses import dataclass, asdict, field
from typing import Dict, Any, Optional
import yaml

@dataclass
class Athena:
    type: str
    database: str
    table: str

@dataclass
class S3:
    type: str
    bucket: str
    prefix: str
    format: str
    file_name: Optional[str] = field(default=None, metadata={"optional": True})

@dataclass
class Resources:
    # s3: Optional[S3] = field(default=None, metadata={"optional": True})
    source: Any = None
    target: Any = None

    def __post_init__(self):
        if not isinstance(self.source, (S3, Athena)):
            raise TypeError("source must be an instance of S3 or Athena")

@dataclass
class Raw:
    load_type: str = field(default="full", metadata={"optional": True})
    source: Athena = field(default_factory=Athena)
    target: S3 = field(default_factory=S3)

@dataclass
class Trusted:
    load_type: str = field(default="full", metadata={"optional": True})
    source: Athena = field(default_factory=Athena)
    target: S3 = field(default_factory=S3)

@dataclass
class HCO:
    raw: Raw = field(default_factory=Raw)
    trusted: Trusted = field(default_factory=Trusted)

@dataclass
class HCP:
    raw: Raw = field(default_factory=Raw)
    trusted: Trusted = field(default_factory=Trusted)

@dataclass
class Dnb:
    hco: HCO = field(default_factory=HCO)
    hcp: HCP = field(default_factory=HCP)

@dataclass
class Pipeline:
    dnb: Dnb = field(default_factory=Dnb)

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)

# Usage example
pip = Pipeline()

pip_yaml = yaml.dump(pip.to_dict(), default_flow_style=False)

print(pip_yaml)
# from dataclasses import dataclass, asdict, field
# from typing import Dict, Any
# import yaml

# @dataclass
# class Athena:
#     type: str = "athena"
#     database: str = "adl_enriched_gbl_cf_ebx"
#     table: str = "ebx_hierarchy_hierarchy_dunAndBradstreetAccounts"

# @dataclass
# class S3:
#     type: str = "file"
#     bucket: str = "adl-base-customer-mdm-etl-dev-226aog"
#     prefix: str = "/raw/FS_DNB/"
#     file_name: str = field(default=None, metadata={"optional": True})
# @dataclass
# class Resources:
#     s3: S3 = field(default=None, metadata={"optional": True})
#     source: Any
#     target: Any
#     def __post_init__(self):
#         if not isinstance(self.source, (S3, Athena, Api)):
#             raise TypeError("source must be an instance of S3 or Athena")
# @dataclass
# class Raw:
#     load_type: str = field(default="full", metadata={"optional": True})
#     source: Athena = field(default_factory=Athena)
#     target: S3 = field(default_factory=S3)
# @dataclass
# class Trusted:
#     load_type: str = field(default="full", metadata={"optional": True})
#     source: Athena = field(default_factory=Athena)
#     target: S3 = field(default_factory=S3)
    
# @dataclass
# class HCO:
#     raw: Raw = field(default_factory=Raw)
#     trusted: Trusted = field(default_factory=Trusted)
    
# @dataclass
# class HCP:
#     raw: Raw = field(default_factory=Raw)
#     trusted: Trusted = field(default_factory=Trusted)
    
# @dataclass
# class Dnb:
#     hco: HCO = field(default_factory=HCO)
#     hcp: HCP = field(default_factory=HCP)



# @dataclass
# class Pipeline:
#     dnb: Dnb = field(default_factory=Dnb)

#     def to_dict(self) -> Dict[str, Any]:
#         return asdict(self)

# # Usage example
# pip = Pipeline()

# pip_yaml = yaml.dump(pip.to_dict(), default_flow_style=False)

# print(pip_yaml)

