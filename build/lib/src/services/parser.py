from dataclasses import dataclass, asdict, field
import sys
sys.path.append("/workspaces/mdm")

from src.helpers.data import Data
from src.helpers.dataclass import create_pipeline
@dataclass
class Config:
    source: str
    entity: str
    layer: str
  
    def extract(self):
        config = Data._read_config()
        pipeline = create_pipeline(config)

        # Access DNB HCO raw resources
        if self.source=='dnb' and self.entity=='hco' and self.layer=='raw':
            if pipeline.dnb.hco.raw and pipeline.dnb.hco.raw.resources:
                dnb_mapper={}
                for resource in pipeline.dnb.hco.raw.resources:
                    print(f"Resource ID: {resource.id}")
                    if resource.source.athena and resource.target.file:
                        dnb_mapper[resource.source.athena.query] = resource.target.file.prefix
        print(dnb_mapper)
                        

        # # Access SAP HCP raw resources
        # if pipeline.sap.hcp.raw and pipeline.sap.hcp.raw.resources:
        #     for resource in pipeline.sap.hcp.raw.resources:
        #         print(f"Resource ID: {resource.id}")
        #         if resource.source.api:
        #             print(f"S3 Prefix: {resource.source.api.s3_prefix}")