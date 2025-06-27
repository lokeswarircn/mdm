from dataclasses import dataclass, field
from typing import List, Optional, Dict, Any

@dataclass
class ApiConfig:
    s3_prefix: Optional[str] = None
    type: str = "api"

@dataclass
class AthenaConfig:
    database: Optional[str] = None
    table: Optional[str] = None
    type: str = "athena"
    query: Optional[str] = None

@dataclass
class FileConfig:
    bucket: Optional[str] = None
    prefix: Optional[str] = None
    type: str = "s3"
    format: Optional[str] = "parquet"
    file_name: Optional[str] = None

@dataclass
class SourceConfig:
    api: Optional[ApiConfig] = None
    file: Optional[FileConfig] = None
    athena: Optional[AthenaConfig] = None

@dataclass
class TargetConfig:
    api: Optional[ApiConfig] = None
    athena: Optional[AthenaConfig] = None
    file: Optional[FileConfig] = None

@dataclass
class Resource:
    id: str
    source: SourceConfig
    target: TargetConfig

@dataclass
class Resources:
    resources: List[Resource] = field(default_factory=list)

@dataclass
class Stage:
    raw: Optional[Resources] = None
    trusted: Optional[Resources] = None
    enriched: Optional[Resources] = None

@dataclass
class Entity:
    hco: Stage = field(default_factory=Stage)
    hcp: Stage = field(default_factory=Stage)

@dataclass
class Pipeline:
    dnb: Entity = field(default_factory=Entity)
    sap: Entity = field(default_factory=Entity)
    sfdc: Entity = field(default_factory=Entity)

def create_pipeline(config: Dict[str, Any]) -> Pipeline:
    """Create Pipeline instance from config dictionary"""
    pipeline = Pipeline()
    
    for source in ['dnb', 'sap', 'sfdc']:
        if source not in config:
            continue
            
        for entity in ['hco', 'hcp']:
            if entity not in config[source]:
                continue
                
            for stage in ['raw', 'trusted', 'enriched']:
                stage_data = config[source][entity].get(stage)
                if not stage_data:
                    continue
                
                resources_container = Resources()
                
                if 'resources' in stage_data:
                    for resource_data in stage_data['resources']:
                        source_config = SourceConfig(
                            api=ApiConfig(**resource_data['source']['api']) 
                                if resource_data.get('source', {}).get('api') else None,
                            athena=AthenaConfig(**resource_data['source']['athena'])
                                if resource_data.get('source', {}).get('athena') else None,
                            file=FileConfig(**resource_data['source']['file'])
                                if resource_data.get('source', {}).get('file') else None
                        )
                        
                        target_config = TargetConfig(
                            api=ApiConfig(**resource_data['target']['api'])
                                if resource_data.get('target', {}).get('api') else None,
                            athena=AthenaConfig(**resource_data['target']['athena'])
                                if resource_data.get('target', {}).get('athena') else None,
                            file=FileConfig(**resource_data['target']['file'])
                                if resource_data.get('target', {}).get('file') else None
                        )
                        
                        resource = Resource(
                            id=resource_data['id'],
                            source=source_config,
                            target=target_config
                        )
                        resources_container.resources.append(resource)
                
                setattr(getattr(getattr(pipeline, source), entity), stage, resources_container)
    
    return pipeline