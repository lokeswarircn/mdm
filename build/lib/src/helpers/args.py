from src.utils.params import Params
from src.helpers.logger import Logger
from dataclasses import dataclass

log = Logger.get_logger(__name__)

@dataclass
class Raw:
    args:str

    def get_dnb_raw_args(self):
        log.info("Reading Job Parameter")
        self.env=Params(self.args).get('env')
        self.configpath=Params(self.args).get('configpath') 
        self.rawpath=Params(self.args).get('rawpath')
        self.dnb_raw_path = f"s3://{self.rawpath}/FS_DNB"
        self._log_dnb_raw_args()
        return self.dnb_raw_path

    def _log_dnb_raw_args(self):
        log.info(f"env: {self.env}")
        log.info(f"configpath: {self.configpath}")
        log.info(f"rawpath: {self.rawpath}")
        log.info(f"dnb_raw_path: {self.dnb_raw_path}")
@dataclass
class Enriched:
    args: str

    def get_dnb_enr_args(self):
        log.info("Reading Job Parameter")
        self.env=Params(self.args).get('env',default_value='notset')
        self.configpath=Params(self.args).get('configpath',default_value='notset')
        self.tstpath=Params(self.args).get('tstpath',default_value='notset')
        self.enrpath=Params(self.args).get('enrpath',default_value='notset')

        self.dnb_tst_path = f"s3://{self.tstpath}/FS_DNB/DNB_CUST_ADDR_COMB"
        self.dnb_enr_path = f"s3://{self.enrpath}/FS_DNB"
        self.base_bucket = self.enrpath.split('/')[0]
        self.dnb_enr_prefix = "enriched/FS_DNB"
        self._log_dnb_enr_args()
        return self.dnb_tst_path, self.dnb_enr_path, self.base_bucket, self.dnb_enr_prefix

    def _log_dnb_enr_args(self):
        log.info(f"env: {self.env}")
        log.info(f"configpath: {self.configpath}")
        log.info(f"tstpath: {self.tstpath}")
        log.info(f"enrpath: {self.enrpath}")

        log.info(f"dnb_tst_path: {self.dnb_tst_path}")
        log.info(f"dnb_enr_path: {self.dnb_enr_path}")
        log.info(f"base_bucket: {self.base_bucket}")
        log.info(f"key: {self.dnb_enr_prefix}")