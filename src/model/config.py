from dataclasses import dataclass
import os.path


@dataclass
class Config:
    root_dir: str
    monitor_db_root_dir: str
    monitor_db_base_name: str
    monitor_db_import_table: str
    monitor_db_file_info_table: str

    @property
    def monitor_db_file(self):
        return os.path.join(self.monitor_db_root_dir, self.monitor_db_base_name)

    @property
    def monitor_db_log_file(self):
        return self.monitor_db_file + ".log"

    @property
    def monitor_db_log_name(self):
        return os.path.splitext(self.monitor_db_log_file)[0]
