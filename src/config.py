class Config:
    def __init__(self):
        self.progress_db = {
            "host": "localhost",
            "port": "1234",
            "db_name": "your_database",
            "driver_class": "com.ddtek.jdbc.openedge.OpenEdgeDriver",
            "user": "your_username",
            "password": "your_password",
            "jar_file": "/path/to/openedge.jar"
        }
        
        self.sqlite_db = {
            "db_path": "analytics.db"
        }
        
        self.mirror_settings = {
            "batch_size": 1000,
            "log_file": "sync.log",
            "ignore_file": "ignored_tables.txt"
        }
