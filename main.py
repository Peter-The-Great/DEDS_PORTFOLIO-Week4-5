from processing import run
from settings import Settings
import os
from dotenv import load_dotenv

if __name__ == '__main__':
    load_dotenv()
    settings = Settings(
        server=os.getenv("NAME"),
        database=os.getenv("DATABASE"),
        data_dir=os.getenv("DATA_DIR"),
        log_dir=os.getenv("LOGS_DIR"),
        username=os.getenv("USER"),
        password=os.getenv("PASSWORD")
    )

    run(settings)