import os
import time
import json
from dotenv import load_dotenv
from redis import Redis
from utils.logging import setup_logging
from services.git import parse_repo_url, get_repo_info, ingest_repo
from services.mongodb import MongoDBService
from services.ingest import VectorStoreRepoIngestor
load_dotenv()

# ================= REDIS CONFIG ================== #
REDIS_HOST = os.getenv("REDIS_HOST", "127.0.0.1")
REDIS_PORT = int(os.getenv("REDIS_PORT", 6379))
QUEUE_KEY = "repository-creation"

redis = Redis(
    host=REDIS_HOST,
    port=REDIS_PORT,
    decode_responses=True,
)
# ================================================ #


def main():
    logger = setup_logging()

    """
        Block to Treat this as a Worker Process
    """
    logger.info("Running Repo Processor !")
    logger.info(f"Connecting to Redis at {REDIS_HOST}:{REDIS_PORT}...")
    while True:
        item = redis.blpop(QUEUE_KEY, timeout=1)
        if item:
            print(f"Received item from queue: {item}")
            # TODO: Process the Item Here
            queue_key, repo_url = item
            logger.info(f"Processing item from queue: {repo_url}")

            # TODO: Validate Repo URL, Altough API will validate this ideally
            # [ Defense in Depth ]

            # TODO: Move to class based Process Steps

            # Step 1 : Parse Repo URL & Other Validation #
            _owner, _name = parse_repo_url(repo_url)
            logger.info(f"Repo Owner: {_owner}, Repo Name: {_name}")

            # Step 2 : Fetch Metadata from GitHub API #
            metadata = get_repo_info(_owner, _name, logger)
            logger.info(json.dumps(
                metadata, sort_keys=True, indent=4, default=str))

            mongodb_service = MongoDBService(
                uri=os.getenv("DB_URI", "mongodb://localhost:27017"),
                logger=logger
            )

            # TODO: Validate if the Metadata doesn't pass our validations
            # 1. Size Check
            # 2. Should not be Private
            # 2. ... Anything Else ?
            # Update Status Accordingly

            # Step 3 : Ingest Repository using GitIngest #
            status = "ingesting"
            availability = False
            mongodb_service.update_repo_metadata(
                repo_url, status, availability)

            ingestor = VectorStoreRepoIngestor(
                repo_owner=_owner,
                repo_name=_name,
                logger=logger
            )
            try:
                success = ingestor.ingest()
                if success:
                    status = "ingested"
                    availability = True
                else:
                    status = "failed_to_ingest"
            except Exception as e:
                logger.error(f"Failed to ingest {repo_url}: {e}")
                status = "failed"

            mongodb_service.update_repo_metadata(
                repo_url, status, availability)

            logger.info(f"Repo Ingestion Status: {status}")

            mongodb_service.close()

            logger.info(f"Completed Processing Repo Name: {item[1]}")

        time.sleep(0.1)


if __name__ == "__main__":
    main()
