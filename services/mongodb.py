import logging
from pymongo import MongoClient

""""
A Class for MongoDB Connection and Operations
"""
DATABASE_NAME = "gitcodebot"
DATABASE_COLLECTION = "repositories"


class MongoDBService:
    def __init__(self, uri: str, logger: logging.Logger):
        """
        Initialize the MongoDBService with connection parameters.

        Args:
            uri (str): MongoDB connection URI.
        """
        self.client = MongoClient(uri)
        self.logger = logger

    def get_repo_metadata(self, repo_url: str) -> dict:
        """
        Retrieve repository metadata from the MongoDB collection.

        Args:
            repo_url (str): The URL of the repository.

        Returns:
            dict: The metadata of the repository if found, otherwise None.
        """
        db = self.client[DATABASE_NAME]
        collection = db[DATABASE_COLLECTION]

        try:
            metadata = collection.find_one({'url': repo_url})
            if metadata:
                self.logger.info(
                    f"Retrieved metadata for repository {repo_url}.")
                return metadata
            else:
                self.logger.warning(
                    f"No metadata found for repository {repo_url}.")
                return None
        except Exception as e:
            self.logger.error(
                f"Error retrieving metadata for repository {repo_url}: {e}")
            return None

    def update_repo_metadata(self, repo_url: str, status: str, availability: bool) -> None:
        """
        Update the repository metadata in the MongoDB collection.

        Args:
            repo_url (str): The URL of the repository.
            status (str): The status to update for the repository.
        """
        db = self.client[DATABASE_NAME]
        collection = db[DATABASE_COLLECTION]

        try:
            metadata = collection.find_one({'url': repo_url})
            if metadata:
                collection.update_one(
                    {'url': repo_url},
                    {'$set': {
                        'status': status,
                        'availableToConsume': availability
                    }
                    }
                )
                self.logger.info(
                    f"Updated metadata for repository {repo_url} with status '{status}'.")
            else:
                self.logger.warning(
                    f"No existing metadata found for repository {repo_url}. Continuing.")
        except Exception as e:
            self.logger.error(
                f"Error updating metadata for repository {repo_url}: {e}")

    # DO NOT USE IT From Worker Service

    def delete_repo_metadata(self, repo_url: str) -> None:
        """
        Delete the repository metadata from the MongoDB collection.

        Args:
            repo_url (str): The URL of the repository.
        """
        db = self.client[DATABASE_NAME]
        collection = db[DATABASE_COLLECTION]

        try:
            result = collection.delete_one({'url': repo_url})
            if result.deleted_count > 0:
                self.logger.info(
                    f"Deleted metadata for repository {repo_url}.")
            else:
                self.logger.warning(
                    f"No metadata found for repository {repo_url} to delete.")
        except Exception as e:
            self.logger.error(
                f"Error deleting metadata for repository {repo_url}: {e}")

    def close(self) -> None:
        """
        Close the MongoDB connection.
        """
        self.client.close()
        self.logger.info("MongoDB connection closed.")
