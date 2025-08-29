import os
from uuid import uuid4
import logging
from langchain_qdrant import QdrantVectorStore
from qdrant_client import QdrantClient
from qdrant_client.http.models import Distance, VectorParams
from langchain_openai import OpenAIEmbeddings
from langchain.schema import Document
from qdrant_client.http.models import CollectionInfo
from typing import Optional, List

class QdrantService:
    def __init__(self, logger: logging.Logger):
        """
        Initializes a Client
        """
        try:
            self.client = QdrantClient(
                url=os.getenv("QDRANT_CLUSTER_URL", "http://localhost:6333"),
                prefer_grpc=True,
                api_key=os.getenv("QDRANT_API_TOKEN", None),
                check_compatibility=True
            )
        except Exception as e:
            logger.error(f"Failed to instantiate Qdrant client: {e}")

        self.logger = logger

    def create_collection(self, collection_name: str) -> bool:
        """
        Create a Qdrant collection with the specified name.

        Args:
            collection_name (str): The name of the collection to create.
        """
        try:
            self.client.recreate_collection(
                collection_name=collection_name,
                vectors_config=VectorParams(size=3072, distance=Distance.COSINE)
            )
            self.logger.info(f"Collection '{collection_name}' created successfully.")
            return True
        except Exception as e:
            self.logger.error(f"Failed to create collection '{collection_name}': {e}")
            return False

    def get_vector_store(self, collection_name: str) -> Optional[QdrantVectorStore]:
        """
        Create a Qdrant Vector Store for the given Collection / OpenAIEmbeddings
        """
        try:
            embedding = self._get_open_ai_embedding("text-embedding-3-large")
            if not embedding:
                raise ValueError("Open AI Embedding couldn't be fetched")
            vector_store = QdrantVectorStore(
                client=self.client,
                collection_name=collection_name,
                embedding=embedding
            )
            self.vector_store = vector_store
            return vector_store
        except Exception as e:
            self.logger.error(f"Failed to get Vector Store for collection_name {collection_name}: {e}")
            return None

    def add_documents(self, vector_store: QdrantVectorStore, documents: List[Document]) -> bool:
        try:
            _ids = [str(uuid4()) for _ in range(len(documents))]
            vector_store.add_documents(
                documents = documents,
                ids=_ids
            )
            return True
        except Exception as e:
            self.logger.error(f"Failed to add documents to the Vector Store w/ Error: {e}")
            return False

    def get_collection(self, collection_name: str) -> Optional[CollectionInfo]:
        """
        Get the Qdrant Vector Store for the specified collection name.
        """
        try:
            if not self.client.get_collection(collection_name):
                self.logger.warning(f"Collection '{collection_name}' does not exist.")
                return None
            return self.client.get_collection(collection_name)
        except Exception as e:
            self.logger.error(f"Failed to get collection '{collection_name}': {e}")
            return None

    def _get_open_ai_embedding(self, model_name: str) -> Optional[OpenAIEmbeddings]:
        try:
            embedding = OpenAIEmbeddings(model=model_name)
            return embedding
        except Exception as e:
            self.logger.error(f"Failed to fetch Open AI Embedding Model {model_name} with error : {e}")
            return None

    def close(self):
        """
        Close the Qdrant client connection.
        """
        self.client.close()
        self.logger.info("Qdrant client connection closed.")
