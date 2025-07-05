# langgraph_app/services/vector_store_service.py
import logging

import chromadb

from langgraph_app.config import LangGraphSettings

logger = logging.getLogger(__name__)


class VectorStoreService:
    def __init__(self, settings: LangGraphSettings):
        self.settings = settings
        self.client = None
        self.collection = None

    def initialize(self):
        try:
            self.client = chromadb.PersistentClient(path="./chroma_db")
            self.collection = self.client.get_or_create_collection(
                name="whatsapp_messages",
            )
            logger.info("Successfully initialized ChromaDB client and collection.")
        except Exception as e:
            logger.error(f"Failed to initialize ChromaDB: {e}")
            raise

    def add_message(self, message_id: str, message_body: str, metadata: dict):
        if not self.collection:
            self.initialize()

        try:
            self.collection.add(
                ids=[message_id],
                documents=[message_body],
                metadatas=[metadata],
            )
            logger.info(f"Added message {message_id} to vector store.")
        except Exception as e:
            logger.error(f"Failed to add message to vector store: {e}")

    def query_messages(self, query_text: str, n_results: int = 5) -> list:
        if not self.collection:
            self.initialize()

        try:
            results = self.collection.query(
                query_texts=[query_text],
                n_results=n_results,
            )
            logger.info(f"Queried vector store with text: '{query_text}'")
            logger.info(f"Results: {results}")
            return results.get("documents", [])
        except Exception as e:
            logger.error(f"Failed to query vector store: {e}")
            return []
