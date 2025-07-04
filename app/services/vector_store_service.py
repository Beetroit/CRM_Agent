import logging

import chromadb
from chromadb.utils import embedding_functions

logger = logging.getLogger(__name__)


class VectorStoreService:
    def __init__(self, collection_name: str = "whatsapp_messages_collection"):
        self.client = None
        self.collection = None
        self.collection_name = collection_name
        # Default embedding function (e.g., Sentence Transformers all-MiniLM-L6-v2)
        # For production, consider a more robust or cloud-based embedding service
        # self.embedding_function = (
        #     embedding_functions.GoogleGenerativeAiEmbeddingFunction(
        #         model_name="all-MiniLM-L6-v2"
        #     )
        # )

    async def initialize(self):
        """Initializes the ChromaDB client and gets/creates the collection."""
        try:
            # Use a persistent client for local storage
            self.client = chromadb.PersistentClient(path="./chroma_db")
            self.collection = self.client.get_or_create_collection(
                name=self.collection_name  # , embedding_function=self.embedding_function
            )
            logger.info(
                f"ChromaDB client initialized and collection '{self.collection_name}' ready."
            )
        except Exception as e:
            logger.error(f"Failed to initialize ChromaDB: {e}")
            raise

    async def add_message(self, message_id: str, content: str, metadata: dict):
        """
        Adds a message to the ChromaDB collection.

        Args:
            message_id: Unique ID for the message (e.g., WhatsApp MessageSid).
            content: The text content of the message to be embedded.
            metadata: A dictionary of additional metadata to store with the document.
        """
        if not self.collection:
            logger.error("ChromaDB collection not initialized. Cannot add message.")
            raise ConnectionError("ChromaDB service not ready.")
        try:
            self.collection.add(
                documents=[content], metadatas=[metadata], ids=[message_id]
            )
            logger.info(f"Message '{message_id}' added to ChromaDB.")
        except Exception as e:
            logger.error(f"Failed to add message '{message_id}' to ChromaDB: {e}")
            raise

    async def query_messages(
        self, query_texts: list[str], n_results: int = 5
    ) -> list[dict]:
        """
        Queries the ChromaDB collection for similar messages.

        Args:
            query_texts: A list of text strings to query for.
            n_results: The number of similar results to return.

        Returns:
            A list of dictionaries, each representing a retrieved document with its metadata.
        """
        if not self.collection:
            logger.error("ChromaDB collection not initialized. Cannot query messages.")
            raise ConnectionError("ChromaDB service not ready.")
        try:
            results = self.collection.query(
                query_texts=query_texts,
                n_results=n_results,
                include=["documents", "metadatas", "distances"],
            )
            logger.info(
                f"Query for '{query_texts}' returned {len(results['ids'][0])} results."
            )

            # Format results for easier consumption
            formatted_results = []
            if results and results["ids"] and results["ids"][0]:
                for i in range(len(results["ids"][0])):
                    formatted_results.append(
                        {
                            "id": results["ids"][0][i],
                            "document": results["documents"][0][i],
                            "metadata": results["metadatas"][0][i],
                            "distance": results["distances"][0][i],
                        }
                    )
            return formatted_results
        except Exception as e:
            logger.error(f"Failed to query ChromaDB: {e}")
            raise

    async def close(self):
        """ChromaDB client does not have an explicit close method for PersistentClient."""
        logger.info("ChromaDB PersistentClient does not require explicit closing.")
