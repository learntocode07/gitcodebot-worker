import os
import yaml
import logging
from langchain.text_splitter import RecursiveCharacterTextSplitter, Language
from langchain.schema import Document
from typing import List, Optional

class LangChainSplitter:
    def __init__(self, file_name: str, metadata: dict, logger: logging.Logger):
        """
        Initializes the LangChainSplitter with a logger.

        Args:
            logger (logging.Logger): Logger instance for logging.
        """
        self.logger = logger
        self.file_name = file_name
        self.path = metadata.get('path', '')
        self.extension = metadata.get('extension', '')
        try:
            self.language = self._infer_language()
        except ValueError as e:
            self.logger.warning(f"Error inferring language for {self.file_name}: {e}")
            self.language = "PlainText"

        if self.language != "PlainText":
            self.splitter = RecursiveCharacterTextSplitter.from_language(
                chunk_size=1500,
                chunk_overlap=150,
                length_function=len,
                add_start_index=True,
                language=self.language
            )
        else:
            self.splitter = RecursiveCharacterTextSplitter(
                chunk_size=1500,
                chunk_overlap=150,
                length_function=len,
                add_start_index=True
            )

    def split(self) -> Optional[List[Document]]:
        file_path = os.path.join(os.getcwd(), self.path)
        if not os.path.exists(file_path):
            self.logger.error(f"File {file_path} does not exist.")
            return []

        # TODO: Check for File Size constraints
        # Might run into issues loading huge files [ if any ]
        # Limited by the available memory to the python service
        with open(file_path, 'r', encoding='utf-8') as file:
            content = file.read()

        try:
            self.logger.info(f"Splitting {self.file_name} with language {self.language}.")
            documents = self.splitter.create_documents(
                    texts=[content],
                    metadatas=[{
                        "name": self.file_name,
                        "path": self.path.split("sourcefiles/")[1]
                    }]
                )
            return documents
        except Exception as e:
            self.logger.error(f"Error splitting file {self.file_name}: {e}")
            return []

    def _infer_language(self) -> Language:
        with open(os.getcwd() + "/config/supported_languages.yml", 'r') as f:
            supported_languages = yaml.safe_load(f)

        for lang, extensions in supported_languages.items():
            if self.extension in extensions:
                return Language(lang)
        self.logger.info(f"Language for {self.file_name} not found in supported languages. Raising ValueError.")
        raise ValueError(f"Unsupported file extension: {self.extension}")