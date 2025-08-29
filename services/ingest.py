import re
import os
import json
import logging
from gitingest import ingest
from utils.platform import chunk_read
from services.lang_chain import LangChainSplitter
from services.qdrant import QdrantService


class VectorStoreRepoIngestor:
    def __init__(self, repo_owner: str, repo_name: str, logger: logging.Logger):
        self.repo_name = repo_name
        self.repo_owner = repo_owner
        self.logger = logger

    def ingest(self) -> bool:
        self._ingest_git_repo()
        _file_dict = self._parse_repo_contents()
        if not _file_dict:
            self.logger.error("No files found to ingest.")
            return False
        self.logger.info(
            f"Parsed {_file_dict} files from the repository contents.")
        self._ingest_into_vector_store(_file_dict)
        return True

    def _ingest_into_vector_store(self, file_dict: dict) -> bool:
        """
        Split files using LangChainSplitter and return the documents.
        Upsert the Documents to Qdrant Vector Store

        Args:
            file_dict (dict): Dictionary containing file metadata.

        Returns:
            bool: True if splitting was successful, False otherwise.
        """
        try:
            vector_store = None
            qdrant = QdrantService(logger=self.logger)

            # TODO: Add Validation for Git Commits, if updated
            # For now, We will skip if it already exists
            if qdrant.get_collection(collection_name=f"{self.repo_owner}.{self.repo_name}"):
                self.logger.info(
                    f"Collection {self.repo_owner}.{self.repo_name} already exists, Skipping Ingestion")
                return True

            if (
                qdrant.create_collection(
                    collection_name=f"{self.repo_owner}.{self.repo_name}"
                )
            ):
                vector_store = qdrant.get_vector_store(
                    collection_name=f"{self.repo_owner}.{self.repo_name}"
                )

            if not vector_store:
                self.logger.error(f"Ingestion into Vector Store Failed")
                return False

            for file_name, metadata in file_dict.items():
                self.logger.info(f"Splitting file: {file_name}")
                # setting LangChain Splitter with required metadata
                splitter = LangChainSplitter(file_name, metadata, self.logger)
                documents = splitter.split()
                if not documents:
                    self.logger.warning(
                        f"No documents created for {file_name}.")
                    continue

                self.logger.info(
                    f"Created {len(documents)} documents for {file_name}.")
                vector_store.add_documents(documents)
                self.logger.info(
                    f"Successfully Added Documents to the Vector Store for {file_name}")

            return True
        except Exception as e:
            self.logger.error(f"Failed to split files: {e}")
            return False

    def _ingest_git_repo(self) -> bool:
        """
        Ingest a repository using gitingest.

        Args:
            repo_url (str): The URL of the repository to ingest.
            logger (logging.Logger): Logger instance for logging errors.

        Returns:
            bool: True if ingestion was successful, False otherwise.
        """
        try:
            repo_url = f"https://github.com/{self.repo_owner}/{self.repo_name}"
            self.logger.info(
                f"Starting ingestion for {self.repo_owner}/{self.repo_name}")
            summary, tree, content = ingest(repo_url)

            directory = os.path.join(
                os.getcwd() + "/tmp", self.repo_owner, self.repo_name)
            os.makedirs(directory, exist_ok=True)

            with open(os.path.join(directory, 'summary.txt'), 'w') as f:
                f.write(summary)

            with open(os.path.join(directory, 'tree.txt'), 'w') as f:
                f.write(tree)

            with open(os.path.join(directory, 'content.txt'), 'w') as f:
                f.write(content)

            self.logger.info(f"Successfully ingested {repo_url}")

            return True
        except Exception as e:
            self.logger.error(f"Failed to ingest {repo_url}: {e}")
            return False

    # TODO: Write Robust Tests for this Function
    def _get_file_names(self) -> list:
        """
        Get the list of file names in the repository.
        Returns:
            list: List of file names.
        """
        directory = os.path.join(os.getcwd() + "/tmp",
                                 self.repo_owner, self.repo_name)
        """
            Parse the Tree File
            Directory structure:
                └── tensorflow-tensorflow/
                    ├── README.md
                    ├── arm_compiler.BUILD
                    ├── AUTHORS
        """
        file_names = []

        # regex matches:
        #  - indent made of groups of '    ' or '│   '
        #  - then one of the tree markers '└── ' or '├── '
        #  - then the node name (with trailing '/' for dirs, or 'name -> target' for symlinks)
        line_re = re.compile(
            r'^(?P<prefix>(?:    |│   )*)(?:└── |├── )(?P<name>.+)$')

        tree_file_path = os.path.join(directory, 'tree.txt')
        if not os.path.exists(tree_file_path):
            self.logger.error(f"Tree file not found: {tree_file_path}")
            return file_names

        with open(tree_file_path, 'r') as f:
            tree_str = f.read()

        stack = []

        for line in tree_str.splitlines():
            m = line_re.match(line)
            if not m:
                continue

            prefix = m.group('prefix')
            raw_name = m.group('name')
            depth = len(prefix) // 4
            stack = stack[:depth]
            is_dir = raw_name.endswith('/')
            name = raw_name.rstrip('/')
            if ' -> ' in name:
                name = name.split(' -> ', 1)[0]
            stack.append(name)
            if not is_dir:
                file_names.append('/'.join(stack))

        return file_names

    def _parse_repo_contents(self) -> dict:
        """
        Parse the contents file provided by gitingest
        """

        file_dict = {}
        # Ensure content.txt exists
        content_file = os.path.join(
            os.getcwd() + "/tmp", self.repo_owner, self.repo_name, 'content.txt')
        if not os.path.exists(content_file):
            self.logger.error(f"Content file not found: {content_file}")
            return

        boundary_line = "="*48

        # [0] -> Upper Boundary
        # [1] -> FILE: <file_name>
        # [2] -> Lower Boundary
        file_marker = [False, False, False]
        file_content = []
        file_name = None
        for chunk in chunk_read(content_file):
            self.logger.info(f"Processing chunk of size {len(chunk)}")

            """
                Look for the following format in the content:
                ================================================
                FILE: <file_name>
                ================================================
            """
            _lines = chunk.decode('utf-8').splitlines()
            for line in _lines:
                self.logger.debug(f"Processing line: {line}")
                if line == boundary_line:
                    self.logger.debug(
                        f"Found boundary line, current state: {file_marker}")
                    if True not in file_marker:
                        file_marker[0] = True
                    elif file_marker[0] and file_marker[1] and not file_marker[2]:
                        file_marker[2] = True
                    elif file_marker[0] and file_marker[1] and file_marker[2]:
                        _file_path = os.path.join(
                            os.getcwd() + "/tmp", self.repo_owner, self.repo_name, "sourcefiles/", file_name)
                        os.makedirs(os.path.dirname(_file_path), exist_ok=True)
                        with open(os.path.join(os.getcwd() + "/tmp", self.repo_owner, self.repo_name, "sourcefiles/", file_name), 'a') as f:
                            f.write('\n'.join(file_content) + '\n')

                        file_dict[file_name] = {
                            'path': os.path.join(os.getcwd() + "/tmp", self.repo_owner, self.repo_name, "sourcefiles/", file_name),
                            'extension': os.path.splitext(file_name)[1],
                        }

                        self.logger.info(
                            f"Completed writing content for {file_name}")
                        file_marker = [False, False, False]
                        file_content = []
                        file_name = None
                    continue  # Skip boundary lines

                if file_marker[0] and not file_marker[1] and line.startswith("FILE: "):
                    self.logger.debug(
                        f"Found file marker: {line}, current state: {file_marker}")
                    file_name = line[len("FILE: "):].strip()
                    file_marker[1] = True
                    continue  # Skip marker line

                if file_marker[0] and file_marker[1] and file_marker[2]:
                    self.logger.debug(
                        f"Appending line to file content: {line}")
                    file_content.append(line)

        if file_name and file_content:
            _file_path = os.path.join(
                os.getcwd() + "/tmp", self.repo_owner, self.repo_name, "sourcefiles/", file_name)
            os.makedirs(os.path.dirname(_file_path), exist_ok=True)
            with open(os.path.join(os.getcwd() + "/tmp", self.repo_owner, self.repo_name, "sourcefiles/", file_name), 'a') as f:
                f.write('\n'.join(file_content) + '\n')

            file_dict[file_name] = {
                'path': os.path.join(os.getcwd() + "/tmp", self.repo_owner, self.repo_name, "sourcefiles/", file_name),
                'extension': os.path.splitext(file_name)[1],
            }

            self.logger.info(f"Completed writing content for {file_name}")

        self.logger.info(
            f"Parsed {len(file_dict)} files from the repository contents.")
        return file_dict
