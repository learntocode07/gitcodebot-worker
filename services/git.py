import os
import logging
import requests
from gitingest import ingest

def parse_repo_url(repo_url: str) -> list:
    """
    Parse the repo URL into owner, name
     -> https://github.com/owner/name
    """
    repo_url = repo_url.rstrip('/')
    parts = repo_url.split('/')

    # TODO: Ensure that Repo URL
    # has More than 2 Parts after Split
    owner = parts[-2]
    name = parts[-1]
    return [owner, name]


def get_repo_info(repo_owner: str, repo_name: str, logger: logging.Logger) -> dict:
    """
    Fetch repository information from the GitHub API.

    Args:
        repo_owner (str): The owner of the repository.
        repo_name (str): The name of the repository.
        logger (logging.Logger): Logger instance for logging errors.

    Returns:
        dict: A dictionary containing repository information.
    """

    try:
        repo_url = f"https://api.github.com/repos/{repo_owner}/{repo_name}"

        headers = {
            'Accept': 'application/vnd.github+json',
            'Authorization': 'Bearer ' + os.getenv('GITHUB_API_TOKEN', ''),
            'X-GitHub-Api-Version': '2022-11-28'
        }
        response = requests.get(
            repo_url,
            headers=headers,
            timeout=10
        )
        logger.info(f"Successfully fetched repository info from {repo_url}")
        return response.json()
    except requests.RequestException as e:
        logger.error(f"Failed to fetch repository info from {repo_url}: {e}")
        return None

def ingest_repo(repo_url: str, logger: logging.Logger) -> bool:
    """
    Ingest a repository using gitingest.

    Args:
        repo_url (str): The URL of the repository to ingest.
        logger (logging.Logger): Logger instance for logging errors.

    Returns:
        bool: True if ingestion was successful, False otherwise.
    """
    try:
        owner, name = parse_repo_url(repo_url)
        logger.info(f"Starting ingestion for {repo_url}")
        summary, tree, content = ingest(repo_url)

        # Create a Directory owner/name
        directory = os.path.join(os.getcwd() + "/tmp", owner, name)
        os.makedirs(directory, exist_ok=True)

        with open(os.path.join(directory, 'summary.json'), 'w') as f:
            f.write(summary)

        with open(os.path.join(directory, 'tree.txt'), 'w') as f:
            f.write(tree)

        with open(os.path.join(directory, 'content.txt'), 'w') as f:
            f.write(content)

        logger.info(f"Successfully ingested {repo_url}")

        return True
    except Exception as e:
        logger.error(f"Failed to ingest {repo_url}: {e}")
        return False