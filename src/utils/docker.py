from re import match
from yaml import safe_load, YAMLError
from os import environ

from setup.logging import logger

def load_docker_compose_file(docker_compose_file="docker-compose.yaml"):
    """
    Loads the docker-compose.yaml file.

    Args:
        docker_compose_file (str): Path to the docker-compose.yaml file (default: 'docker-compose.yaml').

    Returns:
        dict: The parsed data from the docker-compose.yaml file.
    """
    
    try:
        with open(docker_compose_file, "r") as f:
            data = safe_load(f)
    except FileNotFoundError:
        print(f"Error: docker-compose.yaml file not found at {docker_compose_file}")
        return None
    except YAMLError as e:
        print(f"Error parsing docker-compose.yaml: {e}")
        return None
    
    return data

def get_postgres_host(docker_compose_file="docker-compose.yaml"):
  """
  Parses the docker-compose.yaml file to find the service name for the 'postgres' image.
  Sets the POSTGRES_HOST environment variable if a matching service is found.

  Args:
    docker_compose_file (str): Path to the docker-compose.yaml file (default: 'docker-compose.yaml').

  Returns:
    str: The service name for the 'postgres' image (if found), otherwise None.
  """
  if environ.get("ENVIRONMENT") != "docker":
    logger.info("Skipping automatic POSTGRES_HOST setup (environment not 'docker')")
    return None

  data = load_docker_compose_file(docker_compose_file)
  if data:
    # Case-insensitive search for 'postgres' at the beginning
    postgres_regex = r"(?i)^postgres.*"
    services = data.get("services", {})
    for service_name, service_config in services.items():
        
      if match(postgres_regex, service_config.get("image", "")):
        logger.info(f"Using service name '{service_name}' for POSTGRES_HOST")
        environ["POSTGRES_HOST"] = service_name
        return service_name

  else:
    logger.warn("WARNING: No service with image 'postgres' found in docker-compose.yaml")
    return None