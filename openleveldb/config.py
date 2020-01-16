import os

import dotenv


def load_envs(env_file: str = "system.env") -> None:
    """
    Load all the environment variables defined in the `env_file`.
    This is equivalent to `. env_file` in bash.

    It is possible to define all the system specific variables in the `env_file`.

    :param env_file: the file that defines the environment variables to use
    """
    if not os.path.isfile(env_file):
        raise FileNotFoundError(f"no such config file: {env_file}")
    dotenv.load_dotenv(dotenv_path=env_file, override=True)


def get_env(env_name: str) -> str:
    """
    Read an environment variable.
    Raises errors if it is not defined or empty.

    :param env_name: the name of the environment variable
    :return: the value of the environment variable
    """
    if env_name not in os.environ:
        raise NameError(f"name {env_name} is not defined")
    env_value: str = os.environ[env_name]
    if not env_value:
        raise ValueError(f"{env_name} is defined but empty")
    return env_value
