from typing import Any, Dict
import yaml

'''
Currently assumes the provided config file name is a YAML file.
'''
def get_config(config_filename: str) -> Dict[str, Any]:

    # TODO: how to handle if there is an exception (file not found, YAML error, etc.)?

    try:
        with open(config_filename, 'r') as f:
            try:
                config_data: Dict[str, Any] = yaml.safe_load(f)
            except yaml.YAMLError as ex:
                pass

    except FileNotFoundError:
        pass

    return config_data


# TODO: add fallbacks to config loading (?)
# similar to: openlineage.client.utils:
# https://github.com/OpenLineage/OpenLineage/blob/main/client/python/openlineage/client/utils.py

# def load_config() -> dict[str, Any]:
#     file = _find_yaml()
#     if file:
#         try:
#             with open(file) as f:
#                 config: dict[str, Any] = yaml.safe_load(f)
#                 return config
#         except Exception:  # noqa: BLE001, S110
#             # Just move to read env vars
#             pass
#     return defaultdict(dict)


# def _find_yaml() -> str | None:
#     # Check OPENLINEAGE_CONFIG env variable
#     path = os.getenv("OPENLINEAGE_CONFIG", None)
#     try:
#         if path and os.path.isfile(path) and os.access(path, os.R_OK):
#             return path
#     except Exception:  # noqa: BLE001
#         if path:
#             log.exception("Couldn't read file %s: ", path)
#         else:
#             pass  # We can get different errors depending on system

#     # Check current working directory:
#     try:
#         cwd = os.getcwd()
#         if "openlineage.yml" in os.listdir(cwd):
#             return os.path.join(cwd, "openlineage.yml")
#     except Exception:  # noqa: BLE001, S110
#         pass  # We can get different errors depending on system

#     # Check $HOME/.openlineage dir
#     try:
#         path = os.path.expanduser("~/.openlineage")
#         if "openlineage.yml" in os.listdir(path):
#             return os.path.join(path, "openlineage.yml")
#     except Exception:  # noqa: BLE001, S110
#         # We can get different errors depending on system
#         pass
#     return None
