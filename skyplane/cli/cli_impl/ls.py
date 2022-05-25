from pathlib import Path

from skyplane.obj_store.object_store_interface import ObjectStoreInterface


def ls_local(path: Path):
    if not path.exists():
        raise FileNotFoundError(path)
    if path.is_dir():
        for child in path.iterdir():
            yield child.name
    else:
        yield path.name


def ls_objstore(obj_store: str, bucket_name: str, key_name: str):
    client = ObjectStoreInterface.create(obj_store, bucket_name)
    for obj in client.list_objects(prefix=key_name):
        yield obj.full_path()
