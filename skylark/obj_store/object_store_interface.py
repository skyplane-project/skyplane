class ObjectStoreInterface:
    def bucket_exists(self):
        raise NotImplementedError

    def create_bucket(self):
        raise NotImplementedError

    def list_objects(self, prefix=""):
        raise NotImplementedError

    def get_obj_size(self, obj_name):
        raise NotImplementedError

    def download_object(self, src_object_name, dst_file_path):
        raise NotImplementedError

    def upload_object(self, src_file_path, dst_object_name, content_type="infer"):
        raise NotImplementedError


class NoSuchObjectException(Exception):
    pass
