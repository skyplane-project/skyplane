class SkyplaneException(Exception):
    pass


class MissingBucketException(SkyplaneException):
    pass


class MissingObjectException(SkyplaneException):
    pass


class InsufficientVCPUException(SkyplaneException):
    pass


class ObjectStoreException(SkyplaneException):
    pass


class TransferFailedException(Exception):
    def __init__(self, message, failed_objects=None):
        super().__init__(message)
        self.failed_objects = failed_objects

    def __str__(self):
        if self.failed_objects and len(self.failed_objects) > 0:
            failed_obj_str = (
                str(self.failed_objects)
                if len(self.failed_objects) <= 16
                else str(self.failed_objects[:16]) + f" and {len(self.failed_objects) - 16} more"
            )
            return super().__str__() + "\nFailed objects: " + failed_obj_str
