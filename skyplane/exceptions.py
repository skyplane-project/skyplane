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
