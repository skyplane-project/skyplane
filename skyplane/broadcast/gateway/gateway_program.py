import json
from collections import defaultdict
from typing import Optional, List


class GatewayOperator:
    def __init__(self, op_type):
        self.op_type = op_type
        self.children = []
        self.handle = None

    def add_children(self, children):
        self.children.extend(children)

    def add_child(self, child):
        self.children.append(child)

    def set_handle(self, handle: str):
        self.handle = handle

    def to_dict(self):
        if len(self.children) == 0:
            return {**self.__dict__, **{"children": []}}

        return {**self.__dict__, **{"children": [child.to_dict() for child in self.children]}}

    def to_json(self):
        return json.dumps(self.to_dict())

    def __repr__(self):
        return self.to_json()


class GatewaySend(GatewayOperator):
    def __init__(self, ip_address: str, region: str, num_connections: int = 32, compress: bool = False, encrypt: bool = False):
        super().__init__("send")
        self.ip_address = ip_address
        self.region = region  # region to send to
        self.num_connections = num_connections  # default this for now
        self.compress = compress
        self.encrypt = encrypt


class GatewayReceive(GatewayOperator):
    def __init__(self, decompress: bool = False, decrypt: bool = False, max_pending_chunks: int = 1000):
        super().__init__("receive")
        self.decompress = decompress
        self.decrypt = decrypt
        self.max_pending_chunks = max_pending_chunks


class GatewayGenData(GatewayOperator):
    def __init__(self, size_mb: int):
        super().__init__("gen_data")
        self.size_mb = size_mb


class GatewayReadObjectStore(GatewayOperator):
    def __init__(self, bucket_name: str, bucket_region: str, num_connections: int = 32):
        super().__init__("read_object_store")
        self.bucket_name = bucket_name
        self.bucket_region = bucket_region
        self.num_connections = num_connections


class GatewayWriteObjectStore(GatewayOperator):
    def __init__(self, bucket_name: str, bucket_region: str, num_connections: int = 32):
        super().__init__("write_object_store")
        self.bucket_name = bucket_name
        self.bucket_region = bucket_region
        self.num_connections = num_connections


class GatewayWriteLocal(GatewayOperator):
    def __init__(self, path: Optional[str] = None):
        super().__init__("write_local")
        self.path = path


class GatewayMuxAnd(GatewayOperator):
    def __init__(self):
        super().__init__("mux_and")


class GatewayMuxOr(GatewayOperator):
    def __init__(self):
        super().__init__("mux_or")


class GatewayProgram:

    """
    GatewayProgram is a class that defines the local topology for a gateway (i.e. how to handle chunks)
    We intend to extend GatewayProgram to eventually support operations on chunk data.
    """

    def __init__(self):
        self._plan = defaultdict(list)
        self._ops = {}

    def add_operators(self, ops: List[GatewayOperator], parent_op: Optional[GatewayOperator] = None, partition_id: Optional[str] = None):
        ops_handles = []
        for op in ops:
            ops_handles.append(self.add_operator(op, parent_op, partition_id))

        return ops_handles

    def add_operator(self, op: GatewayOperator, parent_op: Optional[GatewayOperator] = None, partition_id: Optional[str] = None):
        if not parent_op:  # root operation
            self._plan[partition_id].append(op)
        else:
            parent_op.add_child(op)
        op.set_handle(f"operator_{len(list(self._ops.keys()))}")
        self._ops[op.handle] = op
        return op.handle

    def to_dict(self):
        plan_dict = {}
        for partition_id, op_list in self._plan.items():
            plan_dict[partition_id] = []
            for op in op_list:
                plan_dict[partition_id].append(op.to_dict())
        return plan_dict

    def to_json(self):
        return json.dumps(self.to_dict())
