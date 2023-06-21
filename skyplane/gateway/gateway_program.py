from typing import Optional, List, Tuple
import json
from collections import defaultdict


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
    def __init__(
        self,
        target_gateway_id: str,
        region: str,
        num_connections: int = 32,
        compress: bool = False,
        encrypt: bool = False,
        private_ip: bool = False,
    ):
        super().__init__("send")
        self.target_gateway_id = target_gateway_id  # gateway to send to
        self.region = region  # region to send to
        self.num_connections = num_connections  # default this for now
        self.compress = compress
        self.encrypt = encrypt
        self.private_ip = private_ip  # whether to send to private or public IP (private for GCP->GCP)


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
    def __init__(self, bucket_name: str, bucket_region: str, num_connections: int = 32, key_prefix: Optional[str] = ""):
        super().__init__("write_object_store")
        self.bucket_name = bucket_name
        self.bucket_region = bucket_region
        self.num_connections = num_connections
        self.key_prefix = key_prefix


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

    ip_address = None

    def __init__(self):
        self._plan = defaultdict(list)
        self._ops = {}

    def get_operators(self) -> List[GatewayOperator]:
        return list(self._ops.values())

    def add_operators(self, ops: List[GatewayOperator], parent_handle: Optional[str] = None, partition_id: Optional[str] = "default"):
        parent_op = self._ops[parent_handle] if parent_handle else None
        ops_handles = []
        for op in ops:
            ops_handles.append(self.add_operator(op, parent_op, partition_id))

        return ops_handles

    def add_operator(self, op: GatewayOperator, parent_handle: Optional[str] = None, partition_id: Optional[str] = "default"):
        parent_op = self._ops[parent_handle] if parent_handle else None
        if not parent_op:  # root operation
            self._plan[partition_id].append(op)
        else:
            parent_op.add_child(op)
        op.set_handle(f"operator_{len(list(self._ops.keys()))}")
        self._ops[op.handle] = op
        return op.handle

    def to_dict(self):
        """
        Return dictionary representation of all partitions and gateway partitions.
        Partitions with equivalent programs are grouped together to save space.
        """
        program_all = []
        for partition_id, op_list in self._plan.items():
            # build gateway program representation
            program = []
            for op in op_list:
                program.append(op.to_dict())

            # check if any existing
            exists = False
            for p in program_all:
                if p["value"] == program:  # equivalent partition exists
                    p["partitions"].append(partition_id)
                    exists = True
                    break
            if not exists:
                program_all.append({"value": program, "partitions": [partition_id]})

        return program_all

    def to_json(self):
        return json.dumps(self.to_dict())
