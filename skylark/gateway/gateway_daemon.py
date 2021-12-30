from skylark.gateway.gateway import Gateway
from skylark.gateway.gateway_api import GatewayMetadataServer


if __name__ == "__main__":
    gateway = Gateway()
    gateway_server = GatewayMetadataServer(gateway)
    gateway_server.run()
