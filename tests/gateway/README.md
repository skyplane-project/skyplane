# Local Gateway Testing 
These scripts allow you to run the Skyplane docker container for local testing. Run the `setup_local_gateway.sh` script to create a docker network for the gateways to run in. 

## Testing with simulated object stores 
To evaluate for a simulated object store, run: 
```
python tests/gateway/test_gateway_synthetic.py --docker-image ${SKYPLANE_DOCKER_IMAGE} --restart-gateways
```
The `TestInterface` object in `test_interface.py` can be overriden to simulate object stores in different ways. 

## Testing with real object stores
To evaluate with real object stores, modify `test_gateway_obj_store.py` and run: 
```
python tests/gateway/test_gateway_obj_store.py --docker-image ${SKYPLANE_DOCKER_IMAGE} --restart-gateways
```
This will interact with actual object stores, but still run the transfer from your local machine. 