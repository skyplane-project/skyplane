## Ray Set-up Instructions

- Make sure you update the latest docker image in the `config.yaml`
- Make sure you have the AWS key-pair as defined in the `config.yaml`

To launch the Ray Autoscaler cluster: `time ray up -y config.yaml`

To attach to the head: `ray attach config.yaml`

To run a python script `script.py`: `ray submit config.yaml script.py`

To teardown the system: `ray down -y config.yaml`
