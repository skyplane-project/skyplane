from skyplane.api.client import SkyplaneClient
from skyplane.api.pipeline import Pipeline

client = SkyplaneClient()

pipeline = client.pipeline(debug=True)

# single direct transfer
# pipeline.queue_copy(src="gs://skyplane-broadcast-datasets/OPT-66B/reshard-model_part-0.pt", dst="gs://test-destination-2/")

# 2 destination transfer
# TODO: Send destination object path to the Write operation on destination gateways (rather than sending destination path in the chunk request)
pipeline.queue_copy(
    src="gs://skyplane-broadcast-datasets/OPT-66B/reshard-model_part-0.pt",
    dst=["gs://test-destination-2/OPT-66B/", "gs://skyplane-broadcast-test-southamerica-east1-a/"],
)

pipeline.start()
