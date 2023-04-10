from skyplane.api.client import SkyplaneClient
from skyplane.api.pipeline import Pipeline

client = SkyplaneClient()

pipeline = client.pipeline()

pipeline.queue_copy(src="gs://skyplane-broadcast-datasets/OPT-66B/reshard-model_part-0.pt", dst="gs://test-destination-2/")

pipeline.start()
