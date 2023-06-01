from siri import SiriArchive, Siri
from bods_client.client import BODSClient
from bods_client.models import BoundingBox, SIRIVMParams
import os

API_KEY = os.environ.get('BODS_API_KEY')

bods = BODSClient(api_key=API_KEY)
box = BoundingBox(min_longitude=-2.315, min_latitude=53.496, max_longitude=-2.131, max_latitude=53.4481)

siri_params = SIRIVMParams(bounding_box=box)

data = bods.get_siri_vm_data_feed(params=siri_params)

siri = Siri(data)