import xml.etree.ElementTree as ET
import io
import re
import requests
import zipfile
from urllib.parse import urlparse
from bods_client.client import BODSClient

class Siri:

    def __init__(self, xml):
        self.xml = xml
        self.dict = self.parse(self.xml)

    def parse(self, raw_xml):
        tree = ET.ElementTree(ET.fromstring(raw_xml))
        root = tree.getroot()
        return self.parse_element(root)

    def parse_element(self, element):
        data = {}

        for current in element:
            name = current.tag[29:]

            if current:
                if name in data:
                    if not isinstance(data[name], list):
                        temp = data[name]
                        data[name] = [temp]
                    data[name].append(self.parse_element(current))
                else:
                    data[name] = self.parse_element(current)

            elif current.text:
                data[name] = current.text

        return data


class SiriArchive(BODSClient):
    """Class for accessing and downloading the bulk archive of SIRI-VM data on BODS. This provides a snapshot of
    all vehicle activities which can be saved locally to create a historic view of vehicle activity"""

    def __init__(self, *args, **kwargs):
        super().__init__(kwargs)

        self.clear()

    def clear(self):
        """Delete current data in the SiriArchive Object, so it can be repopulated with updated data"""
        self._raw_zip = None
        self._raw_xml = None
        self._dict = None
        self._name = None

    @property
    def endpoint(self):
        parsed_url = urlparse(self.base_url)
        return f"{parsed_url.scheme}://{parsed_url.hostname}/avl/download/bulk_archive"

    def _parse_filename(self, headers):
        """Parse the filename of the SIRI-VM bulk archive"""
        return re.findall(r'"(.*?)\.zip"', headers["Content-Disposition"])[0]

    def get_realtime_archive_name(self):
        request = requests.head(self.endpoint)
        return self._parse_filename(request.headers)

    def is_outdated(self):
        """Check if the BODS SIRI-VM bulk file has been updated"""
        return self.name != self.get_realtime_archive_name()

    @property
    def name(self):
        return self._name

    def get_zip(self):
        """Download the bulk SIRI-VM file as a .zip"""
        if self._raw_zip is None:
            response = self._make_request(self.endpoint)
            self._name = self._parse_filename(response.headers)
            self._raw_zip = response.content

        return self._raw_zip

    def save_as_zip(self, path = None):
        """Save the bulk SIRI-VM file as a .zip"""
        zip = self.get_zip()

        if path is None:
            path = f"./{self.name}.zip"

        with open(path, "wb") as f:
            f.write(zip)

    def get_xml(self):
        """Download the bulk SIRI-VM file as a .xml"""
        if self._raw_xml is None:
            with zipfile.ZipFile(io.BytesIO(self.get_zip())) as zf:
                with zf.open("siri.xml") as f:
                    self._raw_xml = f.read()

        return self._raw_xml

    def save_as_xml(self, path = None):
        """Save the bulk SIRI-VM file as a .xml"""
        xml = self.get_xml()

        if path is None:
            path = f"./{self.name}.xml"

        with open(path, "wb") as f:
            f.write(xml)

    def get_dict(self):
        """Download the bulk SIRI-VM file as a python dictionary"""
        if self._dict is None:
            siri = Siri(self.get_xml())
            self._dict = siri.dict

        return self._dict