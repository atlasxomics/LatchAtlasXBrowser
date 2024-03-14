import yaml
import uuid


def load_configuration():
    return yaml.safe_load(open('/root/LatchAtlasXBrowser/config.yml','r'))

def get_uuid():
    return str(uuid.uuid4())
### download file
