import requests
import pandas as pd
import json
from typing import Dict
from pandas.io.json import json_normalize

def get_structure_course(url: str) -> Dict[str, str]:
    """Function gets cource structure"""

    try:
        r = requests.post(url)
        r.raise_for_status()
    except requests.exceptions.HTTPError as errh:
        print('HTTPError: {}'.format(errh))
        print(r.content.decode('UTF-8'))
        raise
    
    result = json.loads(r.text)
    
    return result