################################################################################
#
# Module: create_pr.py
#
################################################################################

import json
import requests

################################################################################
################################################################################

if __name__ == "__main__":
    url = "https://api.github.com/repos/dotnet/runtime/pulls"

    data = {
        "title": "[Automated PR] master -> dev/infrastucture",
        "body": "This pr has been created automatically.",
        "head": "jashook:dev/infra-merge1",
        "base": "dev/infrastructure"
    }

    data_json = json.dumps(data)

    response = requests.post(url, data = data_json, auth = ('jashook', '304269a0813537d7810982de9f2a1a17a7c091f2'))
    parsed_response = json.loads(response.text)

    print(json.dumps(parsed_response, indent=4, sort_keys=True))