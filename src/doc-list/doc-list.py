################################################################################
#
# Module: doc-list.py
#
################################################################################

import azure.cosmos.documents as documents
import azure.cosmos.cosmos_client as cosmos_client
import azure.cosmos.errors as errors
import datetime
import os
import math

from collections import defaultdict

################################################################################
# Helper class
################################################################################

class IDisposable(cosmos_client.CosmosClient):
    """ A context manager to automatically close an object with a close method
    in a with statement. """

    def __init__(self, obj):
        self.obj = obj

    def __enter__(self):
        return self.obj # bound to target

    def __exit__(self, exception_type, exception_val, trace):
        # extra cleanup in here
        self.obj = None

################################################################################
################################################################################

def read_helix_workitems_for_pipeline(client, pipeline_id=None):
    runtime_collection_link = "dbs/coreclr-infra/colls/runtime-pipelines"
    helix_collection_link = "dbs/coreclr-infra/colls/helix-jobs"

    pipeline_id = "20200505.36"

    if pipeline_id is None:
        runtime_docs = list(client.ReadItems(runtime_collection_link, {'maxItemCount': 1000}))
        last_pipeline = runtime_docs[-1]

        pipeline_id = last_pipeline["id"]

    discontinued_items = list(client.QueryItems(helix_collection_link,
                                       {
                                            'query': 'SELECT * FROM root r WHERE r.RuntimePipelineId=@pipeline_id',
                                            'parameters': [
                                                {'name': '@pipeline_id', 'value': pipeline_id}
                                            ]
                                       },
                                       {'enableCrossPartitionQuery': True}))

    print()


def read_documents(client):
    print('\n1.3 - Reading all documents in a collection\n')
    collection_link = "dbs/coreclr-infra/colls/helix-workitems"

    # NOTE: Use MaxItemCount on Options to control how many documents come back per trip to the server
    #       Important to handle throttles whenever you are doing operations such as this that might
    #       result in a 429 (throttled request)
    documentlist = list(client.ReadItems(collection_link, {'maxItemCount':1000}))
    
    print('Found {0} documents'.format(documentlist.__len__()))
    
    # names = defaultdict(lambda: [])

    # for item in documentlist:
    #     names[item['Name']].append(item)

    # coreclr_names = [item for item in names.keys() if "JIT" in item]

    # for name in coreclr_names:
    #     setup_time = 0
    #     run_time = 0
    #     total_runtime = 0

    #     workitem_name = name
    #     for item in names[name]:
    #         setup_time += item["ElapsedSetupTime"]
    #         run_time += item["ElapsedRunTime"]

    #         total_runtime += (setup_time + run_time)

    #     average_runtime = run_time / len(names[name])
    #     average_setuptime = setup_time / len(names[name])

    #     average_total_runtime = average_runtime + average_setuptime
    #     average_totaled_runtime = total_runtime / len(names[name])

    #     percent_setup = int(math.floor((average_setuptime / average_total_runtime)*100))
    #     percent_run = int(math.floor((average_runtime / average_total_runtime)*100))

    #     print("[{}] average setup time: {}({}%) average runtime {}({}%) average_total_runtme {}".format(workitem_name, average_setuptime, percent_setup, average_runtime, percent_run, average_total_runtime))

def main():
    with IDisposable(cosmos_client.CosmosClient("https://coreclr-infra.documents.azure.com:443/", {'masterKey': os.environ["coreclrInfraKey"]} )) as client:
        try:
            read_documents(client)

        except errors.HTTPFailure as e:
            print('\nrun_sample has caught an error. {0}'.format(e))
        
        finally:
            print("\nrun_sample done")

################################################################################
################################################################################

if __name__ == "__main__":
    main()