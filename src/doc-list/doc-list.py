################################################################################
#
# Module: doc-list.py
#
################################################################################

import azure.cosmos.documents as documents
import azure.cosmos.cosmos_client as cosmos_client
import azure.cosmos.errors as errors
import datetime
import math
import pickle
import os

from collections import defaultdict

################################################################################
# Helper class
################################################################################

class CosmosDBGuard(cosmos_client.CosmosClient):
    def __init__(self, obj):
        self.obj = obj

    def __enter__(self):
        return self.obj

    def __exit__(self, exception_type, exception_val, trace):
        # extra cleanup in here
        self.obj = None

def get_last_date():
    data_location = os.path.join("/Users/jashoo/data/")
    pipeline_runs = pickle.load(open(os.path.join(data_location, "pipelines.txt"), "rb" ))

    pipeline_runs.sort(key=lambda item: item["DateStart"])
    last_run = pipeline_runs[-1]

    return last_run["DateStart"]

################################################################################
################################################################################

def get_all_pipelines_run_for_user(user, pr_title):
    data_location = os.path.join("/Users/jashoo/data/")

    # jobs = pickle.load(open(os.path.join(data_location, "jobs.txt"), "rb" ))
    pipeline_runs = pickle.load(open(os.path.join(data_location, "pipelines.txt"), "rb" ))
    # helix_submissions = pickle.load(open(os.path.join(data_location, "helix_submissions.txt"), "rb" ))
    # helix_workitems = pickle.load(open(os.path.join(data_location, "helix-workitems.txt"), "rb" ))

    pipeline_runs.sort(key=lambda item: item["DateStart"])

    pipelines_run_by_user = []
    
    for item in pipeline_runs:
        if item["BuildReasonString"].lower() == "manual":
            i = 0
        
        if item["PrSenderName"] == user and item["PrTitle"] == pr_title:
            pipelines_run_by_user.append(item)
    
    jobs = pickle.load(open(os.path.join(data_location, "jobs.txt"), "rb" ))
    helix_submissions = pickle.load(open(os.path.join(data_location, "helix_submissions.txt"), "rb" ))
    helix_workitems = pickle.load(open(os.path.join(data_location, "helix-workitems.txt"), "rb" ))

    print('Found {0} helix submissions'.format(len(helix_submissions)))
    print('Found {0} helix workitems'.format(len(helix_workitems)))

    pipeline_id_to_run = defaultdict(lambda: None)
    for pipeline in pipeline_runs:
        assert pipeline["id"] not in pipeline_id_to_run
        pipeline_id_to_run[pipeline["id"]] = pipeline

    submission_by_pr_number = defaultdict(lambda: [])
    submissions_grouped_by_source = defaultdict(lambda: [])
    buckets = defaultdict(lambda: [])

    for submission in helix_submissions:
        submissions_grouped_by_source[submission["Source"]].append(submission)

        if "pull" in submission["Source"]:
            pr_number = submission["Source"].split("pull/")[1].split("/merge")[0]
            submission_by_pr_number[pr_number] = submission

    ci_work_items = submissions_grouped_by_source["ci/public/dotnet/runtime/refs/heads/master"]
    helix_queues_used = defaultdict(lambda: [])

    for item in ci_work_items:
        for queue in item["Queues"]:
            helix_queues_used[queue].append(item)

    jobs_categorized = defaultdict(lambda: defaultdict(lambda: None))
    for job in jobs:
        if jobs_categorized[job["JobGuid"]][job["PipelineId"]] != None:
            assert jobs_categorized[job["JobGuid"]][job["PipelineId"]]["ElapsedTime"] == job["ElapsedTime"]
            
        jobs_categorized[job["JobGuid"]][job["PipelineId"]] = job

    workitems_for_job = defaultdict(lambda: [])
    job_for_pipeline = defaultdict(lambda: [])

    job_id_to_job = defaultdict(lambda: None)
    for job in jobs:
        assert job["id"] not in job_id_to_job
        job_id_to_job[job["id"]] = job

    not_found_list = []

    workitem_map = defaultdict(lambda: None)
    for workitem in helix_workitems:
        possible_jobs = jobs_categorized[workitem["JobId"]]

        found = False
        job = jobs_categorized[workitem["JobId"]][workitem["RuntimePipelineId"]]

        if job != None:
            found = True
            assert workitem["id"] not in workitem_map
            assert workitem["RuntimePipelineId"] in pipeline_id_to_run
            workitem_map[workitem["id"]] = (workitem, job, pipeline_id_to_run[workitem["RuntimePipelineId"]])

            workitems_for_job[job["id"]].append(workitem)

        if not found:
            not_found_list.append(workitem)

    for job_id in workitems_for_job:
        job = job_id_to_job[job_id]
        job_for_pipeline[job["PipelineId"]].append(job)

    sorted_pipelines = [item for item in job_for_pipeline]
    sorted_pipelines.sort()

    for pipeline in pipelines_run_by_user:
        pipeline_id = pipeline["id"]
    
        # get a like pipeline
        index = 0
        for index, item in enumerate(pipeline_runs):
            if item["id"] == pipeline_id:
                break

        compare_pipeline = None

        index -= 1
        while index != -1:
            current_pipeline = pipeline_runs[index]
            if pipeline["BuildReasonString"] == current_pipeline["BuildReasonString"]:
                if pipeline["IsCoreclrRun"] == current_pipeline["IsCoreclrRun"] and pipeline["IsLibrariesRun"] == current_pipeline["IsLibrariesRun"] and pipeline["IsMonoRun"] == current_pipeline["IsMonoRun"]:
                    compare_run = current_pipeline
                    break
            
            index -= 1

        assert compare_run is not None

        compare_pipeline_id = compare_run["id"]
        
        base_jobs = [item for item in job_for_pipeline[pipeline_id]]
        diff_jobs = [item for item in job_for_pipeline[compare_pipeline_id]]

        categorized_workitems = defaultdict(lambda: defaultdict(lambda: []))

        base_workitems = []

        for job in base_jobs:
            assert job["Name"] not in categorized_workitems["base"]
            for workitem in workitems_for_job[job["id"]]:
                categorized_workitems["base"][job["Name"]].append(workitem)

        diff_workitems = []
        for job in diff_jobs:
            assert job["Name"] not in categorized_workitems["diff"]
            for workitem in workitems_for_job[job["id"]]:
                categorized_workitems["diff"][job["Name"]].append(workitem)

        total_runtime_base_test_setup_time = 0
        total_runtime_diff_test_setup_time = 0

        for job in categorized_workitems["base"]:
            if "Runtime Tests" not in job:
                continue 

            print(job)

            runtime_base_test_setup_time = 0
            runtime_diff_test_setup_time = 0

            base_workitems = categorized_workitems["base"][job]
            diff_workitems = categorized_workitems["diff"][job]

            workitems_by_name = defaultdict(lambda: defaultdict(lambda: []))

            for workitem in base_workitems:
                workitems_by_name[workitem["Name"]]["base"].append(workitem)

            for workitem in diff_workitems:
                workitems_by_name[workitem["Name"]]["diff"].append(workitem)

            for name in workitems_by_name:
                base_workitems_by_name = workitems_by_name[name]["base"]
                diff_workitems_by_name = workitems_by_name[name]["diff"]

                assert(len(base_workitems_by_name) == len(diff_workitems_by_name))

                for index in range(len(base_workitems_by_name)):
                    runtime_base_test_setup_time += base_workitems_by_name[index]["ElapsedSetupTime"]
                    runtime_diff_test_setup_time += diff_workitems_by_name[index]["ElapsedSetupTime"]
                    print("[{}] Base setup time {}, Diff setup time {}.".format(name, base_workitems_by_name[index]["ElapsedSetupTime"], diff_workitems_by_name[index]["ElapsedSetupTime"]))

            total_runtime_base_test_setup_time += runtime_base_test_setup_time
            total_runtime_diff_test_setup_time += runtime_diff_test_setup_time

            print("base setup time: {}, diff setup time: {}".format(runtime_base_test_setup_time, runtime_diff_test_setup_time))
            print()

        print("Total base setup time: {}, total diff setup time: {}".format(runtime_base_test_setup_time, runtime_diff_test_setup_time))


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


def bucket_results(client):
    helix_workitems_link = "dbs/coreclr-infra/colls/helix-workitems"
    helix_submission_link = "dbs/coreclr-infra/colls/helix-submissions"

    pipeline_link = "dbs/coreclr-infra/colls/runtime-pipelines"
    jobs_link = "dbs/coreclr-infra/colls/runtime-jobs"

    # Get only jobs from 8 may
    jobs = list(client.QueryItems(jobs_link,
                                    {
                                        'query': 'SELECT * FROM root job WHERE job.DateStart>@min_start',
                                        'parameters': [
                                            {'name': '@min_start', 'value': "2020-05-08T02:30:52.635-07:00"}
                                        ]
                                    },
                                    {'enableCrossPartitionQuery': True}))

    pipeline_runs = list(client.ReadItems(pipeline_link, {'maxItemCount':1000}))
    #jobs = list(client.ReadItems(jobs_link, {'maxItemCount':1000}))
    helix_submissions = list(client.ReadItems(helix_submission_link, {'maxItemCount':1000}))
    helix_workitems = list(client.ReadItems(helix_workitems_link, {'maxItemCount':1000}))
    
    print('Found {0} helix submissions'.format(len(helix_submissions)))
    print('Found {0} helix workitems'.format(len(helix_workitems)))

    pipeline_id_to_run = defaultdict(lambda: None)
    for pipeline in pipeline_runs:
        assert pipeline["id"] not in pipeline_id_to_run
        pipeline_id_to_run[pipeline["id"]] = pipeline

    submission_by_pr_number = defaultdict(lambda: [])
    submissions_grouped_by_source = defaultdict(lambda: [])
    buckets = defaultdict(lambda: [])

    for submission in helix_submissions:
        submissions_grouped_by_source[submission["Source"]].append(submission)

        if "pull" in submission["Source"]:
            pr_number = submission["Source"].split("pull/")[1].split("/merge")[0]
            submission_by_pr_number[pr_number] = submission

    ci_work_items = submissions_grouped_by_source["ci/public/dotnet/runtime/refs/heads/master"]
    helix_queues_used = defaultdict(lambda: [])

    for item in ci_work_items:
        for queue in item["Queues"]:
            helix_queues_used[queue].append(item)

    jobs_categorized = defaultdict(defaultdict(lambda: []))
    for job in jobs:
        jobs_categorized[job["JobGuid"]][job["PipelineId"]].append(job)

    workitems_for_job = defaultdict(lambda: [])
    job_for_pipeline = defaultdict(lambda: [])

    job_id_to_job = defaultdict(lambda: None)
    for job in jobs:
        assert job["id"] not in job_id_to_job
        job_id_to_job[job["id"]] = job

    workitem_map = defaultdict(lambda: None)
    for workitem in helix_workitems:
        possible_jobs = jobs_categorized[workitem["JobId"]]

        found = False
        for job in possible_jobs:
            if job["PipelineId"] == workitem["RuntimePipelineId"]:
                found = True
                assert workitem["id"] not in workitem_map
                assert workitem["RuntimePipelineId"] in pipeline_id_to_run
                workitem_map[workitem["id"]] = (workitem, job, pipeline_id_to_run[workitem["RuntimePipelineId"]])

                workitems_for_job[job["id"]].append(workitem)

                break
            
        assert found

    for job_id in workitems_for_job:
        job = job_id_to_job[job_id]
        job_for_pipeline[job["PipelineId"]].append(job)

    sorted_pipelines = [item for item in job_for_pipeline]
    sorted_pipelines.sort()

    # Time spent in setup by pipeline
    for pipeline in sorted_pipelines:
        total_setup_time = 0
        total_run_time = 0

        workitem_count = 0

        helix_submission_jobs = job_for_pipeline[pipeline]

        # These are only jobs with helix submissions
        for job in helix_submission_jobs:
            workitems_in_job = workitems_for_job[job["id"]]

            workitem_count += len(workitems_in_job)

            for workitem in workitems_in_job:
                total_setup_time += workitem["ElapsedSetupTime"]
                total_run_time += workitem["ElapsedRunTime"]

        total_setup_time_seconds = total_setup_time / 1000
        total_run_time_seconds = total_run_time / 1000

        print("[{}] -- Workitems ({}). Total Setup Time ({}). Total Run Time ({})".format(pipeline, workitem_count, total_setup_time_seconds, total_run_time_seconds))

    i = 0

def get_last_runtime_pipeline(client):
    pipeline_link = "dbs/coreclr-infra/colls/runtime-pipelines"
    pipeline_runs = list(client.ReadItems(pipeline_link, {'maxItemCount':1000}))

    pipeline_runs.sort(key=lambda item: item["DateStart"])

    last_pipeline = pipeline_runs[-1]

    print("Last Ingested Pipeline: {}".format(last_pipeline["id"]))
    print("------------------------------------------------------")
    print()

    for key in last_pipeline:
        print("[{}]: {}".format(key, last_pipeline[key]))

def update_pickled_data(client):
    helix_workitems_link = "dbs/coreclr-infra/colls/helix-workitems"
    helix_submission_link = "dbs/coreclr-infra/colls/helix-submissions"

    pipeline_link = "dbs/coreclr-infra/colls/runtime-pipelines"
    jobs_link = "dbs/coreclr-infra/colls/runtime-jobs"

    pipeline_runs = list(client.ReadItems(pipeline_link, {'maxItemCount':1000}))
    #jobs = list(client.ReadItems(jobs_link, {'maxItemCount':1000}))
    helix_submissions = list(client.ReadItems(helix_submission_link, {'maxItemCount':1000}))
    helix_workitems = list(client.ReadItems(helix_workitems_link, {'maxItemCount':1500}))

    # Get only jobs from 8 may
    jobs = list(client.QueryItems(jobs_link,
                                    {
                                        'query': 'SELECT * FROM root job WHERE job.DateStart>@min_start',
                                        'parameters': [
                                            {'name': '@min_start', 'value': "2020-05-08T02:30:52.635-07:00"}
                                        ]
                                    },
                                    {'enableCrossPartitionQuery': True}))
    
    data_location = os.path.join("/Users/jashoo/data/")

    with open(os.path.join(data_location, "jobs.txt"), "wb") as file_handle:
        pickle.dump(jobs, file_handle)
    
    with open(os.path.join(data_location, "pipelines.txt"), "wb") as file_handle:
        pickle.dump(pipeline_runs, file_handle)
    
    with open(os.path.join(data_location, "helix_submissions.txt"), "wb") as file_handle:
        pickle.dump(helix_submissions, file_handle)

    with open(os.path.join(data_location, "helix-workitems.txt"), "wb") as file_handle:
        pickle.dump(helix_workitems, file_handle)

def compare_run(base, diff):
    data_location = os.path.join("/Users/jashoo/data/")

    jobs = pickle.load(open(os.path.join(data_location, "jobs.txt"), "rb" ))
    pipeline_runs = pickle.load(open(os.path.join(data_location, "pipelines.txt"), "rb" ))
    helix_submissions = pickle.load(open(os.path.join(data_location, "helix_submissions.txt"), "rb" ))
    helix_workitems = pickle.load(open(os.path.join(data_location, "helix-workitems.txt"), "rb" ))
    
    print('Found {0} helix submissions'.format(len(helix_submissions)))
    print('Found {0} helix workitems'.format(len(helix_workitems)))

    pipeline_id_to_run = defaultdict(lambda: None)
    for pipeline in pipeline_runs:
        assert pipeline["id"] not in pipeline_id_to_run
        pipeline_id_to_run[pipeline["id"]] = pipeline

    submission_by_pr_number = defaultdict(lambda: [])
    submissions_grouped_by_source = defaultdict(lambda: [])
    buckets = defaultdict(lambda: [])

    for submission in helix_submissions:
        submissions_grouped_by_source[submission["Source"]].append(submission)

        if "pull" in submission["Source"]:
            pr_number = submission["Source"].split("pull/")[1].split("/merge")[0]
            submission_by_pr_number[pr_number] = submission

    ci_work_items = submissions_grouped_by_source["ci/public/dotnet/runtime/refs/heads/master"]
    helix_queues_used = defaultdict(lambda: [])

    for item in ci_work_items:
        for queue in item["Queues"]:
            helix_queues_used[queue].append(item)

    jobs_categorized = defaultdict(lambda: defaultdict(lambda: None))
    for job in jobs:
        if jobs_categorized[job["JobGuid"]][job["PipelineId"]] != None:
            assert jobs_categorized[job["JobGuid"]][job["PipelineId"]]["ElapsedTime"] == job["ElapsedTime"]
            
        jobs_categorized[job["JobGuid"]][job["PipelineId"]] = job

    workitems_for_job = defaultdict(lambda: [])
    job_for_pipeline = defaultdict(lambda: [])

    job_id_to_job = defaultdict(lambda: None)
    for job in jobs:
        assert job["id"] not in job_id_to_job
        job_id_to_job[job["id"]] = job

    not_found_list = []

    workitem_map = defaultdict(lambda: None)
    for workitem in helix_workitems:
        possible_jobs = jobs_categorized[workitem["JobId"]]

        found = False
        job = jobs_categorized[workitem["JobId"]][workitem["RuntimePipelineId"]]

        if job != None:
            found = True
            assert workitem["id"] not in workitem_map
            assert workitem["RuntimePipelineId"] in pipeline_id_to_run
            workitem_map[workitem["id"]] = (workitem, job, pipeline_id_to_run[workitem["RuntimePipelineId"]])

            workitems_for_job[job["id"]].append(workitem)

        if not found:
            not_found_list.append(workitem)

    for job_id in workitems_for_job:
        job = job_id_to_job[job_id]
        job_for_pipeline[job["PipelineId"]].append(job)

    sorted_pipelines = [item for item in job_for_pipeline]
    sorted_pipelines.sort()

    base_jobs = [item for item in job_for_pipeline[base]]
    diff_jobs = [item for item in job_for_pipeline[diff]]

    categorized_workitems = defaultdict(lambda: defaultdict(lambda: []))

    base_workitems = []
    for job in base_jobs:
        assert job["Name"] not in categorized_workitems["base"]
        for workitem in workitems_for_job[job["id"]]:
            categorized_workitems["base"][job["Name"]].append(workitem)

    diff_workitems = []
    for job in diff_jobs:
        assert job["Name"] not in categorized_workitems["diff"]
        for workitem in workitems_for_job[job["id"]]:
            categorized_workitems["diff"][job["Name"]].append(workitem)

    total_runtime_base_test_setup_time = 0
    total_runtime_diff_test_setup_time = 0

    for job in categorized_workitems["base"]:
        if "Runtime Tests" not in job:
            continue 

        print(job)

        runtime_base_test_setup_time = 0
        runtime_diff_test_setup_time = 0

        base_workitems = categorized_workitems["base"][job]
        diff_workitems = categorized_workitems["diff"][job]

        workitems_by_name = defaultdict(lambda: defaultdict(lambda: []))

        for workitem in base_workitems:
            workitems_by_name[workitem["Name"]]["base"].append(workitem)

        for workitem in diff_workitems:
            workitems_by_name[workitem["Name"]]["diff"].append(workitem)

        for name in workitems_by_name:
            base_workitems_by_name = workitems_by_name[name]["base"]
            diff_workitems_by_name = workitems_by_name[name]["diff"]

            assert(len(base_workitems_by_name) == len(diff_workitems_by_name))

            for index in range(len(base_workitems_by_name)):
                runtime_base_test_setup_time += base_workitems_by_name[index]["ElapsedSetupTime"]
                runtime_diff_test_setup_time += diff_workitems_by_name[index]["ElapsedSetupTime"]
                print("[{}] Base setup time {}, Diff setup time {}.".format(name, base_workitems_by_name[index]["ElapsedSetupTime"], diff_workitems_by_name[index]["ElapsedSetupTime"]))

        total_runtime_base_test_setup_time += runtime_base_test_setup_time
        total_runtime_diff_test_setup_time += runtime_diff_test_setup_time

        print("base setup time: {}, diff setup time: {}".format(runtime_base_test_setup_time, runtime_diff_test_setup_time))
        print()

    print("Total base setup time: {}, total diff setup time: {}".format(runtime_base_test_setup_time, runtime_diff_test_setup_time))


def bucket_from_disk():
    data_location = os.path.join("/Users/jashoo/data/")

    jobs = pickle.load(open(os.path.join(data_location, "jobs.txt"), "rb" ))
    pipeline_runs = pickle.load(open(os.path.join(data_location, "pipelines.txt"), "rb" ))
    helix_submissions = pickle.load(open(os.path.join(data_location, "helix_submissions.txt"), "rb" ))
    helix_workitems = pickle.load(open(os.path.join(data_location, "helix-workitems.txt"), "rb" ))
    
    print('Found {0} helix submissions'.format(len(helix_submissions)))
    print('Found {0} helix workitems'.format(len(helix_workitems)))

    pipeline_id_to_run = defaultdict(lambda: None)
    for pipeline in pipeline_runs:
        assert pipeline["id"] not in pipeline_id_to_run
        pipeline_id_to_run[pipeline["id"]] = pipeline

    submission_by_pr_number = defaultdict(lambda: [])
    submissions_grouped_by_source = defaultdict(lambda: [])
    buckets = defaultdict(lambda: [])

    for submission in helix_submissions:
        submissions_grouped_by_source[submission["Source"]].append(submission)

        if "pull" in submission["Source"]:
            pr_number = submission["Source"].split("pull/")[1].split("/merge")[0]
            submission_by_pr_number[pr_number] = submission

    ci_work_items = submissions_grouped_by_source["ci/public/dotnet/runtime/refs/heads/master"]
    helix_queues_used = defaultdict(lambda: [])

    for item in ci_work_items:
        for queue in item["Queues"]:
            helix_queues_used[queue].append(item)

    jobs_categorized = defaultdict(lambda: defaultdict(lambda: None))
    for job in jobs:
        if jobs_categorized[job["JobGuid"]][job["PipelineId"]] != None:
            assert jobs_categorized[job["JobGuid"]][job["PipelineId"]]["ElapsedTime"] == job["ElapsedTime"]
            
        jobs_categorized[job["JobGuid"]][job["PipelineId"]] = job

    workitems_for_job = defaultdict(lambda: [])
    job_for_pipeline = defaultdict(lambda: [])

    job_id_to_job = defaultdict(lambda: None)
    for job in jobs:
        assert job["id"] not in job_id_to_job
        job_id_to_job[job["id"]] = job

    not_found_list = []

    workitem_map = defaultdict(lambda: None)
    for workitem in helix_workitems:
        possible_jobs = jobs_categorized[workitem["JobId"]]

        found = False
        job = jobs_categorized[workitem["JobId"]][workitem["RuntimePipelineId"]]

        if job != None:
            found = True
            assert workitem["id"] not in workitem_map
            assert workitem["RuntimePipelineId"] in pipeline_id_to_run
            workitem_map[workitem["id"]] = (workitem, job, pipeline_id_to_run[workitem["RuntimePipelineId"]])

            workitems_for_job[job["id"]].append(workitem)

        if not found:
            not_found_list.append(workitem)

    for job_id in workitems_for_job:
        job = job_id_to_job[job_id]
        job_for_pipeline[job["PipelineId"]].append(job)

    sorted_pipelines = [item for item in job_for_pipeline]
    sorted_pipelines.sort()

    with open("data.md", "w") as file_handle:
        file_handle.write("# dotnet/runtime builds" + os.linesep)

        file_handle.write("## All pipelines (Sorted by date)." + os.linesep)
        file_handle.write(os.linesep)

        rows = ["Pipeline ID", "WorkItem Count", "Total Setup Time (seconds)", "Total Run Time (seconds)"]
        format_str = "|{}" * len(rows) + "|"
        hyphen_str = "|--------" * len(rows) + "|"
        file_handle.write(format_str.format(*rows) + os.linesep)
        file_handle.write(hyphen_str)

        # Time spent in setup by pipeline
        for pipeline in sorted_pipelines:
            total_setup_time = 0
            total_run_time = 0

            workitem_count = 0

            pipeline_object = pipeline_id_to_run[pipeline]
            helix_submission_jobs = job_for_pipeline[pipeline]

            # These are only jobs with helix submissions
            for job in helix_submission_jobs:
                workitems_in_job = workitems_for_job[job["id"]]

                workitem_count += len(workitems_in_job)

                for workitem in workitems_in_job:
                    total_setup_time += workitem["ElapsedSetupTime"]
                    total_run_time += workitem["ElapsedRunTime"]

            total_setup_time_seconds = total_setup_time / 1000
            total_run_time_seconds = total_run_time / 1000

            file_handle.write(os.linesep)

            if "BuildUri" in pipeline_object:
                pipeline = "[{}]({})".format(pipeline, pipeline_object["BuildUri"])

            file_handle.write(format_str.format(pipeline, workitem_count, total_setup_time_seconds, total_run_time_seconds))

        file_handle.write(os.linesep)

        all_pr = []
        manual_runs = []
        coreclr_prs = []
        library_prs = []
        installer_prs = []
        mono_prs = []
        ci_builds = []

        for item in sorted_pipelines:
            pipeline_object = pipeline_id_to_run[item]
            if pipeline_object["IsLibrariesRun"] is True:
                library_prs.append(item)
            elif pipeline_object["IsCoreclrRun"] is True:
                coreclr_prs.append(item)
            elif pipeline_object["IsMonoRun"] is True:
                mono_prs.append(item)
            elif pipeline_object["IsInstallerRun"] is True:
                installer_prs.append(item)
            elif pipeline_object["BuildReasonString"] == "PullRequest":
                # This triggered all testing.
                all_pr.append(item)
            elif pipeline_object["BuildReasonString"] == "BatchedCI":
                ci_builds.append(item)
            elif pipeline_object["BuildReasonString"] == "Manual":
                manual_runs.append(item)
            else:
                assert False

        # CI Builds
        file_handle.write("## CI Builds" + os.linesep)

        rows = ["Pipeline ID", "Passed", "WorkItem Count", "Total Run Tests", "Passed Test Count", "Failed Test Count", "Total Setup Time (seconds)", "Total Run Time (seconds)"]
        format_str = "|{}" * len(rows) + "|"
        hyphen_str = "|--------" * len(rows) + "|"
        file_handle.write(format_str.format(*rows) + os.linesep)
        file_handle.write(hyphen_str)

        # Time spent in setup by pipeline
        for pipeline in ci_builds:
            total_setup_time = 0
            total_run_time = 0
            total_tests_run = 0
            total_passed_tests = 0
            total_failed_tests = 0

            workitem_count = 0

            pipeline_object = pipeline_id_to_run[pipeline]
            helix_submission_jobs = job_for_pipeline[pipeline]

            # These are only jobs with helix submissions
            for job in helix_submission_jobs:
                workitems_in_job = workitems_for_job[job["id"]]

                workitem_count += len(workitems_in_job)

                for workitem in workitems_in_job:
                    total_setup_time += workitem["ElapsedSetupTime"]
                    total_run_time += workitem["ElapsedRunTime"]

                    if "PassedTests" not in workitem:
                        total_tests_run = "N/A"
                        total_failed_tests = "N/A"
                        total_passed_tests = "N/A"
                    
                    else:
                        total_failed_tests += workitem["FailedTests"]
                        total_passed_tests += workitem["PassedTests"]
                        total_tests_run += workitem["TotalRunTests"]

            total_setup_time_seconds = total_setup_time / 1000
            total_run_time_seconds = total_run_time / 1000

            passed = pipeline_object["BuildResult"]

            if passed == 0:
                passed = "Canceled"
            elif passed == 1:
                passed = "Failed"
            elif passed == 4:
                passed = "Passed"
            else:
                "NA"

            file_handle.write(os.linesep)

            if "BuildUri" in pipeline_object:
                pipeline = "[{}]({})".format(pipeline, pipeline_object["BuildUri"])

            file_handle.write(format_str.format(pipeline, passed, workitem_count, total_tests_run, total_passed_tests, total_failed_tests, total_setup_time_seconds, total_run_time_seconds))

        file_handle.write(os.linesep)

        # Pull Requests
        file_handle.write("# Pull Requests" + os.linesep)

        file_handle.write(os.linesep)
        file_handle.write(os.linesep)

        # CI Builds
        file_handle.write("## All Testing (Common code changed)" + os.linesep)

        rows = ["Pipeline ID", "Passed", "WorkItem Count", "Total Run Tests", "Passed Test Count", "Failed Test Count", "Total Setup Time (seconds)", "Total Run Time (seconds)"]
        format_str = "|{}" * len(rows) + "|"
        hyphen_str = "|--------" * len(rows) + "|"
        file_handle.write(format_str.format(*rows) + os.linesep)
        file_handle.write(hyphen_str)

        # Time spent in setup by pipeline
        for pipeline in all_pr:
            total_setup_time = 0
            total_run_time = 0
            total_tests_run = 0
            total_passed_tests = 0
            total_failed_tests = 0

            workitem_count = 0

            pipeline_object = pipeline_id_to_run[pipeline]
            helix_submission_jobs = job_for_pipeline[pipeline]

            # These are only jobs with helix submissions
            for job in helix_submission_jobs:
                workitems_in_job = workitems_for_job[job["id"]]

                workitem_count += len(workitems_in_job)

                for workitem in workitems_in_job:
                    total_setup_time += workitem["ElapsedSetupTime"]
                    total_run_time += workitem["ElapsedRunTime"]

                    if "PassedTests" not in workitem:
                        total_tests_run = "N/A"
                        total_failed_tests = "N/A"
                        total_passed_tests = "N/A"
                    
                    else:
                        total_failed_tests += workitem["FailedTests"]
                        total_passed_tests += workitem["PassedTests"]
                        total_tests_run += workitem["TotalRunTests"]

            total_setup_time_seconds = total_setup_time / 1000
            total_run_time_seconds = total_run_time / 1000

            passed = pipeline_object["BuildResult"]

            if passed == 0:
                passed = "Canceled"
            elif passed == 1:
                passed = "Failed"
            elif passed == 4:
                passed = "Passed"
            else:
                "NA"

            file_handle.write(os.linesep)

            if "BuildUri" in pipeline_object:
                pipeline = "[{}]({})".format(pipeline, pipeline_object["BuildUri"])

            file_handle.write(format_str.format(pipeline, passed, workitem_count, total_tests_run, total_passed_tests, total_failed_tests, total_setup_time_seconds, total_run_time_seconds))

        file_handle.write(os.linesep)

        # CoreCLR Pull Requests
        file_handle.write("## CoreCLR Pull Requests" + os.linesep)

        rows = ["Pipeline ID", "Passed", "WorkItem Count", "Total Run Tests", "Passed Test Count", "Failed Test Count", "Total Setup Time (seconds)", "Total Run Time (seconds)"]
        format_str = "|{}" * len(rows) + "|"
        hyphen_str = "|--------" * len(rows) + "|"
        file_handle.write(format_str.format(*rows) + os.linesep)
        file_handle.write(hyphen_str)

        # Time spent in setup by pipeline
        for pipeline in coreclr_prs:
            total_setup_time = 0
            total_run_time = 0
            total_tests_run = 0
            total_passed_tests = 0
            total_failed_tests = 0

            workitem_count = 0

            pipeline_object = pipeline_id_to_run[pipeline]
            helix_submission_jobs = job_for_pipeline[pipeline]

            # These are only jobs with helix submissions
            for job in helix_submission_jobs:
                workitems_in_job = workitems_for_job[job["id"]]

                workitem_count += len(workitems_in_job)

                for workitem in workitems_in_job:
                    total_setup_time += workitem["ElapsedSetupTime"]
                    total_run_time += workitem["ElapsedRunTime"]

                    if "PassedTests" not in workitem:
                        total_tests_run = "N/A"
                        total_failed_tests = "N/A"
                        total_passed_tests = "N/A"
                    
                    else:
                        total_failed_tests += workitem["FailedTests"]
                        total_passed_tests += workitem["PassedTests"]
                        total_tests_run += workitem["TotalRunTests"]

            total_setup_time_seconds = total_setup_time / 1000
            total_run_time_seconds = total_run_time / 1000

            file_handle.write(os.linesep)

            passed = pipeline_object["BuildResult"]

            if passed == 0:
                passed = "Canceled"
            elif passed == 1:
                passed = "Failed"
            elif passed == 4:
                passed = "Passed"
            else:
                "NA"

            if "BuildUri" in pipeline_object:
                pipeline = "[{}]({})".format(pipeline, pipeline_object["BuildUri"])

            file_handle.write(format_str.format(pipeline, passed, workitem_count, total_tests_run, total_passed_tests, total_failed_tests, total_setup_time_seconds, total_run_time_seconds))

        file_handle.write(os.linesep)

        # Mono Pull Requests
        file_handle.write("## Mono Pull Requests" + os.linesep)

        rows = ["Pipeline ID", "Passed", "WorkItem Count", "Total Run Tests", "Passed Test Count", "Failed Test Count", "Total Setup Time (seconds)", "Total Run Time (seconds)"]
        format_str = "|{}" * len(rows) + "|"
        hyphen_str = "|--------" * len(rows) + "|"
        file_handle.write(format_str.format(*rows) + os.linesep)
        file_handle.write(hyphen_str)

        # Time spent in setup by pipeline
        for pipeline in mono_prs:
            total_setup_time = 0
            total_run_time = 0
            total_tests_run = 0
            total_passed_tests = 0
            total_failed_tests = 0

            workitem_count = 0

            pipeline_object = pipeline_id_to_run[pipeline]
            helix_submission_jobs = job_for_pipeline[pipeline]

            # These are only jobs with helix submissions
            for job in helix_submission_jobs:
                workitems_in_job = workitems_for_job[job["id"]]

                workitem_count += len(workitems_in_job)

                for workitem in workitems_in_job:
                    total_setup_time += workitem["ElapsedSetupTime"]
                    total_run_time += workitem["ElapsedRunTime"]

                    if "PassedTests" not in workitem:
                        total_tests_run = "N/A"
                        total_failed_tests = "N/A"
                        total_passed_tests = "N/A"
                    
                    else:
                        total_failed_tests += workitem["FailedTests"]
                        total_passed_tests += workitem["PassedTests"]
                        total_tests_run += workitem["TotalRunTests"]

            total_setup_time_seconds = total_setup_time / 1000
            total_run_time_seconds = total_run_time / 1000

            file_handle.write(os.linesep)

            passed = pipeline_object["BuildResult"]

            if passed == 0:
                passed = "Canceled"
            elif passed == 1:
                passed = "Failed"
            elif passed == 4:
                passed = "Passed"
            else:
                "NA"

            if "BuildUri" in pipeline_object:
                pipeline = "[{}]({})".format(pipeline, pipeline_object["BuildUri"])

            file_handle.write(format_str.format(pipeline, passed, workitem_count, total_tests_run, total_passed_tests, total_failed_tests, total_setup_time_seconds, total_run_time_seconds))

        file_handle.write(os.linesep)

        # Library Pull Requests
        file_handle.write("## Libraries Pull Requests" + os.linesep)

        rows = ["Pipeline ID", "Passed", "WorkItem Count", "Total Run Tests", "Passed Test Count", "Failed Test Count", "Total Setup Time (seconds)", "Total Run Time (seconds)"]
        format_str = "|{}" * len(rows) + "|"
        hyphen_str = "|--------" * len(rows) + "|"
        file_handle.write(format_str.format(*rows) + os.linesep)
        file_handle.write(hyphen_str)

        # Time spent in setup by pipeline
        for pipeline in library_prs:
            total_setup_time = 0
            total_run_time = 0
            total_tests_run = 0
            total_passed_tests = 0
            total_failed_tests = 0

            workitem_count = 0

            pipeline_object = pipeline_id_to_run[pipeline]
            helix_submission_jobs = job_for_pipeline[pipeline]

            # These are only jobs with helix submissions
            for job in helix_submission_jobs:
                workitems_in_job = workitems_for_job[job["id"]]

                workitem_count += len(workitems_in_job)

                for workitem in workitems_in_job:
                    total_setup_time += workitem["ElapsedSetupTime"]
                    total_run_time += workitem["ElapsedRunTime"]

                    if "PassedTests" in workitem:
                        total_failed_tests += workitem["FailedTests"]
                        total_passed_tests += workitem["PassedTests"]
                        total_tests_run += workitem["TotalRunTests"]

            total_setup_time_seconds = total_setup_time / 1000
            total_run_time_seconds = total_run_time / 1000

            passed = pipeline_object["BuildResult"]

            if passed == 0:
                passed = "Canceled"
            elif passed == 1:
                passed = "Failed"
            elif passed == 4:
                passed = "Passed"
            else:
                "NA"

            file_handle.write(os.linesep)

            if "BuildUri" in pipeline_object:
                pipeline = "[{}]({})".format(pipeline, pipeline_object["BuildUri"])

            file_handle.write(format_str.format(pipeline, passed, workitem_count, total_tests_run, total_passed_tests, total_failed_tests, total_setup_time_seconds, total_run_time_seconds))

        file_handle.write(os.linesep)

        assert len(installer_prs) == 0

        # # Installer Pull Requests
        # file_handle.write("## Installer Pull Requests" + os.linesep)

        # rows = ["Pipeline ID", "Passed", "WorkItem Count", "Total Run Tests", "Passed Test Count", "Failed Test Count", "Total Setup Time (seconds)", "Total Run Time (seconds)"]
        # format_str = "|{}" * len(rows) + "|"
        # hyphen_str = "|--------" * len(rows) + "|"
        # file_handle.write(format_str.format(*rows) + os.linesep)
        # file_handle.write(hyphen_str)

        # # Time spent in setup by pipeline
        # for pipeline in installer_prs:
        #     total_setup_time = 0
        #     total_run_time = 0
        #     total_tests_run = 0
        #     total_passed_tests = 0
        #     total_failed_tests = 0

        #     workitem_count = 0

        #     pipeline_object = pipeline_id_to_run[pipeline]
        #     helix_submission_jobs = job_for_pipeline[pipeline]

        #     # These are only jobs with helix submissions
        #     for job in helix_submission_jobs:
        #         workitems_in_job = workitems_for_job[job["id"]]

        #         workitem_count += len(workitems_in_job)

        #         for workitem in workitems_in_job:
        #             total_setup_time += workitem["ElapsedSetupTime"]
        #             total_run_time += workitem["ElapsedRunTime"]

        #             if "PassedTests" not in workitem:
        #                 total_tests_run = "N/A"
        #                 total_failed_tests = "N/A"
        #                 total_passed_tests = "N/A"
                    
        #             else:
        #                 total_failed_tests += workitem["FailedTests"]
        #                 total_passed_tests += workitem["PassedTests"]
        #                 total_tests_run += workitem["TotalRunTests"]

        #     total_setup_time_seconds = total_setup_time / 1000
        #     total_run_time_seconds = total_run_time / 1000

        #     passed = pipeline_object["BuildResult"]

        #     if passed == 0:
        #         passed = "Canceled"
        #     elif passed == 1:
        #         passed = "Failed"
        #     elif passed == 4:
        #         passed = "Passed"
        #     else:
        #         "NA"

        #     file_handle.write(os.linesep)

        #     if "BuildUri" in pipeline_object:
        #         pipeline = "[{}]({})".format(pipeline, pipeline_object["BuildUri"])

        #     file_handle.write(format_str.format(pipeline, passed, workitem_count, total_tests_run, total_passed_tests, total_failed_tests, total_setup_time_seconds, total_run_time_seconds))

def main():
    with CosmosDBGuard(cosmos_client.CosmosClient("https://coreclr-infra.documents.azure.com:443/", {'masterKey': os.environ["coreclrInfraKey"]} )) as client:
        try:
            #get_last_runtime_pipeline(client)
            #update_pickled_data(client)
            #bucket_from_disk()
            #compare_run("20200526.78", "20200526.76")

            get_all_pipelines_run_for_user("jashook", "Submit to helix without managed pdbs")

        except errors.HTTPFailure as e:
            print('Error. {0}'.format(e))

        print()

################################################################################
################################################################################

if __name__ == "__main__":
    main()