################################################################################
#
# Module: complete_pipeline_run.py
#
# Notes:
#
# Finding all of the jobs and workitems for a pipeline is an expensive process.
# It will touch ~50 thousand documents in cosmosdb.
#
################################################################################

from collections import defaultdict

import math

################################################################################
# Helper methods
################################################################################

def check_if_timeout(job, step, divisor=30):
    """ Based on the amount of time run check if this is a timeout
    """

    valid_timeout_numbers = {
        90: True,
        120: True,
        150: True,
        160: True,
        180: True,
        200: True,
        270: True,
        320: True,
        390: True,
        480: True,
        510: True
    }

    job_elapsed_time = job["ElapsedTime"]
    step_elapsed_time = step["ElapsedTime"]

    job_timed_out = get_task_result(job["Result"]) == "Canceled"
    step_timed_out = get_task_result(step["Result"]) == "Canceled"

    timed_out = False

    if step_timed_out or job_timed_out:
        timed_out = True

    job_time_is_valid_timeout = math.floor(job_elapsed_time) in valid_timeout_numbers or math.ceil(job_elapsed_time) in valid_timeout_numbers
    step_time_is_valid_timeout = math.floor(step_elapsed_time) in valid_timeout_numbers or math.ceil(step_elapsed_time) in valid_timeout_numbers
    
    return_value = False

    if step_time_is_valid_timeout:
        # If the step timed out at the max time for the job, it is most likely because
        # all other steps ran almost in no time.
        assert job_time_is_valid_timeout
        return_value = timed_out
    elif job_time_is_valid_timeout:
        return_value = timed_out
    else:
        # This is most likely not a timeout
        return_value = False

    return return_value

def get_task_result(result):
    """ Return the state of a azure job or azure step
    """

    if result == 0:
        result = "Abandoned"
    elif result == 1:
        result = "Canceled"
    elif result == 2:
        result = "Failed"
    elif result == 3:
        result = "Skipped"
    elif result == 4:
        result = "Succeeded"
    elif result == 5:
        result = "SucceededWithIssues"

    return result

################################################################################
# Class
################################################################################

class CompletePipelineRun:
    
    def __init__(self, cosmodb_db, pipeline, verbose=True):
        """ Constructor
        """
        self.db = cosmodb_db

        self.helix_workitem_container = self.db.get_container_client("helix-workitems")
        self.helix_submission_container = self.db.get_container_client("helix-submissions")

        self.pipeline_container = self.db.get_container_client("runtime-pipelines")
        self.jobs_container = self.db.get_container_client("runtime-jobs")

        self.failed_jobs = []
        self.jobs = []

        self.failed_work_items = []
        self.work_items = []

        self.failed_helix_submissions = []
        self.helix_submissions = []

        self.requires_computing_values = False
        self.required_keys = ["TotalWorkItemSetupTime", 
                              "TotalWorkItemSetupTimeCoreclr",
                              "TotalWorkItemSetupTimeMono",
                              "TotalWorkItemSetupTimeLibraries",
                              "TotalWorkItemRunTime", 
                              "TotalTestsRun", 
                              "TotalFailedTests"]

        for key in self.required_keys:
            if key not in pipeline:
                self.requires_computing_values = True
                break
        
        self.total_work_items = 0
        self.pipeline = pipeline

        # Seconds
        if self.requires_computing_values:
            self.total_setup_time = 0
            self.total_run_time = 0

            self.total_tests_run = 0
            self.total_test_failures = 0
            
            self.total_coreclr_setup_time = 0
            self.total_mono_setup_time = 0
            self.total_libraries_setup_time = 0
        else:
            self.total_setup_time = self.pipeline["TotalWorkItemSetupTime"]
            self.total_run_time = self.pipeline["TotalWorkItemRunTime"]

            self.total_tests_run = self.pipeline["TotalTestsRun"]
            self.total_test_failures = self.pipeline["TotalFailedTests"]

            self.total_coreclr_setup_time = self.pipeline["TotalWorkItemSetupTimeCoreclr"]
            self.total_mono_setup_time = self.pipeline["TotalWorkItemSetupTimeMono"]
            self.total_libraries_setup_time = self.pipeline["TotalWorkItemSetupTimeLibraries"]

        self.verbose = verbose

        self._get_jobs_for_pipeline()

        self.job_id_map = defaultdict(lambda: None)
        self.job_id_map_submissions = defaultdict(lambda: [])
        self.submission_id_map = defaultdict(lambda: [])

        for job in self.jobs:
            if self.job_id_map[job["JobGuid"]] != None:
                self.job_id_map[job["JobGuid"]]["Name"] == job["Name"]
            self.job_id_map[job["JobGuid"]] = job

        if self.requires_computing_values:
            self._get_workitems_for_pipeline()

    ############################################################################
    # Member methods
    ############################################################################

    def analyze(self):
        """ Go through each job and look for failure reasons.
        """
        failed_jobs = self.failed_jobs
        failure_reasons = defaultdict(lambda: [])

        for failed_job in failed_jobs:
            first_failed_step = None
            if failed_job["Name"] == "__default":
                continue

            for step in failed_job["Steps"]:
                if step["Result"] == 2 or step["Result"] == 1:
                    first_failed_step = step
                    break

            # If this is a helix submission we cannot just look at the console.
            # this is most likely a test failure.
            if first_failed_step["IsHelixSubmission"] or failed_job["Name"] == "Test crossgen-comparison Linux arm checked":
                if len(self.work_items) == 0:
                    self._get_workitems_for_pipeline()
                if len(self.helix_submissions) == 0:
                    self._get_submissions_for_pipeline()

                # Get all of the workitems for this job
                workitems = self.workitems_by_job[failed_job["JobGuid"]]
                result = get_task_result(failed_job["Result"])

                # This is most likely because we canceled the job waiting
                # for the helix submission to finish.

                divisor = 20 if failed_job["Name"] == "Test crossgen-comparison Linux arm checked" else 30

                assert result == "Canceled"
                # This is most likely a timeout
                time_out = check_if_timeout(failed_job, first_failed_step)

                submissions_for_job = self.job_id_map_submissions[failed_job["id"]]
                if time_out:
                    failure_reasons["Helix Submission Timeout"].append((failed_job, first_failed_step, submissions_for_job))
                else:
                    failure_reasons["User Canceled"].append((failed_job, first_failed_step))

                continue

                failed_workitems = []
                strange_workitems = []
                for item in workitems:
                    if item["ExitCode"] != 0:
                        failed_workitems.append(item)
                    if item["TotalRunTests"] == 0:
                        strange_workitems.append(item)

                i = 0

            else:
                # We will attempt to categorize this as infrastructure or build
                # related.

                is_infrastructure_related = False
                time_out = check_if_timeout(failed_job, first_failed_step)

                # Check for timeout
                if time_out:
                    # This is almost certainly a timeout.
                    failure_reasons["Timeout"].append((failed_job, first_failed_step))
                else:
                    # Check to see if this was user canceled
                    result = self.pipeline["BuildResult"]
                    if result == 0:
                        result = "Canceled"
                    elif result == 1:
                        result = "Failed"
                    elif result == 4:
                        result = "Passed"
                    else:
                        "NA"

                    if result == "Canceled":
                        failure_reasons["User Canceled"].append((failed_job, first_failed_step))
                        continue

                    result = get_task_result(failed_job["Result"])
                    if result == "Canceled":
                        failure_reasons["User Canceled"].append((failed_job, first_failed_step))
                        continue

                    i = 0

        self.failure_reasons = failure_reasons
        return failure_reasons

    def analyze_submissions(self):
        self._get_submissions_for_pipeline()
        helix_submission_jobs = []

        for job in self.jobs:
            is_helix_submission = False
            for step in job["Steps"]:
                if step["IsHelixSubmission"] is True:
                    is_helix_submission = True
                    break

            if is_helix_submission:
                helix_submission_jobs.append(job)

        submissions = []

        for job in helix_submission_jobs:
            # Get all of the submissions for this job
            submissions_for_job = self.job_id_map_submissions[job["id"]]
            workitems_for_job = self.workitems_by_job[job["JobGuid"]]

            for submission in submissions_for_job:
                submission["workitems"] = []
                for item in workitems_for_job:
                    if item["HelixSubmissionId"] == submission["id"]:
                        submission["workitems"].append(item)

                submissions.append(submission)

        # We now have all of the workitems for each submission
        for submission in submissions:
            submision_setup_time = 0
            submission_run_time = 0

            for item in submission["workitems"]:
                submision_setup_time += item["ElapsedSetupTime"]
                submission_run_time += item["ElapsedRunTime"]

            submission["TotalSetupTime"] = submision_setup_time
            submission["TotalRunTime"] = submission_run_time
        
        return submissions

    def analyze_workitems(self):
        self._get_submissions_for_pipeline()
        helix_submission_jobs = []

        for job in self.jobs:
            is_helix_submission = False
            for step in job["Steps"]:
                if step["IsHelixSubmission"] is True:
                    is_helix_submission = True
                    break

            if is_helix_submission:
                helix_submission_jobs.append(job)

        submissions = []

        for job in helix_submission_jobs:
            # Get all of the submissions for this job
            submissions_for_job = self.job_id_map_submissions[job["id"]]
            workitems_for_job = self.workitems_by_job[job["JobGuid"]]

            for submission in submissions_for_job:
                submission["workitems"] = []
                for item in workitems_for_job:
                    if item["HelixSubmissionId"] == submission["id"]:
                        submission["workitems"].append(item)

                submissions.append(submission)

        # We now have all of the workitems for each submission
        for submission in submissions:
            submision_setup_time = 0
            submission_run_time = 0

            for item in submission["workitems"]:
                submision_setup_time += item["ElapsedSetupTime"]
                submission_run_time += item["ElapsedRunTime"]

            submission["TotalSetupTime"] = submision_setup_time
            submission["TotalRunTime"] = submission_run_time
        
        return submissions

    def passed(self):
        """ Returned wether the pipeline failed
        """

        result = self.pipeline["BuildResult"]

        if result == 0:
            result = "Canceled"
        elif result == 1:
            result = "Failed"
        elif result == 4:
            result = "Passed"
        else:
            "NA"

        if result != "Passed" and len(self.failed_jobs) > 0:
            return False

        return True

    def update_pipeline_if_needed(self):
        """ Update a pipeline data for the pipeline changed

        Notes:
            This is generally done if the calculated work item setup time, run
            time, failed tests, and total tests are not present in the pipeline.
        
            Adding them to the database, avoid having to recompute all of these
            values.
        
        """

        required_update = False

        for key in self.required_keys:
            if key not in self.pipeline:
                required_update = True
                break

        if not required_update:
            return

        self.pipeline["TotalWorkItemSetupTime"] = self.total_setup_time
        self.pipeline["TotalWorkItemRunTime"] = self.total_run_time

        self.pipeline["TotalTestsRun"] = self.total_tests_run
        self.pipeline["TotalFailedTests"] = self.total_test_failures

        self.pipeline["TotalWorkItemSetupTimeCoreclr"] = self.total_coreclr_setup_time
        self.pipeline["TotalWorkItemSetupTimeMono"] = self.total_mono_setup_time
        self.pipeline["TotalWorkItemSetupTimeLibraries"] = self.total_libraries_setup_time

        # Make sure there are no duplicates
        pipelines = list(self.pipeline_container.query_items(query={
                                                'query': 'SELECT * FROM root pipeline WHERE pipeline.id=@pipeline_id',
                                                'parameters': [
                                                    {'name': '@pipeline_id', 'value': self.pipeline["id"]}
                                                ]
                                            },
                                            enable_cross_partition_query=True))
        
        assert(len(pipelines) == 1)

        # pipeline container
        self.pipeline_container.upsert_item(body=self.pipeline)

        pipelines = list(self.pipeline_container.query_items(query={
                                                'query': 'SELECT * FROM root pipeline WHERE pipeline.id=@pipeline_id',
                                                'parameters': [
                                                    {'name': '@pipeline_id', 'value': self.pipeline["id"]}
                                                ]
                                            },
                                            enable_cross_partition_query=True))

        assert(len(pipelines) == 1)

        for key in self.required_keys:
            assert key in pipelines[0]

        for key in self.pipeline:
            assert key in pipelines[0]

    ############################################################################
    # Helper methods
    ############################################################################

    def _get_jobs_for_pipeline(self):
        """ Get all of the jobs for this pipeline.
        """

        # Get only jobs from this pipeline
        jobs = list(self.jobs_container.query_items(query = {
                                                'query': 'SELECT * FROM root job WHERE job.PipelineId=@pipeline_id',
                                                'parameters': [
                                                    {'name': '@pipeline_id', 'value': self.pipeline["id"]}
                                                ]
                                            },
                                            enable_cross_partition_query=True))

        self._log("[{}]: Jobs downloaded.".format(len(jobs)))

        self.jobs = jobs

        for job in self.jobs:
            passed = job["Result"]

            if passed == 0:
                passed = "Canceled"
            elif passed == 1:
                passed = "Failed"
            elif passed == 4:
                passed = "Passed"
            else:
                "NA"

            if passed == "Failed":
                self.failed_jobs.append(job)

        self._log("[{}]: Failed jobs.".format(len(self.failed_jobs)))

    def _get_workitems_for_pipeline(self):
        # Get only jobs from this pipeline
        workitems = list(self.helix_workitem_container.query_items(query= {
                                                'query': 'SELECT * FROM root workitem WHERE workitem.RuntimePipelineId=@pipeline_id',
                                                'parameters': [
                                                    {'name': '@pipeline_id', 'value': self.pipeline["id"]}
                                                ]
                                            },
                                            enable_cross_partition_query=True))

        self._log("[{}]: Workitems downloaded.".format(len(workitems)))

        self.work_items = workitems

        for item in self.work_items:
            passed = item["ExitCode"] == 0
            assert self.job_id_map[item["JobId"]] != None
            job_name = self.job_id_map[item["JobId"]]["Name"].lower()

            if passed == False:
                self.failed_work_items.append(item)

            self.total_run_time += item["ElapsedRunTime"]
            self.total_setup_time += item["ElapsedSetupTime"]

            if "FailedTests" in item:
                self.total_test_failures += item["FailedTests"]
                self.total_tests_run += item["TotalRunTests"]

            if "libraries" in job_name:
                self.total_libraries_setup_time += item["ElapsedSetupTime"]
            elif "coreclr" in job_name and "runtime" in job_name:
                assert "libraries" not in job_name
                assert "mono" not in job_name
                self.total_coreclr_setup_time += item["ElapsedSetupTime"]
            elif "mono" in job_name and "runtime" in job_name:
                assert "libraries" not in job_name
                assert "coreclr" not in job_name
                self.total_mono_setup_time += item["ElapsedSetupTime"]
            elif "coreclr" in job_name and "test run" in job_name:
                assert "libraries" not in job_name
                assert "mono" not in job_name
                self.total_coreclr_setup_time += item["ElapsedSetupTime"]
            elif "mono" in job_name and ("test run" in job_name or "allsubsets" in job_name):
                assert "libraries" not in job_name
                assert "coreclr" not in job_name
                self.total_mono_setup_time += item["ElapsedSetupTime"]
            else:
                assert False

        self.total_setup_time /= 1000
        self.total_run_time /= 1000

        self.total_libraries_setup_time /= 1000
        self.total_coreclr_setup_time /= 1000
        self.total_mono_setup_time /= 1000

        self._log("[{}]: Setup time: {}. Libraries setup time: {}, CoreCLR setup time: {}, Mono setup time: {}, Run time: {}. Total tests: {}. Failed tests: {}.".format(self.pipeline["id"], self.total_setup_time, self.total_libraries_setup_time, self.total_coreclr_setup_time , self.total_mono_setup_time, self.total_run_time, self.total_tests_run, self.total_test_failures))
        
        self.workitems_by_job = defaultdict(lambda: [])
        for workitem in self.work_items:
            self.workitems_by_job[workitem["JobId"]].append(workitem)

        for item in self.work_items:
            assert item["HelixSubmissionId"] is not None
            self.submission_id_map[item["HelixSubmissionId"]].append(item)

    def _get_submissions_for_pipeline(self):
        # Get only jobs from this pipeline
        submissions = list(self.helix_submission_container.query_items(query= {
                                                'query': 'SELECT * FROM root workitem WHERE workitem.RuntimePipelineId=@pipeline_id',
                                                'parameters': [
                                                    {'name': '@pipeline_id', 'value': self.pipeline["id"]}
                                                ]
                                            },
                                            enable_cross_partition_query=True))

        self._log("[{}]: Submissions downloaded.".format(len(submissions)))

        self.helix_submissions = submissions
        if len(self.work_items) == 0:
            self._get_workitems_for_pipeline()

        for submission in self.helix_submissions:
            workitems_for_submission = self.submission_id_map[submission["id"]]

            passed = True
            for item in workitems_for_submission:
                if item["ExitCode"] != 0:
                    passed = False
                    break

            if not passed:
                self.failed_helix_submissions.append(submission)

        for submission in self.helix_submissions:
            self.job_id_map_submissions[submission["JobId"]].append(submission)

    def _log(self, message):
        if self.verbose:
            print(message)