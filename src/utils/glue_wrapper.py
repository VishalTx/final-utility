import time
from botocore.exceptions import ClientError
from utils.aws_wrapper import AWS, AWSCloudwatch
from utils.jira_wrapper import log_defect

class AWSGlueWrapper(AWS):
    def __init__(self):
        super().__init__()
        self.client = self.session.client(
            'glue',
            region_name=self.region
        )
        self.cloudwatch = AWSCloudwatch()

    def monitor(self, job_name, job_run_id, poll_interval=10):
        state = None
        try:
            while True:
                response = self.client.get_job_run(
                    JobName=job_name,
                    RunId=job_run_id
                )
                state = response['JobRun']['JobRunState']
                print("Current State: ", state)
                if state in ['SUCCEEDED', 'FAILED', 'STOPPED']:
                    break

                time.sleep(poll_interval)
        except ClientError as e:
            print(f"Error fetching job run status: {e.response['Error']['Message']}")
            return False

        if state == 'SUCCEEDED':
            return job_run_id, state, 'Glue job completed successfully.'
        else:
            return job_run_id, state, f"Glue job did not complete successfully. Final status: {state}"

    def run_job(self, job_name, job_arguments):
        try:
            response = self.client.start_job_run(
                JobName=job_name,
                Arguments=job_arguments
            )
            job_run_id = response['JobRunId']
            return self.monitor(job_name, job_run_id)
        except ClientError as e:
            return None, "NOTSTARTED", f"Failed to start Glue job: {e.response['Error']['Message']}"


if __name__ == "__main__":
    glue = AWSGlueWrapper()