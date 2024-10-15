import os
import datetime
import subprocess
from git import Repo
from git import rmtree
from utils import utilities
from utils.base_dag import BaseDAG as DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.models import Variable

OWNER = "Pratyush"
EMAILS = ["pratyushpratapsingh11@gmail.com"]
START_DATE = "2024-01-01 01:00"
SCHEDULE = None
TAGS = ["bot"]

PROJECT, NAME = utilities.get_dag_name()

current_directory = os.getcwd()
key_path = Variable.get("gitlab_key_path")
servicenow_api_token = Variable.get("app1_api_token")
gemini_dev_api_token = Variable.get("app2_dev_api_token")
remote_git_url = "git@gitorgn.internal.domain.com:CORE/cops_bot.git" # CORE is the group name where it is hosted

def trigger_bot_script(**kwargs):
    print(f"*********Bot Script Start**********")
    bash_command = (
        "python "
        + current_directory
        + "/cops_bot/cops_bot.py --"
        + servicenow_api_token 
        + " --"
        + gemini_dev_api_token 
    )

    if servicenow_api_token and gemini_dev_api_token:
        # delete the repo if it exists, perform shallow clone, get SHA, delete repo
        repo = os.path.join(current_directory, "cops_bot")

        if os.path.exists(repo):
            rmtree(repo)

        print(f"**********Cloning Started **********")

        Repo.clone_from(
            remote_git_url,
            repo,
            branch="dev",
            recursive=True,
            depth=1,
            env={"GIT_SSH_COMMAND": f"ssh -i {key_path} -o StrictHostKeyChecking=no"},
        )

        print(f"**********Cloning Done **********")
    else:
        print(f"Please Provide config parameters to onboard BU")

    try:
        ls = subprocess.run(
            bash_command, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE
        )
        ls.check_returncode()
    except subprocess.CalledProcessError as e:
        print(
            "Error:\nreturn code: ",
            e.returncode,
            "\nOutput: ",
            e.stderr.decode("utf-8"),
            "\nStdout:",
            e.stdout,
        )
        raise AssertionError(str(e)) from None

    print(f"**********Bot Script End**********")


dag = DAG(
    project=PROJECT,
    name=NAME,
    schedule_interval=SCHEDULE,
    catchup=False,
    default_args={
        "start_date": datetime.datetime.strptime(START_DATE, "%Y-%m-%d %H:%S"),
        "owner": OWNER,
        "email": EMAILS,
        "email_on_retry": False,
        "email_on_failure": True,
        "retries": 0,
    },
    tags=TAGS,
    # access_control={"core": {"can_dag_read", "can_dag_edit"}},
)

dummy_start = DummyOperator(task_id="start_task", dag=dag)

trigger_bot_script = PythonOperator(
    task_id="trigger_bot_script",
    python_callable=trigger_bot_script,
    provide_context=True,
    dag=dag,
)

dummy_end = DummyOperator(task_id="end_task", dag=dag)

dummy_start >> trigger_bot_script >> dummy_end
