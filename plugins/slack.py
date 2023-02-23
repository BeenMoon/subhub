from slack_sdk import WebClient
from airflow.models import Variable

def send_slack_result(result:str) -> None:
    client = WebClient(Variable.get('slack_api_token'))
    client.chat_postMessage(channel='#general', text=f"The DAG {{ dag.dag_id }} {result}", username="Airflow")