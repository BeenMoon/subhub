from slack_sdk import WebClient
from airflow.models import Variable

def send_slack_success(context) -> None:
    client = WebClient(Variable.get('slack_api_token'))
    client.chat_postMessage(channel='#일반', text=f"The DAG {context['dag_run'].dag_id} succeeded", username="Airflow")
    
def send_slack_failure(context) -> None:
    client = WebClient(Variable.get('slack_api_token'))
    client.chat_postMessage(channel='#일반', text=f"The DAG {context['dag_run'].dag_id} failed", username="Airflow")
