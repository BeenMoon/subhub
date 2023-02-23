from slack_sdk import WebClient
from airflow.models import Variable

def send_slack_result(result:str) -> None:
    client = WebClient(Variable.get('slack_api_token'))
    client.chat_postMessage(channel='#일반', text=f"The DAG {context['run_id']} {result}", username="Airflow")
