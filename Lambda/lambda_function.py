import json
import os
import boto3

SNS_TOPIC_ARN = os.environ.get('SNS_TOPIC_ARN')

def lambda_handler(event, context):
    sns_client = boto3.client('sns')
    
    # Logs do evento de entrada para depuração
    print(f"Received event: {json.dumps(event)}")
    
    message = "Erro ao processar o status do Job."
    subject = "Status do AWS Glue Job Desconhecido"
    
    try:

        job_run_state = event.get('JobRunState')
        job_name = event.get('JobName', 'UNKNOWN_JOB')
        job_run_id = event.get('Id', 'UNKNOWN_RUN_ID')
        
        if job_run_state == 'SUCCEEDED':
            subject = f"✅ SUCESSO: Job Glue '{job_name}' Concluído!"
            message = (
                f"O Job Glue '{job_name}' (ID: {job_run_id}) foi concluído com SUCESSO.\n\n"
                f"Detalhes do Job Run:\n{json.dumps(event, indent=2)}"
            )
        elif job_run_state == 'FAILED':
            error_message = event.get('ErrorMessage', 'N/A')
            subject = f"❌ FALHA: Job Glue '{job_name}' Falhou!"
            message = (
                f"O Job Glue '{job_name}' (ID: {job_run_id}) FALHOU.\n\n"
                f"Mensagem de Erro: {error_message}\n\n"
                f"Detalhes do Job Run:\n{json.dumps(event, indent=2)}"
            )
        else:
            # Caso o estado não seja SUCCEEDED nem FAILED (e.g., STOPPED, CANCELED)
            subject = f"⚠️ ALERTA: Job Glue '{job_name}' com Estado: {job_run_state}"
            message = (
                f"O Job Glue '{job_name}' (ID: {job_run_id}) terminou com o estado '{job_run_state}'.\n\n"
                f"Detalhes do Job Run:\n{json.dumps(event, indent=2)}"
            )

        # Publica a mensagem no tópico SNS
        sns_client.publish(
            TopicArn=SNS_TOPIC_ARN,
            Subject=subject,
            Message=message
        )
        print(f"Notification sent to SNS topic: {SNS_TOPIC_ARN}")
        
    except Exception as e:
        print(f"Error processing Lambda event or sending SNS: {e}")
        # Em caso de erro na própria Lambda, você pode querer enviar uma notificação de erro interno
        sns_client.publish(
            TopicArn=SNS_TOPIC_ARN,
            Subject=f"🔴 ERRO INTERNO NA LAMBDA DE NOTIFICAÇÃO DO GLUE JOB!",
            Message=f"Falha ao processar evento ou enviar notificação SNS.\nErro: {str(e)}\nEvento recebido: {json.dumps(event, indent=2)}"
        )
        raise # Re-lança a exceção para que o Step Functions marque a Lambda como falha

    return {
        'statusCode': 200,
        'body': json.dumps('Notification sent successfully!')
    }
