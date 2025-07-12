# aws-bank-transactions-pipeline

Este repositório no GitHub define a infraestrutura e a automação de um pipeline de dados na AWS, focado na ingestão, processamento e notificação de transações bancárias.

---

## Propósito do Projeto

O objetivo principal deste projeto é criar um fluxo de trabalho automatizado e escalável na AWS para gerenciar transações bancárias. Isso abrange desde a ingestão inicial dos dados até o processamento e a emissão de notificações relevantes, garantindo que os dados das transações sejam tratados de forma eficiente e segura na nuvem.

---

## Tecnologias AWS Utilizadas

O pipeline de transações bancárias é construído usando uma combinação de serviços da Amazon Web Services (AWS) para garantir um fluxo de trabalho robusto e confiável:

* **AWS Glue:** Usado para o processamento de dados, transformando e preparando as transações bancárias.
* **AWS Step Functions:** Orquestra o fluxo de trabalho do pipeline, gerenciando as etapas e a lógica de execução.
* **AWS Lambda:** Funções serverless que executam código em resposta a eventos, essenciais para a automação e execução de tarefas específicas.
* **Amazon SNS (Simple Notification Service):** Utilizado para o sistema de notificação, permitindo o envio de mensagens ou alertas sobre o status ou eventos das transações.
* **Amazon EventBridge:** Provavelmente usado para agendamento ou roteamento de eventos que disparam partes do pipeline.
* **IAM Role (Identity and Access Management):** Para gerenciar permissões e acessos dos diferentes serviços e componentes do pipeline.

---

## Estrutura do Repositório

O repositório está organizado em pastas que correspondem aos diferentes serviços da AWS e componentes do pipeline, facilitando a navegação e a compreensão da arquitetura:

* `EventBridge/`
* `Glue Job/`
* `IAM Role/`
* `Lambda/`
* `Payload/`
* `Step Function/`

---

## Linguagem de Programação

Todo o projeto é desenvolvido em **Python** (representando 100% da base de código).

---

Para mais detalhes e para explorar o código-fonte, visite o repositório no GitHub: [LeoTortega/aws-bank-transactions-pipeline](https://github.com/LeoTortega/aws-bank-transactions-pipeline)