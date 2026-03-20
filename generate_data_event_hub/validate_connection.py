"""
Valida a conexão com o Azure Event Hub antes de rodar o producer.

Uso:
    python validate_connection.py
"""

import json
import sys
import logging
from azure.eventhub import EventHubProducerClient, EventData

import config

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)


def validate():
    if not config.EVENT_HUB_CONNECTION_STRING:
        log.error("EVENT_HUB_CONNECTION_STRING não definida no .env")
        sys.exit(1)

    log.info("Conectando ao Event Hub '%s'...", config.EVENT_HUB_NAME)

    try:
        client = EventHubProducerClient.from_connection_string(
            conn_str=config.EVENT_HUB_CONNECTION_STRING,
            eventhub_name=config.EVENT_HUB_NAME,
        )
        with client:
            props = client.get_eventhub_properties()
            log.info("Conexao OK!")
            log.info("  Nome:       %s", props["eventhub_name"])
            log.info("  Partitions: %s", props["partition_ids"])
            log.info("  Criado em:  %s", props["created_at"])

            # envia 1 evento de teste
            batch = client.create_batch()
            batch.add(EventData(json.dumps({"test": True, "source": "validate_connection.py"})))
            client.send_batch(batch)
            log.info("Evento de teste enviado com sucesso.")

    except Exception as e:
        log.error("Falha na conexao: %s", e)
        sys.exit(1)


if __name__ == "__main__":
    validate()
