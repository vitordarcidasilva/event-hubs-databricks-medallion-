"""
Producer de eventos de Retail Media para Azure Event Hub.

Uso:
    python producer.py                  # envia TOTAL_EVENTS eventos
    python producer.py --total 500      # sobrescreve quantidade
    python producer.py --dry-run        # apenas imprime, não envia
"""

import argparse
import json
import time
import logging
from azure.eventhub import EventHubProducerClient, EventData, EventDataBatch

import config
from generators import generate_funnel_events

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
log = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Envio em batch — mais eficiente que envio unitário
# ---------------------------------------------------------------------------

def send_batch(client: EventHubProducerClient, events: list[dict]) -> int:
    """Envia uma lista de dicts como eventos em batch. Retorna qtd enviada."""
    batch: EventDataBatch = client.create_batch()
    sent = 0
    for ev in events:
        try:
            batch.add(EventData(json.dumps(ev)))
            sent += 1
        except ValueError:
            # batch cheio — envia e abre novo
            client.send_batch(batch)
            batch = client.create_batch()
            batch.add(EventData(json.dumps(ev)))
            sent += 1

    if len(batch) > 0:
        client.send_batch(batch)

    return sent


# ---------------------------------------------------------------------------
# Loop principal
# ---------------------------------------------------------------------------

def run(total_events: int, events_per_second: int, dry_run: bool) -> None:
    delay = 1.0 / events_per_second
    total_sent = 0
    pending: list[dict] = []

    log.info(
        "Iniciando producer | total=%d | rate=%d ev/s | dry_run=%s",
        total_events, events_per_second, dry_run,
    )

    client = None
    if not dry_run:
        client = EventHubProducerClient.from_connection_string(
            conn_str=config.EVENT_HUB_CONNECTION_STRING,
            eventhub_name=config.EVENT_HUB_NAME,
        )

    try:
        while total_sent < total_events:
            funnel = generate_funnel_events()
            for ev in funnel:
                if total_sent >= total_events:
                    break

                ev_dict = json.loads(ev.to_json())

                if dry_run:
                    print(json.dumps(ev_dict, ensure_ascii=False))
                else:
                    pending.append(ev_dict)
                    # flush a cada 100 eventos para latência razoável
                    if len(pending) >= 100:
                        sent = send_batch(client, pending)
                        total_sent += sent
                        log.info("Enviados: %d / %d", total_sent, total_events)
                        pending.clear()

                if dry_run:
                    total_sent += 1
                    log.info("Evento gerado: %d / %d [%s]", total_sent, total_events, ev_dict["event_type"])

                time.sleep(delay)

        # flush restante
        if not dry_run and pending:
            sent = send_batch(client, pending)
            total_sent += sent
            log.info("Enviados: %d / %d (flush final)", total_sent, total_events)

    finally:
        if client:
            client.close()

    log.info("Producer finalizado. Total de eventos enviados: %d", total_sent)


# ---------------------------------------------------------------------------
# Entrypoint
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Retail Media Event Hub Producer")
    parser.add_argument("--total",   type=int,  default=config.TOTAL_EVENTS,       help="Total de eventos a enviar")
    parser.add_argument("--rate",    type=int,  default=config.EVENTS_PER_SECOND,  help="Eventos por segundo")
    parser.add_argument("--dry-run", action="store_true",                           help="Apenas imprime eventos sem enviar")
    args = parser.parse_args()

    run(
        total_events=args.total,
        events_per_second=args.rate,
        dry_run=args.dry_run,
    )
