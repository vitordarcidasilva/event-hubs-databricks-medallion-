"""
Schemas dos eventos de campanha de Retail Media.

Tipos de evento:
  - impression : anúncio exibido para o usuário
  - click      : usuário clicou no anúncio
  - conversion : usuário realizou compra após interação com anúncio
"""

from dataclasses import dataclass, field, asdict
from typing import Literal, Optional
import json

EventType = Literal["impression", "click", "conversion"]


@dataclass
class ImpressionEvent:
    event_id: str
    event_type: str = "impression"
    event_timestamp: str = ""
    campaign_id: str = ""
    ad_id: str = ""
    advertiser_id: str = ""
    publisher_id: str = ""
    placement: str = ""          # home_banner, search_sponsored, category_top
    user_id_hashed: str = ""     # SHA-256 — sem PII exposto
    session_id: str = ""
    device_type: str = ""        # mobile, desktop, tablet
    channel: str = ""            # app, web
    # métricas de viewability
    viewable: bool = True
    viewable_seconds: float = 0.0

    def to_json(self) -> str:
        return json.dumps(asdict(self))


@dataclass
class ClickEvent:
    event_id: str
    event_type: str = "click"
    event_timestamp: str = ""
    campaign_id: str = ""
    ad_id: str = ""
    advertiser_id: str = ""
    publisher_id: str = ""
    placement: str = ""
    user_id_hashed: str = ""
    session_id: str = ""
    device_type: str = ""
    channel: str = ""
    impression_id: str = ""      # referência ao impression original

    def to_json(self) -> str:
        return json.dumps(asdict(self))


@dataclass
class ConversionEvent:
    event_id: str
    event_type: str = "conversion"
    event_timestamp: str = ""
    campaign_id: str = ""
    ad_id: str = ""
    advertiser_id: str = ""
    publisher_id: str = ""
    user_id_hashed: str = ""
    session_id: str = ""
    click_id: str = ""           # referência ao click que gerou a conversão
    impression_id: str = ""
    # dados de transação (sem PII)
    order_id: str = ""
    product_id: str = ""
    product_category: str = ""
    revenue: float = 0.0
    quantity: int = 1
    # atribuição
    attribution_model: str = "last_click"   # last_click | linear | time_decay

    def to_json(self) -> str:
        return json.dumps(asdict(self))
