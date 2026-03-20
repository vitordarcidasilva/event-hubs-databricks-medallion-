"""
Geradores de eventos sintéticos de campanha Retail Media.

Simula o fluxo realista de um funil de mídia:
  impressão → clique (CTR ~2%) → conversão (CVR ~5% dos cliques)
"""

import uuid
import hashlib
import random
from datetime import datetime, timezone
from typing import Optional

from faker import Faker

from schema import ImpressionEvent, ClickEvent, ConversionEvent

fake = Faker("pt_BR")

# ---------------------------------------------------------------------------
# Dados de referência — simulam catálogo real de campanhas
# ---------------------------------------------------------------------------

CAMPAIGNS = [
    {"campaign_id": "camp_001", "advertiser_id": "adv_carrefour", "name": "Verão Carrefour"},
    {"campaign_id": "camp_002", "advertiser_id": "adv_unilever",  "name": "Dove Hidratação"},
    {"campaign_id": "camp_003", "advertiser_id": "adv_ambev",     "name": "Brahma Verão"},
    {"campaign_id": "camp_004", "advertiser_id": "adv_nestle",    "name": "Kit Kat Especial"},
    {"campaign_id": "camp_005", "advertiser_id": "adv_samsung",   "name": "Galaxy S24"},
]

PUBLISHERS = ["pub_marketplace_br", "pub_app_mobile", "pub_site_receitas"]

PLACEMENTS = ["home_banner", "search_sponsored", "category_top", "pdp_sidebar", "checkout_upsell"]

PRODUCT_CATEGORIES = [
    "bebidas", "higiene_pessoal", "alimentos", "eletronicos",
    "limpeza", "beleza", "snacks", "laticinios",
]

PRODUCTS = {
    "bebidas":         ["prod_brahma_350", "prod_heineken_600", "prod_agua_mineral"],
    "higiene_pessoal": ["prod_dove_shampoo", "prod_oral_b", "prod_gillette"],
    "alimentos":       ["prod_kitkat_4f", "prod_bis_chocolate", "prod_oreo_pck"],
    "eletronicos":     ["prod_galaxy_s24", "prod_galaxy_tab", "prod_galaxy_buds"],
    "limpeza":         ["prod_omo_liq", "prod_veja_multiuso", "prod_pinho_sol"],
    "beleza":          ["prod_loreal_creme", "prod_pantene_cond", "prod_nivea_uv"],
    "snacks":          ["prod_ruffles_orig", "prod_doritos_nacho", "prod_pringles_orig"],
    "laticinios":      ["prod_danone_ativia", "prod_nesquik_pó", "prod_piracanjuba_lt"],
}

DEVICES  = ["mobile", "desktop", "tablet"]
CHANNELS = ["app", "web"]


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _new_id() -> str:
    return str(uuid.uuid4())


def _hash_user(raw_id: str) -> str:
    """Simula anonimização: recebe um ID interno e devolve hash SHA-256."""
    return hashlib.sha256(raw_id.encode()).hexdigest()


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _pick_campaign() -> dict:
    return random.choice(CAMPAIGNS)


def _pick_product(category: str) -> str:
    return random.choice(PRODUCTS[category])


# ---------------------------------------------------------------------------
# Geradores principais
# ---------------------------------------------------------------------------

def generate_impression(user_raw_id: Optional[str] = None) -> ImpressionEvent:
    campaign = _pick_campaign()
    user_raw = user_raw_id or str(uuid.uuid4())
    viewable_secs = round(random.uniform(0.5, 30.0), 2)

    return ImpressionEvent(
        event_id=_new_id(),
        event_timestamp=_now_iso(),
        campaign_id=campaign["campaign_id"],
        ad_id=f"ad_{_new_id()[:8]}",
        advertiser_id=campaign["advertiser_id"],
        publisher_id=random.choice(PUBLISHERS),
        placement=random.choice(PLACEMENTS),
        user_id_hashed=_hash_user(user_raw),
        session_id=_new_id(),
        device_type=random.choice(DEVICES),
        channel=random.choice(CHANNELS),
        viewable=viewable_secs >= 1.0,
        viewable_seconds=viewable_secs,
    )


def generate_click(impression: ImpressionEvent) -> ClickEvent:
    """Gera um click derivado de uma impressão (mantém contexto de campanha)."""
    return ClickEvent(
        event_id=_new_id(),
        event_timestamp=_now_iso(),
        campaign_id=impression.campaign_id,
        ad_id=impression.ad_id,
        advertiser_id=impression.advertiser_id,
        publisher_id=impression.publisher_id,
        placement=impression.placement,
        user_id_hashed=impression.user_id_hashed,
        session_id=impression.session_id,
        device_type=impression.device_type,
        channel=impression.channel,
        impression_id=impression.event_id,
    )


def generate_conversion(click: ClickEvent) -> ConversionEvent:
    """Gera uma conversão derivada de um click."""
    category = random.choice(PRODUCT_CATEGORIES)
    product  = _pick_product(category)
    quantity = random.randint(1, 5)
    unit_price = round(random.uniform(5.0, 350.0), 2)

    return ConversionEvent(
        event_id=_new_id(),
        event_timestamp=_now_iso(),
        campaign_id=click.campaign_id,
        ad_id=click.ad_id,
        advertiser_id=click.advertiser_id,
        publisher_id=click.publisher_id,
        user_id_hashed=click.user_id_hashed,
        session_id=click.session_id,
        click_id=click.event_id,
        impression_id=click.impression_id,
        order_id=f"ord_{_new_id()[:12]}",
        product_id=product,
        product_category=category,
        revenue=round(unit_price * quantity, 2),
        quantity=quantity,
        attribution_model=random.choice(["last_click", "linear", "time_decay"]),
    )


# ---------------------------------------------------------------------------
# Gerador de funil completo
# ---------------------------------------------------------------------------

def generate_funnel_events(
    ctr: float = 0.02,
    cvr: float = 0.05,
) -> list:
    """
    Gera 1 impressão e, probabilisticamente, click e conversão.

    Args:
        ctr: Click-Through Rate esperado (padrão 2%)
        cvr: Conversion Rate sobre clicks (padrão 5%)

    Returns:
        Lista de eventos na ordem cronológica do funil.
    """
    events = []
    impression = generate_impression()
    events.append(impression)

    if random.random() < ctr:
        click = generate_click(impression)
        events.append(click)

        if random.random() < cvr:
            conversion = generate_conversion(click)
            events.append(conversion)

    return events
