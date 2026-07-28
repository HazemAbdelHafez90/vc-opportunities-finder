from __future__ import annotations

import re
import unicodedata
from typing import Any
from urllib import parse


# Exported from the Fairpicture project database: organisation name, website, project count.
# Refresh this list by re-running the client export query and pasting the rows below.
RAW_CLIENT_ROSTER: list[tuple[str, str, int]] = [
    ("ACT Alliance", "", 1),
    ("Age International", "https://www.ageinternational.org.uk/", 3),
    ("Aktion gegen den Hunger gGmbH", "https://www.aktiongegendenhunger.de", 1),
    ("Amref Health Africa", "https://Amref.org", 1),
    ("Avaloq", "https://avaloq.com", 1),
    ("BMS World Mission", "https://www.bmsworldmission.org", 5),
    ("Beyond Fossil Fuels", "https://beyondfossilfuels.org/", 4),
    ("Biovision Foundation", "", 3),
    ("Bridge-the-Gap", "https://www.bridge-the-gap.co.za", 1),
    ("Brink", "https://www.hellobrink.co/", 1),
    ("Brot für die Welt", "https://www.brot-fuer-die-welt.de", 1),
    ("CARE Österreich", "", 1),
    ("CARTIER PHILANTHROPY", "https://cartierphilanthropy.org", 1),
    ("Canadian Lutheran World Relief", "https://clwr.org", 2),
    ("Caritas Schweiz", "", 1),
    ("Caritas Österreich", "http://caritas.at", 3),
    ("Cartier for Nature", "https://www.cartierfornature.org", 2),
    ("Christian Aid", "https://christianaid.org.uk", 1),
    ("Commerce Equitable Afrique", "https://www.fairtradeafrica.net", 1),
    ("Comundo", "https://www.comundo.org", 1),
    ("Connexio hope and develop", "https://connexio.ch/", 3),
    ("DAI", "https://dai.com", 3),
    ("Deutsche Welthungerhilfe", "https://welthungerhilfe.de", 5),
    ("Disasters Emergency Commission", "https://www.dec.org.uk", 15),
    ("Don Bosco Mondo e.V.", "", 1),
    ("Drosos Foundation", "https://drosos.org", 1),
    ("Enfants du Monde", "", 2),
    ("Enviu", "https://www.enviu.org", 1),
    ("Fairtrade Deutschland e.V.", "https://www.fairtrade-deutschland.de", 4),
    ("Fairventures Worldwide", "https://fairventures.org", 1),
    ("Fauna & Flora", "https://www.fauna-flora.org/", 2),
    ("Fondation Hirondelle", "https://www.hirondelle.org", 3),
    ("Fondation Terre des hommes", "https://www.tdh.org", 2),
    ("Frauenbund Schweiz (Swiss Women's League)", "https://www.elisabethenwerk.ch", 1),
    ("Fundación Vicente Ferrer", "https://fundacionvicenteferrer.org", 1),
    ("Green Fleming Inc", "https://www.caxa.ws", 6),
    ("HNST DESIGN STUDIO", "https://www.thehnst.co", 1),
    ("Help – Hilfe zur Selbsthilfe e.V.", "https://www.help-ev.de", 1),
    ("HelpAge International", "", 1),
    ("IEC - International Electrotechnical Commission", "https://www.iec.ch", 4),
    ("IFRC", "https://www.ifrc.org", 7),
    ("ILO Viet Nam", "https://www.ilo.org", 2),
    ("International Health Partners", "https://www.ihpuk.org/", 2),
    ("International Trade Center", "https://www.intracen.org/", 1),
    ("International Trade Centre (ITC)", "http://intracen.org", 7),
    ("IsraAID", "https://www.israaid.org/", 1),
    ("Leopold Bachmann Foundation", "https://www.lb-foundation.ch", 4),
    ("MCTC Marine", "https://mctconsultancy.com/", 1),
    ("Martin and Martin", "https://www.martinandmartin.eu", 1),
    ("Mennonite Central Committee U.S.", "http://mcc.org", 8),
    ("Misereor", "https://www.misereor.de", 1),
    ("Missio Weltkirche", "https://missio.ch", 1),
    ("Missio Österreich", "https://www.missio.at", 2),
    ("Mission Evangélique Braille", "https://www.mebraille.ch", 1),
    ("Médecins Sans Frontières", "https://www.msf.org", 1),
    ("Naturland e.V.", "https://www.naturland.org", 2),
    ("Norbrook", "https://www.norbrook.com", 1),
    ("Novo Nordisk Haemophilia & Haemoglobinopathies Foundation", "http://nnhf.org", 6),
    ("OroVerde", "https://www.oroverde.de", 1),
    ("Pakka AG", "", 1),
    ("Public Eye", "https://www.publiceye.ch", 1),
    ("Remotecoders", "https://remotecoders.org/", 1),
    ("Research Institute AG & Co KG", "https://researchinstitute.at", 1),
    ("SOS-Kinderdorf Schweiz", "", 4),
    ("SWISSAID", "", 10),
    ("Saferworld", "http://www.saferworld-global.org", 3),
    ("Schweizerisches Rotes Kreuz", "http://redcross.ch", 2),
    ("Shared Interest", "http://www.shared-interest.com", 6),
    ("SolarBuddy", "https://solarbuddy.org", 2),
    ("SolidarMed", "http://www.solidarmed.ch", 3),
    ("Stadt Luzern / Präsidiales", "https://www.stadtluzern.ch", 1),
    ("Stadt Neumarkt i.d.OPf.", "https://www.neumarkt.de", 1),
    ("Swiss Re Foundation", "http://www.swissrefoundation.org", 8),
    ("Tearfund", "https://www.tearfund.org", 1),
    ("Touton", "https://touton.com", 1),
    ("UN-Habitat", "https://unhabitat.org/", 3),
    ("Vitamin Angel Alliance", "https://vitaminangels.org", 1),
    ("Viva con Agua Schweiz", "", 1),
    ("Viva con Agua Österreich - Leben mit Wasser", "https://www.vivaconagua.at", 1),
    ("Vivamos", "http://vivamosmejor.ch", 3),
    ("WASTE", "https://www.waste.nl", 2),
    ("Yuup", "https://www.yuup.co", 2),
    ("action medeor", "https://medeor.de", 1),
    ("adelphi Research gGmbH", "https://adelphi.de", 1),
    ("arche noVa - Initiative für Menschen in Not e.V.", "", 2),
    ("ena", "https://www.ena-schweiz.ch", 1),
    ("gebana AG", "https://www.gebana.com", 1),
    ("missio - Internationales Katholisches Missionswerk e.V.", "", 1),
    ("nph Kinderhilfe Lateinamerika", "https://www.nph-kinderhilfe.org", 1),
    ("plan:g - Partnership for global health", "https://plan-g.at", 1),
    ("raha", "https://rahmanyousefalizadeh.blogfa.com/", 1),
    ("urbaMonde", "https://www.urbamonde.org", 2),
]

# Test and demo rows that exist in the export but are not real clients. Kept explicit so a
# future re-export makes it obvious what was dropped and why.
EXCLUDED_ROSTER_NAMES = {
    "clinet org.",
    "kiwi test",
    "test client",
    "the plant and people company (demo)",
}

# Ordinary words that happen to be a client's name or domain label. WASTE and Help are real
# organisations, but matching them loosely would tag any tender mentioning waste disposal or
# help desks. Such entries are matched only on their exact full name.
GENERIC_NAME_WORDS = {
    "waste",
    "help",
    "care",
    "aid",
    "world",
    "global",
    "plan",
    "shared",
    "research",
    "foundation",
}

# Trailing legal-form tokens removed before comparing organisation names.
LEGAL_SUFFIX_TOKENS = {
    "e", "v", "ev", "eg", "ag", "sa", "nv", "bv", "kg", "co", "aps", "asbl",
    "gmbh", "ggmbh", "inc", "ltd", "llc", "plc", "org",
}

# Organisation families: national chapters and sibling entities of the same parent brand.
# A tender from any of these is one warm introduction away when Fairpicture already works
# with another member of the same family.
ORG_FAMILIES: dict[str, list[str]] = {
    "Caritas": ["caritas"],
    "Viva con Agua": ["viva con agua"],
    "missio": ["missio"],
    "Cartier": ["cartier"],
    "Fairtrade": ["fairtrade", "commerce equitable", "max havelaar"],
    "Red Cross / Red Crescent": [
        "rotes kreuz", "red cross", "croix rouge", "red crescent", "ifrc", "icrc",
    ],
    "Terre des hommes": ["terre des hommes"],
    "SOS Children's Villages": ["sos kinderdorf", "sos children", "sos childrens villages"],
    "Welthungerhilfe": ["welthungerhilfe"],
    "Save the Children": ["save the children"],
    "Plan International": ["plan international"],
    "CARE": ["care international", "care osterreich", "care deutschland", "care usa", "care uk"],
    "HelpAge": ["helpage", "age international"],
    "Mennonite Central Committee": ["mennonite central committee"],
    "International Trade Centre": ["international trade cent", "intracen"],
    "Naturland": ["naturland"],
    "Fauna & Flora": ["fauna and flora", "fauna flora"],
    "Swiss Re": ["swiss re"],
    "Novo Nordisk": ["novo nordisk"],
    "Fondation Hirondelle": ["fondation hirondelle"],
    "Brot für die Welt / Diakonie": ["brot fur die welt", "diakonie"],
    "Don Bosco": ["don bosco"],
    "Médecins Sans Frontières": ["medecins sans frontieres", "doctors without borders", "msf"],
}

# Umbrella bodies and alliances. Members share vendor lists and references, so a tender from
# any member is warm when Fairpicture already works with another member of the same network.
NETWORKS: dict[str, list[str]] = {
    "Disasters Emergency Commission": [
        "disasters emergency commission", "action against hunger", "aktion gegen den hunger",
        "actionaid", "age international", "british red cross", "cafod", "care international",
        "christian aid", "concern worldwide", "international rescue committee",
        "islamic relief", "oxfam", "plan international", "save the children", "tearfund",
        "world vision",
    ],
    "ACT Alliance": [
        "act alliance", "brot fur die welt", "christian aid", "danchurchaid",
        "norwegian church aid", "finn church aid", "diakonie katastrophenhilfe",
        "lutheran world federation", "canadian lutheran world relief", "church of sweden",
        "icco", "heks", "eper",
    ],
    "Caritas Internationalis": [
        "caritas", "cafod", "catholic relief services", "secours catholique", "trocaire",
        "cordaid",
    ],
    "Fairtrade International": [
        "fairtrade", "commerce equitable", "max havelaar", "fairtrade africa",
        "fairtrade foundation",
    ],
    "Red Cross / Red Crescent Movement": [
        "ifrc", "icrc", "red cross", "rotes kreuz", "croix rouge", "red crescent",
    ],
}

# Trailing qualifiers that mark a roster entry as a national office rather than a parent body.
COUNTRY_OFFICE_SUFFIXES = {
    "osterreich", "austria", "schweiz", "suisse", "svizzera", "switzerland",
    "deutschland", "germany", "viet nam", "vietnam", "u s", "usa", "uk",
    "france", "nederland", "netherlands", "canada", "australia", "india",
}

TIER_CLIENT = "client"
TIER_FAMILY = "family"
TIER_NETWORK = "network"
TIER_NEW = "new"


def get_client_warmth(organization: Any, link: Any = "") -> dict[str, Any]:
    """Score how warm an opportunity's issuing organisation is for Fairpicture."""
    canonical = canonical_org_name(organization)
    domain = extract_domain_label(link)

    if not canonical and not domain:
        return build_warmth(
            TIER_NEW,
            "New contact",
            0,
            None,
            None,
            None,
            "No issuing organisation was available for client matching.",
        )

    client = match_roster_client(canonical, domain)
    if client:
        score = 90 + min(client["projectCount"], 10)
        return build_warmth(
            TIER_CLIENT,
            "Client",
            score,
            client,
            None,
            None,
            build_client_summary(client),
        )

    family = match_family(canonical)
    if family:
        return build_warmth(
            TIER_FAMILY,
            "Sister org",
            60,
            None,
            family,
            None,
            build_group_summary(family, "is part of the {name} family"),
        )

    network = match_network(canonical)
    if network:
        return build_warmth(
            TIER_NETWORK,
            "Network",
            35,
            None,
            None,
            network,
            build_group_summary(network, "belongs to {name}"),
        )

    return build_warmth(
        TIER_NEW,
        "New contact",
        0,
        None,
        None,
        None,
        f"No existing Fairpicture relationship was found for {display_organization(organization)}.",
    )


def match_roster_client(canonical: str, domain: str) -> dict[str, Any] | None:
    if domain:
        record = ROSTER_BY_DOMAIN.get(domain)
        if record:
            return record

    if not canonical:
        return None

    record = ROSTER_BY_NAME.get(canonical)
    if record:
        return record

    matches = [
        candidate
        for candidate in ROSTER
        if candidate["matchable"] and contains_phrase(canonical, candidate["canonical"])
    ]
    if matches:
        return max(matches, key=lambda item: (len(item["canonical"]), item["projectCount"]))

    # Tenders name the buyer by its everyday abbreviation ("MSF") or as a country office
    # ("Welthungerhilfe Liberia"); neither contains the roster's full legal name.
    alias_matches = [
        (alias, candidate)
        for alias, candidate in ROSTER_ALIASES
        if contains_phrase(canonical, alias)
    ]
    if not alias_matches:
        return None
    alias, candidate = max(alias_matches, key=lambda item: (len(item[0]), item[1]["projectCount"]))
    return {**candidate, "matchedAlias": alias}


def match_family(canonical: str) -> dict[str, Any] | None:
    if not canonical:
        return None
    for family_name, patterns in ORG_FAMILIES.items():
        if not any(contains_phrase(canonical, pattern) for pattern in patterns):
            continue
        clients = FAMILY_CLIENTS.get(family_name) or []
        if clients:
            return {"name": family_name, "clients": clients}

    # A different office of a body we know only through one national office. Warm, but not
    # the same as having worked with this buyer.
    chapter_matches = [
        (alias, candidate)
        for alias, candidate in ROSTER_CHAPTER_ALIASES
        if contains_phrase(canonical, alias)
    ]
    if chapter_matches:
        alias, candidate = max(
            chapter_matches, key=lambda item: (len(item[0]), item[1]["projectCount"])
        )
        return {"name": candidate["name"].split(" ")[0] or alias, "clients": [candidate]}
    return None


def match_network(canonical: str) -> dict[str, Any] | None:
    if not canonical:
        return None
    for network_name, patterns in NETWORKS.items():
        if not any(contains_phrase(canonical, pattern) for pattern in patterns):
            continue
        clients = NETWORK_CLIENTS.get(network_name) or []
        if clients:
            return {"name": network_name, "clients": clients}
    return None


def build_warmth(
    tone: str,
    label: str,
    score: int,
    client: dict[str, Any] | None,
    family: dict[str, Any] | None,
    network: dict[str, Any] | None,
    summary: str,
) -> dict[str, Any]:
    return {
        "label": label,
        "tone": tone,
        "score": score,
        "evidence": {
            "client": client,
            "family": family,
            "network": network,
        },
        "summary": summary,
    }


def build_client_summary(client: dict[str, Any]) -> str:
    count = client["projectCount"]
    return (
        f"{client['name']} is an existing Fairpicture client with "
        f"{count} project{'s' if count != 1 else ''} on record."
    )


def build_group_summary(group: dict[str, Any], template: str) -> str:
    clients = group["clients"]
    names = ", ".join(client["name"] for client in clients[:3])
    remainder = len(clients) - 3
    if remainder > 0:
        names = f"{names} and {remainder} more"
    relation = template.format(name=group["name"])
    return f"This organisation {relation}, where Fairpicture already works with {names}."


def canonical_org_name(value: Any) -> str:
    key = normalize_lookup_key(value)
    if not key or key in {"n a", "na", "unknown"}:
        return ""
    tokens = key.split()
    while tokens and tokens[-1] in LEGAL_SUFFIX_TOKENS:
        tokens.pop()
    return " ".join(tokens)


def normalize_lookup_key(value: Any) -> str:
    text = str(value or "").strip()
    if not text:
        return ""
    normalized = unicodedata.normalize("NFKD", text).encode("ascii", "ignore").decode("ascii")
    normalized = normalized.replace("&", " and ")
    normalized = re.sub(r"[^a-zA-Z0-9]+", " ", normalized).strip().lower()
    return re.sub(r"\s+", " ", normalized)


def contains_phrase(haystack: str, needle: str) -> bool:
    """Whole-word containment so `care` never matches `healthcare`."""
    if not haystack or not needle:
        return False
    return re.search(rf"(?<![a-z0-9]){re.escape(needle)}(?![a-z0-9])", haystack) is not None


def extract_domain_label(value: Any) -> str:
    text = str(value or "").strip()
    if not text:
        return ""
    if "//" not in text:
        text = f"https://{text}"
    host = (parse.urlparse(text).hostname or "").lower()
    if not host:
        return ""
    host = host.removeprefix("www.")
    parts = [part for part in host.split(".") if part]
    if len(parts) < 2:
        return ""
    label = parts[-2]
    if label in {"org", "co", "ac", "gov", "com", "net"} and len(parts) >= 3:
        label = parts[-3]
    return label


def display_organization(value: Any) -> str:
    text = str(value or "").strip()
    return text if text and text.upper() != "N/A" else "this organisation"


def build_roster_record(name: str, website: str, project_count: int) -> dict[str, Any]:
    canonical = canonical_org_name(name)
    return {
        "name": name,
        "website": website,
        "projectCount": project_count,
        "canonical": canonical,
        "domain": extract_domain_label(website),
        # Short or highly generic names are matched exactly to avoid false positives.
        "matchable": len(canonical) >= 5 and canonical not in GENERIC_NAME_WORDS,
    }


ROSTER: list[dict[str, Any]] = [
    build_roster_record(name, website, project_count)
    for name, website, project_count in RAW_CLIENT_ROSTER
    if name.strip().lower() not in EXCLUDED_ROSTER_NAMES
]

ROSTER_BY_NAME: dict[str, dict[str, Any]] = {}
for record in ROSTER:
    existing = ROSTER_BY_NAME.get(record["canonical"])
    if not existing or record["projectCount"] > existing["projectCount"]:
        ROSTER_BY_NAME[record["canonical"]] = record

ROSTER_BY_DOMAIN: dict[str, dict[str, Any]] = {}
for record in ROSTER:
    if not record["domain"]:
        continue
    existing = ROSTER_BY_DOMAIN.get(record["domain"])
    if not existing or record["projectCount"] > existing["projectCount"]:
        ROSTER_BY_DOMAIN[record["domain"]] = record


def build_alias(domain_label: str) -> str:
    """Turn a domain label into a matchable phrase: `brot-fuer-die-welt` -> `brot fuer die welt`."""
    alias = normalize_lookup_key(domain_label.replace("-", " "))
    if len(alias) < 3 or alias in GENERIC_NAME_WORDS:
        return ""
    return alias


def has_country_suffix(canonical: str) -> bool:
    """True for roster entries that name a national office, e.g. `ILO Viet Nam`.

    The direction matters. A headquarters relationship reasonably covers its country
    offices, so `Deutsche Welthungerhilfe` may expand to `Welthungerhilfe Liberia`. The
    reverse does not: working with ILO Viet Nam says nothing about ILO Colombo, so such an
    entry must not expand its alias to every other office of the same body.
    """
    return any(canonical.endswith(" " + suffix) for suffix in COUNTRY_OFFICE_SUFFIXES)


# (alias, roster record) pairs, longest alias first so the most specific match wins.
ROSTER_ALIASES: list[tuple[str, dict[str, Any]]] = []
ROSTER_CHAPTER_ALIASES: list[tuple[str, dict[str, Any]]] = []
for record in ROSTER:
    alias = build_alias(record["domain"])
    # An alias that merely repeats the canonical name adds nothing over substring matching.
    if not alias or alias == record["canonical"]:
        continue
    if has_country_suffix(record["canonical"]):
        ROSTER_CHAPTER_ALIASES.append((alias, record))
    else:
        ROSTER_ALIASES.append((alias, record))
ROSTER_ALIASES.sort(key=lambda item: len(item[0]), reverse=True)
ROSTER_CHAPTER_ALIASES.sort(key=lambda item: len(item[0]), reverse=True)


def collect_group_clients(groups: dict[str, list[str]]) -> dict[str, list[dict[str, Any]]]:
    collected: dict[str, list[dict[str, Any]]] = {}
    for group_name, patterns in groups.items():
        members = [
            record
            for record in ROSTER
            if any(contains_phrase(record["canonical"], pattern) for pattern in patterns)
        ]
        deduped = {record["name"]: record for record in members}
        collected[group_name] = sorted(
            deduped.values(), key=lambda item: item["projectCount"], reverse=True
        )
    return collected


FAMILY_CLIENTS = collect_group_clients(ORG_FAMILIES)
NETWORK_CLIENTS = collect_group_clients(NETWORKS)
