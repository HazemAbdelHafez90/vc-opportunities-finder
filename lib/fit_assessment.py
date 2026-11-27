"""Experimental, explainable fit assessment. Does not replace stored scores."""
import re


def assess(item):
    raw = item.get("raw_payload") or item
    title = str(item.get("title") or raw.get("title") or "")
    scope = str(raw.get("scope") or "")
    docs = raw.get("documents") or []
    evidence = []
    for doc in docs:
        if doc.get("status") != "read":
            continue
        name = doc.get("name") or "Document"
        if re.search(r"(?:supplier|vendor|bidder).{0,30}(?:guide|manual|instruction)|(?:guide|manual|instruction).{0,30}(?:supplier|vendor|bidder)", name, re.I):
            continue
        for excerpt in doc.get("fitExcerpts") or []:
            if excerpt.get("text"):
                evidence.append({"source": name, "page": excerpt.get("page"), "text": excerpt["text"]})
    text = " ".join([title, str(item.get("type") or ""), scope] + [e["text"] for e in evidence]).lower()
    def has(pattern):
        return bool(re.search(pattern, text, re.I))
    visual = has(r"\b(photo(?:graph\w*)?|videograph\w*|video production|filmmak\w*|documentary|visual storytelling|film production)\b")
    procurement = has(r"\b(rfp|rfq|tender|agency|company|firm|service provider|production services|request for (?:proposal|quotation))\b")
    individual = has(r"\b(individual consultant|national consultant|external collaborator|internship|vacancy|staff position|job post)\b")
    excluded = has(r"\b(video conferencing|videoconference|surveillance|supply of cameras?|streaming platform|film restoration|music promotion)\b")
    animation = has(r"\b(animation|animated|motion graphics)\b")
    social = has(r"\b(social media management|manage social media|digital campaigns)\b")
    if excluded:
        production, reason = "weak", "Possible equipment, surveillance, distribution or promotion scope; review the deliverables."
    elif visual and (animation or social):
        production, reason = "possible", "Visual work appears alongside animation or social media; check the main deliverables."
    elif visual and procurement and not individual:
        production, reason = "possible", "Photography or video appears in a procurement or supplier opportunity."
    elif visual:
        production, reason = "possible", "Photography or video is mentioned; confirm the scope and eligibility."
    else:
        production, reason = "unclear", "No clear live-action production evidence in the available text."
    eligibility = "individual" if individual and not procurement else "unclear"
    consent = has(r"\b(visual consent|photo consent|image consent|model release|image rights|consent to (?:film|photograph))\b")
    product = has(r"\b(consent (?:management|platform|system|software|records|tracking))\b")
    fairconsent = "direct" if consent and product else "potential" if consent else "unclear"
    meaningful_scope = bool(scope.strip()) or bool(evidence)
    incomplete = any(d.get("status") != "read" or d.get("truncated") or d.get("analysisTruncated") for d in docs)
    confidence = "partial" if meaningful_scope and not incomplete else "insufficient"
    return {"visualProduction": production, "visualReason": reason, "eligibility": eligibility,
            "fairconsent": fairconsent, "confidence": confidence, "documentEvidence": evidence[:5]}
