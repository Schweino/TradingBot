"""
Append daily post-mortem entries to a Google Doc via the Docs API.

Usage:
    from gdocs_writer import append_postmortem
    append_postmortem(date_iso='2026-04-27', body_text='...long report text...')

The doc gets each new day INSERTED AT THE TOP, under a heading line, so the
most recent post-mortem is always the first thing you see when you open the
doc. Older days flow down the page.

Auth: service account JSON key at C:\\xampp\\htdocs\\Claude\\.gdocs_service_account.json.
The service account email must have Editor access on the target doc.
"""
from __future__ import annotations
import os
from typing import Optional

from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

CREDS_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                          '.gdocs_service_account.json')
SCOPES = ['https://www.googleapis.com/auth/documents']

# Default doc ID — the rolling "Mock Trader — Daily Post-Mortems" doc
DEFAULT_DOC_ID = '1LwAnP0FobVB4yOrgBp0qHH79P-3NFv_ry0MAQ4XSWRE'
STRONG_RECOMMENDATION_MARKERS = (
    'Suggested action items',
    '**STRONGLY CONSIDER THE BELOW CHANGES**',
    '**SUGGESTED CHANGE BASED ON EVIDENCE**',
)
HUMAN_SUMMARY_MARKERS = (
    'Daily human summary',
)
TRACKING_HYPOTHESIS_MARKERS = (
    'Tracking hypotheses',
)
POSTMORTEM_FONT_FAMILY = 'Arial'
POSTMORTEM_BODY_FONT_SIZE_PT = 11
SECTION_HEADING_MARKERS = (
    'Learning trust:',
    'Daily human summary',
    'What worked',
    "What didn't work",
    'Data / ops notes',
    'Suggested action items',
    'Tracking hypotheses',
    "Today's main evidence",
    'Active watchlist',
    'Local detail artifacts',
)


def _service():
    creds = service_account.Credentials.from_service_account_file(
        CREDS_PATH, scopes=SCOPES)
    return build('docs', 'v1', credentials=creds, cache_discovery=False)


def _line_end(content: str, start: int) -> int:
    end = content.find('\n', start)
    return len(content) if end < 0 else end


def _line_style_request(content: str, marker: str, text_style: dict,
                        fields: str, base_index: int = 1,
                        paragraph_style: Optional[dict] = None,
                        paragraph_fields: Optional[str] = None) -> list[dict]:
    start = content.find(marker)
    if start < 0:
        return []
    end = _line_end(content, start)
    requests = [
        {'updateTextStyle': {
            'range': {'startIndex': base_index + start, 'endIndex': base_index + end},
            'textStyle': text_style,
            'fields': fields,
        }},
    ]
    if paragraph_style and paragraph_fields:
        requests.append({'updateParagraphStyle': {
            'range': {'startIndex': base_index + start, 'endIndex': base_index + end},
            'paragraphStyle': paragraph_style,
            'fields': paragraph_fields,
        }})
    return requests


def _strong_recommendation_style_requests(content: str, base_index: int = 1) -> list[dict]:
    marker = next((m for m in STRONG_RECOMMENDATION_MARKERS if content.find(m) >= 0), None)
    if not marker:
        return []
    requests = _line_style_request(
        content,
        marker,
        {
            'bold': True,
            'underline': True,
            'foregroundColor': {
                'color': {'rgbColor': {'red': 0.80, 'green': 0.00, 'blue': 0.00}},
            },
        },
        'bold,underline,foregroundColor',
        base_index=base_index,
        paragraph_style={'namedStyleType': 'HEADING_2'},
        paragraph_fields='namedStyleType',
    )
    start = content.find(marker)
    end_candidates = [
        content.find(next_marker, start + 1)
        for next_marker in TRACKING_HYPOTHESIS_MARKERS + ('Local detail artifacts',)
    ]
    end_candidates = [idx for idx in end_candidates if idx > start]
    end = min(end_candidates) if end_candidates else _line_end(content, start)
    requests.append({'updateTextStyle': {
        'range': {'startIndex': base_index + start, 'endIndex': base_index + end},
        'textStyle': {
            'foregroundColor': {
                'color': {'rgbColor': {'red': 0.80, 'green': 0.00, 'blue': 0.00}},
            },
        },
        'fields': 'foregroundColor',
    }})
    return requests


def _human_summary_style_requests(content: str, base_index: int = 1) -> list[dict]:
    marker = next((m for m in HUMAN_SUMMARY_MARKERS if content.find(m) >= 0), None)
    if not marker:
        return []
    return _line_style_request(
        content,
        marker,
        {
            'bold': True,
            'underline': True,
            'foregroundColor': {
                'color': {'rgbColor': {'red': 0.00, 'green': 0.20, 'blue': 0.80}},
            },
        },
        'bold,underline,foregroundColor',
        base_index=base_index,
        paragraph_style={'namedStyleType': 'HEADING_2'},
        paragraph_fields='namedStyleType',
    )


def _tracking_hypothesis_style_requests(content: str, base_index: int = 1) -> list[dict]:
    marker = next((m for m in TRACKING_HYPOTHESIS_MARKERS if content.find(m) >= 0), None)
    if not marker:
        return []
    return _line_style_request(
        content,
        marker,
        {
            'bold': True,
            'underline': True,
            'foregroundColor': {
                'color': {'rgbColor': {'red': 0.00, 'green': 0.00, 'blue': 0.00}},
            },
        },
        'bold,underline,foregroundColor',
        base_index=base_index,
        paragraph_style={'namedStyleType': 'HEADING_2'},
        paragraph_fields='namedStyleType',
    )


def _section_heading_style_requests(content: str, base_index: int = 1) -> list[dict]:
    requests = []
    for marker in SECTION_HEADING_MARKERS:
        if marker in HUMAN_SUMMARY_MARKERS or marker in STRONG_RECOMMENDATION_MARKERS or marker in TRACKING_HYPOTHESIS_MARKERS:
            continue
        requests.extend(_line_style_request(
            content,
            marker,
            {'bold': True, 'underline': True},
            'bold,underline',
            base_index=base_index,
        ))
    return requests


def _insert_section(svc, doc_id: str, title: str, body_text: str) -> dict:
    heading = f'{title}\n'
    body = body_text.rstrip() + '\n\n'
    content = heading + body
    heading_len = len(heading)
    total_len = len(content)
    requests = [
        {'insertText': {'location': {'index': 1}, 'text': content}},
        {'updateParagraphStyle': {
            'range': {'startIndex': 1, 'endIndex': 1 + heading_len},
            'paragraphStyle': {'namedStyleType': 'HEADING_1'},
            'fields': 'namedStyleType',
        }},
        {'updateParagraphStyle': {
            'range': {'startIndex': 1 + heading_len, 'endIndex': 1 + total_len},
            'paragraphStyle': {'namedStyleType': 'NORMAL_TEXT'},
            'fields': 'namedStyleType',
        }},
        {'updateTextStyle': {
            'range': {'startIndex': 1, 'endIndex': 1 + total_len},
            'textStyle': {
                'weightedFontFamily': {'fontFamily': POSTMORTEM_FONT_FAMILY},
            },
            'fields': 'weightedFontFamily',
        }},
        {'updateTextStyle': {
            'range': {'startIndex': 1 + heading_len, 'endIndex': 1 + total_len},
            'textStyle': {
                'fontSize': {'magnitude': POSTMORTEM_BODY_FONT_SIZE_PT, 'unit': 'PT'},
            },
            'fields': 'fontSize',
        }},
    ]
    requests.extend(_human_summary_style_requests(content, base_index=1))
    requests.extend(_strong_recommendation_style_requests(content, base_index=1))
    requests.extend(_tracking_hypothesis_style_requests(content, base_index=1))
    requests.extend(_section_heading_style_requests(content, base_index=1))
    resp = svc.documents().batchUpdate(
        documentId=doc_id, body={'requests': requests}).execute()
    return {'inserted': total_len, 'replies': len(resp.get('replies') or [])}


def _find_section(doc: dict, title: str) -> Optional[tuple[int, int]]:
    body = doc.get('body') or {}
    content = body.get('content') or []
    start = end = None
    for idx, item in enumerate(content):
        para = item.get('paragraph') or {}
        text = ''.join(
            elem.get('textRun', {}).get('content', '')
            for elem in para.get('elements') or []
        ).strip()
        style = (para.get('paragraphStyle') or {}).get('namedStyleType')
        if text == title and style == 'HEADING_1':
            start = item.get('startIndex')
            next_start = None
            for nxt in content[idx + 1:]:
                npara = nxt.get('paragraph') or {}
                nstyle = (npara.get('paragraphStyle') or {}).get('namedStyleType')
                if nstyle == 'HEADING_1':
                    next_start = nxt.get('startIndex')
                    break
            end = next_start or body.get('content', [{}])[-1].get('endIndex')
            break
    if start is None or end is None or end <= start:
        return None
    return start, end


def append_postmortem(date_iso: str, body_text: str,
                      doc_id: Optional[str] = None,
                      section_title: Optional[str] = None,
                      mode: str = 'append') -> dict:
    """Insert a dated section at the TOP of the doc.

    Layout:
        ─────────────────────────
        2026-04-27               ← HEADING_1
        (blank line)
        <body_text>              ← NORMAL_TEXT
        (blank line)

    Returns a small dict summarizing the insert (revision id, char count).
    Raises HttpError on API failure (caller decides whether to swallow).
    """
    doc_id = doc_id or DEFAULT_DOC_ID
    svc = _service()
    title = section_title or date_iso

    if mode == 'upsert':
        doc = svc.documents().get(documentId=doc_id).execute()
        existing = _find_section(doc, title)
        if existing:
            start, end = existing
            svc.documents().batchUpdate(documentId=doc_id, body={'requests': [
                {'deleteContentRange': {'range': {'startIndex': start, 'endIndex': end}}},
            ]}).execute()

    result = _insert_section(svc, doc_id, title, body_text)
    return {
        'doc_id':     doc_id,
        'inserted':   result['inserted'],
        'replies':    result['replies'],
        'date_iso':   date_iso,
        'section_title': title,
        'mode': mode,
    }


def append_postmortem_addendum(date_iso: str, body_text: str,
                               doc_id: Optional[str] = None,
                               mode: str = 'upsert') -> dict:
    return append_postmortem(
        date_iso=date_iso,
        body_text=body_text,
        doc_id=doc_id,
        section_title=f'{date_iso} ADDENDUM',
        mode=mode,
    )


if __name__ == '__main__':
    # Smoke test — insert a tiny placeholder so you can verify the doc updates.
    sample = (
        "Smoke test from gdocs_writer.py.\n"
        "If you see this, the service account auth, doc sharing, and Docs API\n"
        "all work end-to-end. Safe to delete this section after verifying.\n"
    )
    result = append_postmortem(date_iso='SMOKE-TEST', body_text=sample)
    print('insert result:', result)
    print('open the doc to verify the new SMOKE-TEST section appears at top.')
