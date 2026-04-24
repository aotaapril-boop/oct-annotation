"""
OCT Annotation Tool — Cloud version
Images: Google Drive folder
Annotations: per-annotator Google Sheets (auto-created in same Drive folder)
"""

import streamlit as st
import json
import io
import base64
import time
from datetime import datetime
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build as gapi_build
from googleapiclient.http import MediaIoBaseDownload
import gspread

# ─── Config ──────────────────────────────────────────────────

st.set_page_config(page_title="OCT Annotation", layout="wide")

sidebar_width = st.session_state.get("sidebar_w", 500)

st.markdown(f"""
<style>
/* Sidebar: fixed width controlled by slider */
[data-testid="stSidebar"] {{
    min-width: {sidebar_width}px !important;
    max-width: {sidebar_width}px !important;
    width: {sidebar_width}px !important;
}}
[data-testid="stSidebar"] > div:first-child {{
    width: {sidebar_width}px !important;
}}
.block-container {{ padding-top: 2.5rem; padding-bottom: 0rem; }}
h3 {{ margin-top: 0.2rem; margin-bottom: 0.1rem; font-size: 1.05rem; }}
hr {{ margin-top: 0.2rem; margin-bottom: 0.2rem; }}
[data-testid="stCheckbox"] {{ margin-bottom: -0.8rem; }}
[data-testid="stRadio"] > div {{ margin-top: -0.5rem; }}
.fovea-block {{ background-color: #f0f4ff; border-radius: 8px; padding: 0.5rem 0.8rem; margin-bottom: 0.3rem; }}
.extrafovea-block {{ background-color: #fff8f0; border-radius: 8px; padding: 0.5rem 0.8rem; margin-bottom: 0.3rem; }}
/* === Mobile only === */
@media (min-width: 768px) {{
    .mobile-oct-image {{ display: none !important; }}
}}
@media (max-width: 767px) {{
    .mobile-oct-image {{
        position: fixed !important;
        top: 3rem; left: 0; width: 100vw;
        z-index: 99999;
        background: #0e1117;
        box-shadow: 0 2px 8px rgba(0,0,0,0.5);
    }}
    .mobile-oct-image img {{
        width: 100%; max-height: 28vh;
        object-fit: contain; display: block;
    }}
    .mobile-oct-info {{
        color: #fafafa; font-size: 12px;
        text-align: center; padding: 2px 4px;
    }}
    .block-container {{ padding-top: calc(28vh + 5rem) !important; }}
    /* Sidebar overlay must be above fixed image so it can be closed */
    [data-testid="stSidebar"] {{ z-index: 999999 !important; }}
}}
</style>
""", unsafe_allow_html=True)

SCOPES = [
    "https://www.googleapis.com/auth/drive",
    "https://www.googleapis.com/auth/spreadsheets",
]

# ─── Google API helpers ──────────────────────────────────────

@st.cache_resource
def get_credentials():
    info = st.secrets["gcp_service_account"]
    creds = Credentials.from_service_account_info(dict(info), scopes=SCOPES)
    return creds

@st.cache_resource
def get_drive_service():
    return gapi_build("drive", "v3", credentials=get_credentials())

@st.cache_resource
def get_gspread_client():
    return gspread.authorize(get_credentials())

DRIVE_IMAGES_FOLDER_ID = st.secrets["drive_images_folder_id"]
DRIVE_SHEETS_FOLDER_ID = st.secrets["drive_sheets_folder_id"]

# ─── Google Drive: list & fetch images ───────────────────────

@st.cache_data(ttl=300)
def get_image_list():
    """List image files in the Google Drive folder, sorted by name."""
    service = get_drive_service()
    query = f"'{DRIVE_IMAGES_FOLDER_ID}' in parents and mimeType contains 'image/' and trashed=false"
    results = service.files().list(
        q=query, fields="files(id,name)", orderBy="name", pageSize=1000,
        supportsAllDrives=True, includeItemsFromAllDrives=True,
    ).execute()
    return [(f["name"], f["id"]) for f in results.get("files", [])]

@st.cache_data(ttl=3600, max_entries=20)
def download_image(file_id):
    """Download image bytes from Google Drive. Cached 1h, max 20 images in memory."""
    service = get_drive_service()
    request = service.files().get_media(fileId=file_id, supportsAllDrives=True)
    buf = io.BytesIO()
    downloader = MediaIoBaseDownload(buf, request)
    done = False
    while not done:
        _, done = downloader.next_chunk()
    buf.seek(0)
    return buf.read()

def preload_nearby_images(current_idx, image_ids_list, count=2):
    """Preload next few images into cache."""
    for i in range(1, count + 1):
        next_idx = current_idx + i
        if next_idx < len(image_ids_list):
            name, fid = image_ids_list[next_idx]
            download_image(fid)  # result is cached

# ─── Per-annotator Google Sheets ─────────────────────────────

HEADER_ROW = [
    "image", "annotator", "saved_at",
    "scan_type", "scan_location", "quality",
    "fovea_VRI", "fovea_intraretinal", "fovea_outer_retina",
    "extrafovea_VRI", "extrafovea_intraretinal", "extrafovea_outer_retina",
    "extrafovea_choroid",
    "negative_findings",
    "L2_abnormality", "L3_management", "caption", "auto_caption",
    "raw_json",
]

def _api_call_with_retry(func, retries=3):
    """Retry API calls on transient errors."""
    for attempt in range(retries):
        try:
            return func()
        except Exception as e:
            if attempt < retries - 1:
                time.sleep(2 ** attempt)
                continue
            raise

def _get_or_create_sheet(annotator):
    """Find or create a Google Sheet for the annotator in the sheets folder."""
    # Cache worksheet object in session_state
    cache_key = f"_ws_cache_{annotator}"
    if cache_key in st.session_state:
        return st.session_state[cache_key]

    sheet_name = f"OCT_annotations_{annotator}"
    service = get_drive_service()

    query = (
        f"'{DRIVE_SHEETS_FOLDER_ID}' in parents "
        f"and name='{sheet_name}' "
        f"and mimeType='application/vnd.google-apps.spreadsheet' "
        f"and trashed=false"
    )
    results = _api_call_with_retry(lambda: service.files().list(
        q=query, fields="files(id,name)",
        supportsAllDrives=True, includeItemsFromAllDrives=True,
    ).execute())
    files = results.get("files", [])

    gc = get_gspread_client()

    if files:
        sh = _api_call_with_retry(lambda: gc.open_by_key(files[0]["id"]))
    else:
        file_metadata = {
            "name": sheet_name,
            "mimeType": "application/vnd.google-apps.spreadsheet",
            "parents": [DRIVE_SHEETS_FOLDER_ID],
        }
        created = _api_call_with_retry(lambda: service.files().create(
            body=file_metadata, fields="id",
            supportsAllDrives=True,
        ).execute())
        sh = _api_call_with_retry(lambda: gc.open_by_key(created["id"]))

    # Use the first sheet (Sheet1)
    ws = sh.sheet1

    try:
        first_cell = ws.acell("A1").value
    except Exception:
        first_cell = None
    if first_cell != "image":
        ws.update("A1", [HEADER_ROW], value_input_option="RAW")

    st.session_state[cache_key] = ws
    return ws

def _load_all_annotations(annotator):
    """Load all annotations from sheet into session_state cache."""
    cache_key = f"_ann_cache_{annotator}"
    if cache_key not in st.session_state:
        ws = _get_or_create_sheet(annotator)
        try:
            records = _api_call_with_retry(lambda: ws.get_all_records())
        except Exception:
            records = []
        ann_dict = {}
        for rec in records:
            img = rec.get("image", "")
            raw = rec.get("raw_json", "")
            if img and raw:
                try:
                    ann_dict[img] = json.loads(raw)
                except json.JSONDecodeError:
                    pass
        st.session_state[cache_key] = ann_dict
    return st.session_state[cache_key]

def load_annotation(image_name, annotator):
    """Load annotation from cache (fetched once per session)."""
    return _load_all_annotations(annotator).get(image_name, {})

def save_annotation(data, image_name, annotator):
    """Save annotation to the annotator's Google Sheet (upsert)."""
    data["_meta"] = {
        "image": image_name,
        "annotator": annotator,
        "saved_at": datetime.now().isoformat(),
    }
    ws = _get_or_create_sheet(annotator)
    all_values = _api_call_with_retry(lambda: ws.get_all_values())

    target_row = None
    for i, row_vals in enumerate(all_values):
        if i == 0:
            continue
        if len(row_vals) >= 1 and row_vals[0] == image_name:
            target_row = i + 1
            break

    flat = flatten_to_row(data)
    row_data = [flat.get(h, "") for h in HEADER_ROW]

    if target_row:
        end_col = chr(64 + len(HEADER_ROW)) if len(HEADER_ROW) <= 26 else "T"
        _api_call_with_retry(lambda: ws.update(
            f"A{target_row}:{end_col}{target_row}", [row_data], value_input_option="RAW"
        ))
    else:
        _api_call_with_retry(lambda: ws.append_row(row_data, value_input_option="RAW"))

    # Update local cache
    ann_cache_key = f"_ann_cache_{annotator}"
    if ann_cache_key in st.session_state:
        st.session_state[ann_cache_key][image_name] = data

def get_done_set(annotator):
    """Return set of image names that have been annotated."""
    return set(_load_all_annotations(annotator).keys())

# ─── Flatten ─────────────────────────────────────────────────

def flatten_to_row(data):
    meta = data.get("_meta", {})
    row = {
        "image": meta.get("image", ""),
        "annotator": meta.get("annotator", ""),
        "saved_at": meta.get("saved_at", ""),
        "scan_type": data.get("scan_type", ""),
        "scan_location": data.get("scan_loc", ""),
        "quality": data.get("quality", ""),
    }
    loc_findings = data.get("L1_loc_findings", {})
    for loc_key, loc_data in loc_findings.items():
        loc_short = "fovea" if "Fovea" in loc_key else "extrafovea"
        if isinstance(loc_data, dict):
            merged_cats = {}
            for cat_name, findings in loc_data.items():
                base = cat_name.replace("-1", "").replace("-2", "")
                if base not in merged_cats:
                    merged_cats[base] = []
                merged_cats[base].extend(findings)
            for base_cat, findings in merged_cats.items():
                if "VRI" in base_cat:
                    cat_short = "VRI"
                elif "Intraretinal" in base_cat:
                    cat_short = "intraretinal"
                elif "Outer" in base_cat:
                    cat_short = "outer_retina"
                elif "Choroid" in base_cat:
                    cat_short = "choroid"
                else:
                    cat_short = base_cat
                row[f"{loc_short}_{cat_short}"] = "; ".join(findings) if findings else ""

    row["negative_findings"] = "; ".join(data.get("L1_neg", []))
    row["L2_abnormality"] = data.get("L2", "")
    row["L3_management"] = data.get("L3_mgmt", "")

    row["caption"] = data.get("caption", "")
    row["auto_caption"] = generate_caption(data)
    row["raw_json"] = json.dumps(data, ensure_ascii=False)
    return row

# ─── Auto Caption Generation ─────────────────────────────────

# Negative finding labels as-is (no expansion)
NEG_LABELS = ["no SRF", "no IRF", "no PED", "EZ intact", "no ERM"]

# Location labels -> readable location text
LOCATION_MAP = {
    "fovea_VRI": "vitreoretinal interface at the fovea",
    "fovea_intraretinal": "intraretinal layers at the fovea",
    "fovea_outer_retina": "outer retina at the fovea",
    "extrafovea_VRI": "vitreoretinal interface in the extrafoveal area",
    "extrafovea_intraretinal": "intraretinal layers in the extrafoveal area",
    "extrafovea_outer_retina": "outer retina in the extrafoveal area",
    "extrafovea_choroid": "choroid in the extrafoveal area",
}


def _join_english_list(items):
    """Join list into English: 'a', 'a and b', 'a, b, and c'."""
    if not items:
        return ""
    if len(items) == 1:
        return items[0]
    if len(items) == 2:
        return f"{items[0]} and {items[1]}"
    return ", ".join(items[:-1]) + ", and " + items[-1]


def _expand_finding(name):
    """Return finding name as-is."""
    return name


def generate_caption(data):
    """Generate a deterministic English caption from structured annotation data.

    Rules:
    - Only use information present in the data
    - No diagnostic interpretation or inference
    - Deterministic: same input always produces same output
    """
    sentences = []

    # 1. Image quality
    quality = (data.get("quality") or "").strip().lower()
    if quality == "good":
        sentences.append("Image quality is sufficient for evaluation.")
    elif quality == "fair":
        sentences.append("Image quality is limited but adequate for evaluation.")
    elif quality == "poor":
        sentences.append("The image is not adequate for full evaluation.")

    # 2. Abnormality presence
    abnormality = (data.get("L2") or "").strip().lower()
    if abnormality == "normal":
        sentences.append("No abnormal findings are present.")
    elif abnormality == "abnormal":
        sentences.append("Abnormal findings are present.")
    elif abnormality == "uncertain":
        sentences.append("The presence of abnormality is uncertain.")

    # 3. Location + 4. Positive findings — collect per location
    loc_findings = data.get("L1_loc_findings", {})
    all_findings = []  # (location_label, [findings])
    for loc_key, loc_data in sorted(loc_findings.items()):
        if not isinstance(loc_data, dict):
            continue
        loc_short = "fovea" if "Fovea" in loc_key else "extrafovea"
        for cat_name, findings_list in sorted(loc_data.items()):
            if not findings_list:
                continue
            base = cat_name.replace("-1", "").replace("-2", "")
            if "VRI" in base:
                loc_label = f"{loc_short}_VRI"
            elif "Intraretinal" in base:
                loc_label = f"{loc_short}_intraretinal"
            elif "Outer" in base:
                loc_label = f"{loc_short}_outer_retina"
            elif "Choroid" in base:
                loc_label = f"{loc_short}_choroid"
            else:
                loc_label = loc_short
            all_findings.append((loc_label, findings_list))

    # Merge findings by location
    merged = {}
    for loc_label, findings_list in all_findings:
        if loc_label not in merged:
            merged[loc_label] = []
        merged[loc_label].extend(findings_list)

    # Build location + findings sentences (fovea before extrafovea)
    LOC_ORDER = [
        "fovea_VRI", "fovea_intraretinal", "fovea_outer_retina",
        "extrafovea_VRI", "extrafovea_intraretinal", "extrafovea_outer_retina", "extrafovea_choroid",
    ]
    ordered_keys = [k for k in LOC_ORDER if k in merged] + [k for k in merged if k not in LOC_ORDER]
    for loc_label in ordered_keys:
        findings_list = merged[loc_label]
        expanded = [_expand_finding(f) for f in findings_list if f and f != "other"]
        if not expanded:
            continue
        location_text = LOCATION_MAP.get(loc_label, loc_label.replace("_", " "))
        findings_text = _join_english_list(expanded)
        sentences.append(
            f"{findings_text[0].upper() + findings_text[1:]} {'is' if len(expanded) == 1 else 'are'} "
            f"observed in the {location_text}."
        )

    # 5. Negative findings
    neg_list = data.get("L1_neg", [])
    if neg_list:
        valid_neg = [n for n in neg_list if n and n.strip()]
        if valid_neg:
            neg_text = _join_english_list(valid_neg)
            sentences.append(f"Negative findings: {neg_text}.")

    # 6. Management
    mgmt = (data.get("L3_mgmt") or "").strip().lower()
    if mgmt == "observation":
        sentences.append("Observation is recommended.")
    elif mgmt == "further exam":
        sentences.append("Further examination is recommended.")
    elif mgmt == "treatment":
        sentences.append("Treatment is recommended.")

    return " ".join(sentences)

# ─── Findings definitions ────────────────────────────────────

FOVEA_CATEGORIES = {
    "VRI":              ["PVD", "ERM", "VMT", "VH"],
    "Intraretinal-1":   ["IRF", "hemorrhage", "retinal thickening", "tractional thickening"],
    "Intraretinal-2":   ["inner thinning", "hyperreflective foci", "hard exudates"],
    "Outer retina-1":   ["SRF", "subretinal hemorrhage", "serous PED"],
    "Outer retina-2":   ["SHRM", "EZ disruption", "outer atrophy", "drusen"],
}

EXTRAFOVEA_CATEGORIES = {
    "VRI":              ["PVD", "ERM", "VMT", "VH"],
    "Intraretinal-1":   ["IRF", "hemorrhage", "retinal thickening", "tractional thickening"],
    "Intraretinal-2":   ["inner thinning", "hyperreflective foci", "hard exudates"],
    "Outer retina-1":   ["SRF", "subretinal hemorrhage", "serous PED"],
    "Outer retina-2":   ["SHRM", "EZ disruption", "outer atrophy", "drusen"],
    "Choroid":          ["choroidal thickening", "choroidal thinning"],
}

NEG_FINDINGS = ["no SRF", "no IRF", "no PED", "EZ intact", "no ERM"]

# Mapping: positive finding -> negative to auto-deselect
POS_TO_NEG = {
    "SRF": "no SRF",
    "IRF": "no IRF",
    "serous PED": "no PED",
    "EZ disruption": "EZ intact",
    "ERM": "no ERM",
}

# ─── Image list ──────────────────────────────────────────────

images_info = get_image_list()
if not images_info:
    st.error("No images found in Google Drive folder. Check folder ID and permissions.")
    st.stop()

images = [name for name, _ in images_info]
image_ids = {name: fid for name, fid in images_info}
total = len(images)

# ─── Sidebar: image + navigation (fixed, doesn't scroll with main) ───

st.sidebar.slider("Image panel width", min_value=300, max_value=800, value=500, step=50, key="sidebar_w")
annotator = st.sidebar.text_input("Annotator name", value="default")

if not annotator or annotator.strip() == "":
    st.warning("Please enter your annotator name in the sidebar.")
    st.stop()

annotator = annotator.strip()

if "idx" not in st.session_state:
    st.session_state.idx = 0

col_p, col_n, col_jump = st.sidebar.columns([1, 1, 2])
if col_p.button("◀ Prev"):
    st.session_state.idx = max(0, st.session_state.idx - 1)
if col_n.button("Next ▶"):
    st.session_state.idx = min(total - 1, st.session_state.idx + 1)
jump = col_jump.number_input(
    "No.", min_value=1, max_value=total,
    value=st.session_state.idx + 1, label_visibility="collapsed",
)
st.session_state.idx = jump - 1

# Done set — keyed by annotator, refreshed on save or annotator change
done_key = f"done_set_{annotator}"
if done_key not in st.session_state:
    with st.spinner("Loading progress..."):
        st.session_state[done_key] = get_done_set(annotator)

if st.sidebar.button("⏭ Next incomplete"):
    for i in range(total):
        if images[i] not in st.session_state[done_key]:
            st.session_state.idx = i
            break

idx = st.session_state.idx
current = images[idx]
K = f"{current}__{annotator}__"

done_count = len(st.session_state[done_key])
status = "✅" if current in st.session_state[done_key] else "⬜"
st.sidebar.markdown(f"{status} **{idx+1}/{total}** `{current}` (done: {done_count})")

# Show image from Drive (in sidebar — stays visible while scrolling right side)
try:
    img_bytes = download_image(image_ids[current])
    st.sidebar.image(img_bytes, use_container_width=True)
except Exception as e:
    st.sidebar.error(f"Failed to load image: {e}")

# Load saved annotation (from session cache — no API call per image)
saved = load_annotation(current, annotator)

# ─── Mobile: show image in main area (hidden on desktop via CSS) ───
img_b64 = base64.b64encode(img_bytes).decode()
st.markdown(f"""
<div class="mobile-oct-image" id="mobile-oct-image">
    <img src="data:image/jpeg;base64,{img_b64}" />
    <div class="mobile-oct-info">{status} {idx+1}/{total} &mdash; {current} (done: {done_count})</div>
</div>
""", unsafe_allow_html=True)

# ─── Main area: annotation form (scrolls independently) ─────

# ── Pre-scan session_state for positive findings (before rendering) ──
# This detects which positives are checked from the PREVIOUS render cycle,
# so we can force-clear conflicting negatives BEFORE they are rendered.

def _read_positives_from_session():
    """Read currently checked positive findings from session_state keys."""
    positives = set()
    for prefix, categories in [("fov", FOVEA_CATEGORIES), ("ext", EXTRAFOVEA_CATEGORIES)]:
        for cat_name, cat_findings in categories.items():
            for fi, f in enumerate(cat_findings):
                key = f"{K}{prefix}_{cat_name}_{fi}"
                if st.session_state.get(key, False):
                    positives.add(f)
    return positives

prev_positives = _read_positives_from_session()
forced_off_negatives = set()
for pos_finding, neg_finding in POS_TO_NEG.items():
    if pos_finding in prev_positives:
        forced_off_negatives.add(neg_finding)

# Force-clear conflicting negative checkboxes in session_state BEFORE rendering
NEG_TO_INDEX = {n: i for i, n in enumerate(NEG_FINDINGS)}
for neg in forced_off_negatives:
    neg_key = f"{K}neg_{NEG_TO_INDEX[neg]}"
    if st.session_state.get(neg_key, False):
        st.session_state[neg_key] = False

c1, c2 = st.columns([1, 1])
scan_type_opts = ["B-scan", "C-scan", "OCTA", "other"]
scan_type = c1.selectbox(
    "Scan", scan_type_opts,
    index=scan_type_opts.index(saved.get("scan_type", "B-scan"))
    if saved.get("scan_type") in scan_type_opts else 0,
    key=f"{K}st",
)
scan_loc_opts = ["macula", "optic disc", "periphery", "other"]
scan_loc = c2.selectbox(
    "Location", scan_loc_opts,
    index=scan_loc_opts.index(saved.get("scan_loc", "macula"))
    if saved.get("scan_loc") in scan_loc_opts else 0,
    key=f"{K}sl",
)
saved_quality = saved.get("quality", "good")
quality_opts = ["good", "fair", "poor"]
quality = st.radio(
    "**Quality**", quality_opts,
    index=quality_opts.index(saved_quality) if saved_quality in quality_opts else 0,
    horizontal=True, key=f"{K}qual",
)

st.markdown("---")

# ── L1: Findings ──

saved_loc_findings = saved.get("L1_loc_findings", saved.get("L2_loc_findings", {}))
loc_findings = {}

def render_category(label, categories, prefix, saved_data):
    st.markdown(f"### {label}")
    data = {}
    for cat_name, cat_findings in categories.items():
        if cat_name == "Intraretinal-1":
            display = "**Intraretinal**"
        elif cat_name == "Intraretinal-2":
            display = ""
        elif cat_name == "Outer retina-1":
            display = "**Outer retina**"
        elif cat_name == "Outer retina-2":
            display = ""
        else:
            display = f"**{cat_name}**"

        if cat_name not in ("Intraretinal-2", "Outer retina-2"):
            st.markdown(
                "<hr style='margin:0.1rem 0; border:none; border-top:1px solid #ddd;'>",
                unsafe_allow_html=True,
            )
        if cat_name == "Outer retina-1":
            col_widths = [1.2, 1, 1.6, 1]
        else:
            col_widths = [1.2] + [1] * len(cat_findings)
        cols = st.columns(col_widths)
        cols[0].markdown(display)
        saved_cat = saved_data.get(cat_name, [])
        checked = []
        for fi, f in enumerate(cat_findings):
            if cols[fi + 1].checkbox(f, value=(f in saved_cat), key=f"{K}{prefix}_{cat_name}_{fi}"):
                checked.append(f)
        data[cat_name] = checked
    return data

fovea_label = "Fovea (<500um)"
saved_fovea = saved_loc_findings.get(fovea_label, {})
st.markdown('<div class="fovea-block">', unsafe_allow_html=True)
loc_findings[fovea_label] = render_category(fovea_label, FOVEA_CATEGORIES, "fov", saved_fovea)
st.markdown('</div>', unsafe_allow_html=True)

extra_label = "Extrafovea (>500um)"
saved_extra = saved_loc_findings.get(extra_label, {})
st.markdown('<div class="extrafovea-block">', unsafe_allow_html=True)
loc_findings[extra_label] = render_category(extra_label, EXTRAFOVEA_CATEGORIES, "ext", saved_extra)
st.markdown('</div>', unsafe_allow_html=True)

# Recalculate positives from actual rendered checkboxes (for has_findings & save)
all_positive = set()
for loc_data in loc_findings.values():
    if isinstance(loc_data, dict):
        for cat_findings in loc_data.values():
            all_positive.update(cat_findings)

# Update forced_off based on current render
forced_off_negatives = set()
for pos_finding, neg_finding in POS_TO_NEG.items():
    if pos_finding in all_positive:
        forced_off_negatives.add(neg_finding)

has_findings = any(
    f for loc_data in loc_findings.values() if isinstance(loc_data, dict)
    for f in loc_data.values() if f
)

st.markdown("---")

# Negative findings (1 row)
neg_cols = st.columns([1.2] + [1] * len(NEG_FINDINGS))
neg_cols[0].markdown("**Negative**")
saved_neg = saved.get("L1_neg", saved.get("L2_neg", NEG_FINDINGS))
neg_checked = []
for i, n in enumerate(NEG_FINDINGS):
    neg_key = f"{K}neg_{i}"
    if n in forced_off_negatives:
        neg_cols[i + 1].checkbox(f"~~{n}~~", value=False, disabled=True, key=f"{K}neg_disabled_{i}")
    else:
        if neg_cols[i + 1].checkbox(n, value=(n in saved_neg), key=neg_key):
            neg_checked.append(n)

st.markdown("---")

st.markdown("**L2. Abnormality**")
l2_opts = ["abnormal", "normal", "uncertain"]
l2_key = f"{K}l2"
# Auto-switch L2 based on findings
if has_findings and st.session_state.get(l2_key) == "normal":
    st.session_state[l2_key] = "abnormal"
elif not has_findings and st.session_state.get(l2_key) == "abnormal":
    st.session_state[l2_key] = "normal"
l2_saved = saved.get("L2", saved.get("L1"))
if l2_saved and l2_saved in l2_opts:
    l2_default = l2_saved
elif has_findings:
    l2_default = "abnormal"
else:
    l2_default = "normal"
l2 = st.radio("l2", l2_opts, index=l2_opts.index(l2_default), horizontal=True, key=l2_key, label_visibility="collapsed")

st.markdown("**L3. Management**")
mgmt_opts = ["no abnormality", "observation", "further exam", "treatment"]
mgmt_key = f"{K}mgmt"
# Auto-switch L3 based on findings
if has_findings and st.session_state.get(mgmt_key) == "no abnormality":
    st.session_state[mgmt_key] = "observation"
elif not has_findings and st.session_state.get(mgmt_key) in ("observation", "further exam", "treatment"):
    st.session_state[mgmt_key] = "no abnormality"
saved_mgmt = saved.get("L3_mgmt", saved.get("L4_mgmt", "no abnormality"))
mgmt = st.radio(
    "l3", mgmt_opts,
    index=mgmt_opts.index(saved_mgmt) if saved_mgmt in mgmt_opts else 0,
    horizontal=True, key=mgmt_key, label_visibility="collapsed",
)

st.markdown("---")

# Auto-generate caption from current form state
def _build_preview_data():
    return {
        "quality": quality, "L2": l2,
        "L1_loc_findings": loc_findings,
        "L1_neg": neg_checked,
        "L3_mgmt": mgmt,
    }

col_cap_label, col_cap_btn = st.columns([3, 1])
col_cap_label.markdown("**Caption**")
if col_cap_btn.button("Auto Generate", key=f"{K}auto_cap"):
    st.session_state[f"{K}cap"] = generate_caption(_build_preview_data())
    st.rerun()

caption = st.text_area("Caption", value=saved.get("caption", ""), height=300, key=f"{K}cap")

# ── Save ──
def build_data():
    return {
        "scan_type": scan_type, "scan_loc": scan_loc, "quality": quality,
        "L1_loc_findings": loc_findings,
        "L2": l2,
        "L1_neg": neg_checked,
        "L3_mgmt": mgmt,
        "caption": caption,
    }

c_save, c_next = st.columns(2)
if c_save.button("Save", type="primary", use_container_width=True):
    with st.spinner("Saving to Google Sheets..."):
        save_annotation(build_data(), current, annotator)
        st.session_state[done_key].add(current)
    st.success("Saved")
    st.rerun()

if c_next.button("Save & Next ▶", use_container_width=True):
    with st.spinner("Saving to Google Sheets..."):
        save_annotation(build_data(), current, annotator)
        st.session_state[done_key].add(current)
    st.session_state.idx = min(total - 1, idx + 1)
    st.rerun()

# Scroll to top
st.html("<script>window.parent.document.querySelector('section.main').scrollTo(0,0);</script>")
