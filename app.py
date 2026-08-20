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
    /* Sidebar fully covers screen when open on mobile */
    [data-testid="stSidebar"] {{
        z-index: 999999 !important;
    }}
    [data-testid="stSidebar"][aria-expanded="true"] {{
        position: fixed !important;
        top: 0; left: 0;
        width: 100vw !important;
        height: 100vh !important;
        min-width: 100vw !important;
        max-width: 100vw !important;
        background: #0e1117 !important;
    }}
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

# キャプション比較レポート（compare_v5_19_retest_Recall）で使った40サンプル。
# サイドバーのトグルで「この40枚のみ / 全画像」を切り替える。
SUBSET_40 = {
    "BRVO_002.png", "BRVO_009.png", "BRVO_014.png", "BRVO_023.png", "BRVO_031.png",
    "BRVO_034.png", "BRVO_073.png", "BRVO_074.png", "BRVO_077.png", "BRVO_086.png",
    "BRVO_102.png", "BRVO_117.png", "BRVO_118.png", "BRVO_124.png", "BRVO_126.png",
    "BRVO_127.png", "BRVO_135.png", "BRVO_139.png",
    "CAT_006.png", "CAT_010.png", "CAT_027.png", "CAT_036.png", "CAT_038.png",
    "CAT_046.png", "CAT_051.png", "CAT_061.png", "CAT_062.png", "CAT_082.png",
    "CAT_088.png", "CAT_089.png", "CAT_096.png", "CAT_099.png", "CAT_110.png",
    "CAT_111.png", "CAT_149.png", "CAT_156.png", "CAT_165.png", "CAT_174.png",
    "CAT_180.png", "CAT_189.png",
}

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
    # 部位（fovea/extrafovea）を区別しない統合列
    "VRI", "intraretinal", "outer_retina", "choroid",
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

    # v2: 部位（fovea/extrafovea）統合・フルスペル化に伴い列構造を変更。
    # 旧シート（OCT_annotations_<名前>）はそのまま残り、過去データは失われない。
    sheet_name = f"OCT_annotations_v2_{annotator}"
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
    # 部位を区別せず、カテゴリ単位（VRI / intraretinal / outer_retina / choroid）に集約。
    # 過去データが fovea/extrafovea 別に入っていても、ここで1つにまとめて出力する。
    loc_findings = data.get("L1_loc_findings", {})
    cat_merged = {"VRI": [], "intraretinal": [], "outer_retina": [], "choroid": []}
    for loc_key, loc_data in loc_findings.items():
        if not isinstance(loc_data, dict):
            continue
        for cat_name, findings in loc_data.items():
            base = cat_name.replace("-1", "").replace("-2", "")
            if "VRI" in base:
                cat_short = "VRI"
            elif "Intraretinal" in base:
                cat_short = "intraretinal"
            elif "Outer" in base:
                cat_short = "outer_retina"
            elif "Choroid" in base:
                cat_short = "choroid"
            else:
                continue
            for f in (findings or []):
                if f not in cat_merged[cat_short]:
                    cat_merged[cat_short].append(f)
    for cat_short, findings in cat_merged.items():
        row[cat_short] = "; ".join(findings) if findings else ""

    row["negative_findings"] = "; ".join(data.get("L1_neg", []))
    row["L2_abnormality"] = data.get("L2", "")
    row["L3_management"] = data.get("L3_mgmt", "")

    row["caption"] = data.get("caption", "")
    row["auto_caption"] = generate_caption(data)
    row["raw_json"] = json.dumps(data, ensure_ascii=False)
    return row

# ─── Auto Caption Generation ─────────────────────────────────

# ─── Auto Caption Generation (改訂版) ─────────────────────────

# フルスペル対応表（確定版）
FULLSPELL = {
    "PVD": "posterior vitreous detachment (PVD)",
    "ERM": "epiretinal membrane (ERM)",
    "VMT": "vitreomacular traction (VMT)",
    "VH": "vitreous hemorrhage (VH)",
    "IRF": "intraretinal fluid (IRF)",
    "hemorrhage": "retinal hemorrhage",
    "retinal thickening": "retinal thickening",
    "tractional thickening": "tractional retinal thickening",
    "inner thinning": "inner retinal thinning",
    "hyperreflective foci": "hyperreflective foci",
    "hard exudates": "hard exudates",
    "SRF": "subretinal fluid (SRF)",
    "subretinal hemorrhage": "subretinal hemorrhage",
    "serous PED": "serous pigment epithelial detachment (serous PED)",
    "SHRM": "subretinal hyperreflective material (SHRM)",
    "EZ disruption": "ellipsoid zone (EZ) disruption",
    "outer atrophy": "outer retinal atrophy",
    "drusen": "drusen",
    "choroidal thickening": "choroidal thickening",
    "choroidal thinning": "choroidal thinning",
}
NEG_FULLSPELL = {
    "no SRF": "no subretinal fluid (SRF)",
    "no IRF": "no intraretinal fluid (IRF)",
    "no PED": "no pigment epithelial detachment (PED)",
    "EZ intact": "intact ellipsoid zone (EZ)",
    "no ERM": "no epiretinal membrane (ERM)",
}


def _join_english_list(items):
    if not items:
        return ""
    if len(items) == 1:
        return items[0]
    if len(items) == 2:
        return f"{items[0]} and {items[1]}"
    return ", ".join(items[:-1]) + ", and " + items[-1]


def _full(name):
    """所見名をフルスペル(略語)へ。未定義はそのまま。"""
    return FULLSPELL.get(name, name)


def generate_caption(data):
    """Deterministic English caption. 部位は区別しない。
    - 所見があれば normal でも abnormal 扱い
    - poor + 所見 → 慎重解釈を促す1文
    - 所見はフルスペル(略語)で記述
    """
    sentences = []

    # ---- 全所見を部位区別なしで集約（順序は定義順を維持） ----
    loc_findings = data.get("L1_loc_findings", {})
    findings_all = []  # フラットな所見リスト（重複除去・順序維持）
    seen = set()
    for loc_data in loc_findings.values():
        if not isinstance(loc_data, dict):
            continue
        for findings_list in loc_data.values():
            for f in (findings_list or []):
                if f and f != "other" and f not in seen:
                    seen.add(f)
                    findings_all.append(f)
    has_findings = len(findings_all) > 0

    quality = (data.get("quality") or "").strip().lower()

    # 1. Image quality
    if quality == "good":
        sentences.append("Image quality is sufficient for evaluation.")
    elif quality == "fair":
        sentences.append("Image quality is limited but adequate for evaluation.")
    elif quality == "poor":
        if has_findings:
            sentences.append("Image quality is poor; findings should be interpreted with caution.")
        else:
            sentences.append("The image is not adequate for full evaluation.")

    # 2. Abnormality presence（所見があれば abnormal 扱い）
    abnormality = (data.get("L2") or "").strip().lower()
    if has_findings:
        sentences.append("Abnormal findings are present.")
    elif abnormality == "normal":
        sentences.append("No abnormal findings are present.")
    elif abnormality == "abnormal":
        sentences.append("Abnormal findings are present.")
    elif abnormality == "uncertain":
        sentences.append("The presence of abnormality is uncertain.")

    # 3. Findings（部位なし・フルスペル・1文にまとめる）
    if has_findings:
        expanded = [_full(f) for f in findings_all]
        findings_text = _join_english_list(expanded)
        verb = "is" if len(expanded) == 1 else "are"
        sentences.append(f"{findings_text[0].upper() + findings_text[1:]} {verb} observed.")

    # 4. Negative findings（フルスペル）
    neg_list = data.get("L1_neg", [])
    valid_neg = [NEG_FULLSPELL.get(n, n) for n in neg_list if n and n.strip()]
    if valid_neg:
        sentences.append(f"Negative findings: {_join_english_list(valid_neg)}.")

    # 5. Management
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

# 部位（fovea/extrafovea）を区別しない統合カテゴリ（Choroid を含む上位集合）。
# 保存時はこの1セットを従来の "Fovea (<500um)" キーに入れて、既存の列構造・過去データを壊さない。
FINDING_CATEGORIES = {
    "VRI":              ["PVD", "ERM", "VMT", "VH"],
    "Intraretinal-1":   ["IRF", "hemorrhage", "retinal thickening", "tractional thickening"],
    "Intraretinal-2":   ["inner thinning", "hyperreflective foci", "hard exudates"],
    "Outer retina-1":   ["SRF", "subretinal hemorrhage", "serous PED"],
    "Outer retina-2":   ["SHRM", "EZ disruption", "outer atrophy", "drusen"],
    "Choroid":          ["choroidal thickening", "choroidal thinning"],
}
UNIFIED_LOC_KEY = "Fovea (<500um)"   # 統合後の保存先キー（過去データ互換のため既存キー名を流用）

NEG_FINDINGS = ["no SRF", "no IRF", "no PED", "EZ intact", "no ERM"]

# Mapping: positive finding -> negative to auto-deselect
POS_TO_NEG = {
    "SRF": "no SRF",
    "IRF": "no IRF",
    "serous PED": "no PED",
    "EZ disruption": "EZ intact",
    "ERM": "no ERM",
}


def reconcile_annotation(loc_findings, neg_checked, l2, mgmt):
    """入力内容の整合を取る（Auto Generate / Save 時に呼ぶ）。
    フォーム化で入力中はリアルタイム連動しないため、確定時にまとめて補正する。
    返り値: (neg_checked, l2, mgmt, has_findings)
    """
    # 陽性所見を集める
    positives = set()
    for loc_data in loc_findings.values():
        if isinstance(loc_data, dict):
            for finds in loc_data.values():
                for f in (finds or []):
                    if f and f != "other":
                        positives.add(f)
    has_findings = len(positives) > 0

    # 1) 陽性↔陰性の矛盾を解消（陽性がある陰性所見は外す）
    drop_neg = {neg for pos, neg in POS_TO_NEG.items() if pos in positives}
    neg_checked = [n for n in neg_checked if n not in drop_neg]

    # 2) 所見があれば L2 を abnormal に、なければ normal に補正
    if has_findings:
        l2 = "abnormal"
    elif l2 == "abnormal":
        l2 = "normal"

    # 3) 所見があるのに方針が「no abnormality」なら observation に、
    #    所見が無いのに治療系なら no abnormality に補正
    if has_findings and mgmt == "no abnormality":
        mgmt = "observation"
    elif not has_findings and mgmt in ("observation", "further exam", "treatment"):
        mgmt = "no abnormality"

    return neg_checked, l2, mgmt, has_findings

# ─── Image list ──────────────────────────────────────────────

images_info = get_image_list()
if not images_info:
    st.error("No images found in Google Drive folder. Check folder ID and permissions.")
    st.stop()

# 画像セットの切替（40枚のみ / 全画像）。デフォルトは40枚。
image_set = st.sidebar.radio(
    "Image set",
    ["40 subset", "All images"],
    index=0,
    horizontal=True,
    key="image_set",
    help="40 subset = キャプション比較で使った40枚のみ。All = Driveの全画像。",
)
if image_set == "40 subset":
    filtered = [(n, i) for (n, i) in images_info if n in SUBSET_40]
    if filtered:
        images_info = filtered
    else:
        st.sidebar.warning("40枚のうちDriveに見つかった画像がありません。全画像を表示します。")

images = [name for name, _ in images_info]
image_ids = {name: fid for name, fid in images_info}
total = len(images)
st.sidebar.caption(f"{'40枚のみ' if image_set == '40 subset' else '全画像'}: {total} 枚")

# ─── Sidebar: image + navigation (fixed, doesn't scroll with main) ───

st.sidebar.slider("Image panel width", min_value=300, max_value=800, value=500, step=50, key="sidebar_w")
annotator = st.sidebar.text_input("Annotator name", value="default")

if not annotator or annotator.strip() == "":
    st.warning("Please enter your annotator name in the sidebar.")
    st.stop()

annotator = annotator.strip()

if "idx" not in st.session_state:
    st.session_state.idx = 0

# 画像セット切替で件数が減ったとき、idx が範囲外にならないよう丸める
if st.session_state.idx > total - 1:
    st.session_state.idx = total - 1
if st.session_state.idx < 0:
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

# 入力はフォームにまとめる：チェックや選択を触っても再実行（Running）せず、
# 送信ボタン（Auto Generate / Save / Save & Next）を押したときだけまとめて処理する。
# 矛盾の解消・L2/L3の自動補正は、送信時に reconcile_annotation() で行う。
annot_form = st.form(key=f"{K}form", clear_on_submit=False)
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


saved_loc_findings = saved.get("L1_loc_findings", saved.get("L2_loc_findings", {}))
# 過去データ（fovea/extrafovea 別）はカテゴリ単位でマージして統合表示。
saved_unified = {}
for _loc_key, _loc_data in saved_loc_findings.items():
    if not isinstance(_loc_data, dict):
        continue
    for _cat, _finds in _loc_data.items():
        merged = saved_unified.setdefault(_cat, [])
        for _f in (_finds or []):
            if _f not in merged:
                merged.append(_f)

l2_opts = ["abnormal", "normal", "uncertain"]
mgmt_opts = ["no abnormality", "observation", "further exam", "treatment"]

# Auto Generate の整合結果は、ウィジェットのkeyを直接書き換えられない（Streamlit制約）ため、
# 画像ごとの pending 値として持ち、次の描画でウィジェットの初期値に使ってから消す。
pending_key = f"{K}pending"
pending = st.session_state.pop(pending_key, None)  # あれば取り出して消す

def _init_caption():
    if pending and "caption" in pending:
        return pending["caption"]
    return saved.get("caption", "")

def _init_l2():
    if pending and "l2" in pending:
        return pending["l2"]
    v = saved.get("L2", saved.get("L1"))
    return v if v in l2_opts else "normal"

def _init_mgmt():
    if pending and "mgmt" in pending:
        return pending["mgmt"]
    v = saved.get("L3_mgmt", saved.get("L4_mgmt", "no abnormality"))
    return v if v in mgmt_opts else "no abnormality"

def _init_neg(n):
    if pending and "neg" in pending:
        return n in pending["neg"]
    return n in saved.get("L1_neg", saved.get("L2_neg", NEG_FINDINGS))

# ── 入力フォーム（送信するまで再実行しない） ──
loc_findings = {}
with annot_form:
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

    # ── L1: Findings（部位なし統合） ──
    st.markdown('<div class="fovea-block">', unsafe_allow_html=True)
    loc_findings[UNIFIED_LOC_KEY] = render_category("Findings", FINDING_CATEGORIES, "unif", saved_unified)
    st.markdown('</div>', unsafe_allow_html=True)

    st.markdown("---")

    # Negative findings（フォーム内では常に表示。矛盾は送信時に自動解消）
    # key を付けない：Auto Generate の整合結果を value= で反映するため。
    neg_cols = st.columns([1.2] + [1] * len(NEG_FINDINGS))
    neg_cols[0].markdown("**Negative**")
    neg_checked = []
    for i, n in enumerate(NEG_FINDINGS):
        if neg_cols[i + 1].checkbox(n, value=_init_neg(n)):
            neg_checked.append(n)

    st.markdown("---")

    st.markdown("**L2. Abnormality**")
    l2 = st.radio("l2", l2_opts, index=l2_opts.index(_init_l2()),
                  horizontal=True, label_visibility="collapsed")

    st.markdown("**L3. Management**")
    mgmt = st.radio("l3", mgmt_opts, index=mgmt_opts.index(_init_mgmt()),
                    horizontal=True, label_visibility="collapsed")

    st.markdown("---")
    st.markdown("**Caption**")
    caption = st.text_area("Caption", value=_init_caption(),
                           height=300, label_visibility="collapsed")

    # 送信ボタン（これを押したときだけ処理が走る）
    b1, b2, b3 = st.columns(3)
    do_generate = b1.form_submit_button("Auto Generate", use_container_width=True)
    do_save = b2.form_submit_button("Save", type="primary", use_container_width=True)
    do_next = b3.form_submit_button("Save & Next ▶", use_container_width=True)

# ── 送信後の処理（フォームの外）：整合を取ってから生成／保存 ──
def build_data(neg, l2v, mgmtv, cap):
    return {
        "scan_type": scan_type, "scan_loc": scan_loc, "quality": quality,
        "L1_loc_findings": loc_findings,
        "L2": l2v,
        "L1_neg": neg,
        "L3_mgmt": mgmtv,
        "caption": cap,
    }

if do_generate:
    # 陽性↔陰性の矛盾解消・L2/L3補正をしてからキャプション生成
    neg2, l2b, mgmt2, _ = reconcile_annotation(loc_findings, neg_checked, l2, mgmt)
    new_cap = generate_caption({
        "quality": quality, "L2": l2b,
        "L1_loc_findings": loc_findings, "L1_neg": neg2, "L3_mgmt": mgmt2,
    })
    # 整えた値は pending に保存し、次の描画でウィジェットの初期値として反映する。
    # （ウィジェットkeyを生成後に書き換えるとStreamlitがエラーになるため）
    st.session_state[pending_key] = {
        "caption": new_cap, "l2": l2b, "mgmt": mgmt2, "neg": neg2,
    }
    st.rerun()

if do_save or do_next:
    neg2, l2b, mgmt2, _ = reconcile_annotation(loc_findings, neg_checked, l2, mgmt)
    with st.spinner("Saving to Google Sheets..."):
        save_annotation(build_data(neg2, l2b, mgmt2, caption), current, annotator)
        st.session_state[done_key].add(current)
    if do_next:
        st.session_state.idx = min(total - 1, idx + 1)
    else:
        st.success("Saved")
    st.rerun()

# Scroll to top
st.html("<script>window.parent.document.querySelector('section.main').scrollTo(0,0);</script>")
