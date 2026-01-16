import re
import json
import pdfplumber
from pathlib import Path
from bs4 import BeautifulSoup
from datetime import datetime
from collections import defaultdict
from typing import Dict,Optional


# ============== GLOBAL REGEX PATTERNS ==============
DPD_TOKEN_RE = re.compile(r'\b(?:000|XXX|STD|-|\d{3})\b', re.I)
ACCOUNT_HEADER_RE = re.compile(r'(?im)^\s*(\d+)\.\s*Account\b')
# ============== CORE EXTRACTOR CLASSES ==============


class CIBILExtractor:
    """Base extractor with common patterns for both HTML and PDF"""
    _patterns = {
        'name': re.compile(
            r'CONSUMER(?: NAME)?\s*:\s*([A-Z][A-Z\s\.]+?)(?=\s+DATE\s*:|\n|$)',
            re.I
        ),
        'pan_card': re.compile(r'(?:PAN[:\s]+|INCOME TAX ID NUMBER \(PAN\)\s*)([A-Z]{5}\d{4}[A-Z])', re.I),
        'ckyc': re.compile(r'CKYC[:\s]*(\d{12,15})', re.I),
        'score': re.compile(r'CREDITVISION[®\s]*SCORE[:\s]*(-?\d{1,3})|SCORE[:\s]*(-?\d{1,3})', re.I),
        'report_date': re.compile(r'DATE[:\s]*(\d{2}-\d{2}-\d{4})|REPORT\s+DATE\s*&?\s*TIME\s*:\s*(\d{2}/\d{2}/\d{4})|(\d{2}/\d{2}/\d{4},\s*\d{2}:\d{2})', re.I | re.DOTALL),
        'mobile': re.compile(r'\b([789]\d{9})\b'),
        'email': re.compile(r'[\w.%+-]+@[\w.-]+\.[a-zA-Z]{2,}'),
        'fallback_score': re.compile(r'(?:CREDIT|SCORE).*?(\d{3})\b', re.I | re.S),
        'overdue_count': re.compile(r'OVERDUE[:\s]*(\d+)', re.I),
        'overdue_amount': re.compile(r'OVERDUE[:\s]*([\d,]+)', re.I),
        'current_amount': re.compile(r'CURRENT[:\s]*([\d,]+)', re.I | re.DOTALL),
        'dpd_header': re.compile(r'DAYS\s+PAST\s+DUE/ASSET\s+CLASSIFICATION', re.I),
        'status_token': re.compile(r'\b(STD|XXX|\d{3})\b'),
        'month_token': re.compile(r'\b(\d{2}-\d{2})\b'),
        'enquiry_date': re.compile(r'ENQUIRIES:\s*(.*)$', re.I | re.S),
    }

    @staticmethod
    def _first_match(text, pattern):
        match = pattern.search(text)
        if match:
            for group in match.groups():
                if group:
                    return group.strip()
        return None

    @staticmethod
    def _clean_amount(raw):
        if not raw:
            return "0"
        cleaned = re.sub(r'[₹,\s]', '', raw.strip())
        return cleaned if cleaned.isdigit() else "0"
    
    @staticmethod
    def _parse_date(raw):
        if not raw or not raw.strip():
            return None
        raw = raw.strip()
        date_formats = [
            "%d-%m-%Y",           # "24-12-2025" ✅
            "%d/%m/%Y",           # "24/12/2025"
            "%d/%m/%Y, %H:%M",    # "24/12/2025, 11:59"
            "%d-%m-%Y %H:%M:%S",  # "24-12-2025 11:59:04"
        ]
        for fmt in date_formats:
            try:
                return datetime.strptime(raw, fmt).strftime("%Y-%m-%d")
            except ValueError:
                continue
        return None


    
    @staticmethod
    def _dpd_to_number(status: str) -> int:
        """UPDATED: 000, XXX, and - are all treated as 0 (clean)"""
        status = status.upper().strip()
        if status in ("STD", "000", "XXX", "-"):
            return 0
        try:
            return int(status)
        except ValueError:
            return 0


# ============== HTML-SPECIFIC LOGIC ==============


def _extract_dpd_history_improved(block):
    """FIXED: Precise DPD extraction using strict token matching"""

    
    dpd_history = {}
    lines = [line.strip() for line in block.split('\n') if line.strip()]
    
    year_pattern = re.compile(r'^\s*(\d{4})\s*$', re.I)
    
    for i, line in enumerate(lines):
        year_match = year_pattern.match(line)
        if year_match:
            year = year_match.group(1)
            
            # Collect ONLY true DPD tokens from next 12 lines
            tokens = []
            for j in range(i+1, min(i+13, len(lines))):
                tokens.extend(DPD_TOKEN_RE.findall(lines[j]))
            
            if tokens:
                dpd_history[year] = ' '.join(tokens)
    
    return dpd_history


def _extract_accounts_from_html(soup):
    """NEW: Extracts ALL accounts, every field optional - no header dependency"""
    full_text = soup.get_text(separator='\n', strip=True)
    
    # Try account section, fallback to full text
    account_section_match = re.search(r'CONSUMER ACCOUNT DETAILS', full_text, re.IGNORECASE)
    if not account_section_match:
        print("⚠️ No CONSUMER ACCOUNT DETAILS - scanning full text")
        accounts_text = full_text
    else:
        accounts_text = full_text[account_section_match.start():]
    
    accounts = []
    
    # Split into potential account blocks by common delimiters
    account_blocks = re.split(r'(?i)(?:(?=\d{2}-\d{2}-\d{4})|MEMBER NAME|ACCOUNT TYPE|DATE OPENED)', accounts_text)
    
    for i, block in enumerate(account_blocks):
        block = block.strip()
        if not block or len(block) < 50:  # Skip tiny fragments
            continue
        
        account = {
            'original_html_index': str(i+1),  # Fallback index
            'date_opened': None,
            'date_closed': None,
            'date_reported': None,
            'status': None,
            'account_type': None,
            'member_name': None,
            'sanctioned_amount': "0",
            'current_balance': "0",
            'dpd_history': {},
            'deterioration_reasoning': ""
        }
        
        # Extract WHATEVER fields exist - all optional
        patterns = {
            ('date_opened', r'DATE Opened[:\s]*([^\n|]+?)'),
            ('date_closed', r'DATE CLOSED[:\s]*([^\n|]+?)'),
            ('date_reported', r'DATE REPORTED[^:\n]*[:\s]*([^\n|]+?)'),
            ('status', r'\b(inactive|active|STD|NA|STANDARD)\b'),
            ('account_type', r'ACCOUNT TYPE\s*:\s*([^\n:]+?)'),
            ('member_name', r'MEMBER NAME\s*:\s*([^\n:]+?)'),
            ('sanctioned_amount', r'sanctioned amouNT\s*:\s*₹?\s*([^\n:]+?)'),
            ('current_balance', r'current balance\s*:\s*₹?\s*([^\n:]+?)'),
        }
        
        for field, pattern in patterns:
            match = re.search(pattern, block, re.I)
            if match:
                val = match.group(1).strip()
                if field in ('sanctioned_amount', 'current_balance'):
                    account[field] = CIBILExtractor._clean_amount(val) or "0"
                else:
                    account[field] = val if val and val not in (':', '') else None
        
        # Always extract DPD (core feature)
        dpd_history = _extract_dpd_history_improved(block)
        account['dpd_history'] = dpd_history
        account['deterioration_reasoning'] = _get_deterioration_reasoning(dpd_history)
        
        accounts.append(account)  # ADD EVERY account
    
    print(f"✅ Extracted {len(accounts)} accounts (all fields optional)")
    return accounts


def _get_deterioration_reasoning(dpd_history: dict) -> str:
    """Precise DPD deterioration detection using strict tokens"""
    CLEAN_STATES = {"000", "XXX", "-", "STD"}
    
    for year in sorted(dpd_history.keys()):
        dpd_string = dpd_history[year]
        if not dpd_string:
            continue
        
        # **FIXED: Use strict DPD tokens only**
        dpd_values = [t.upper() for t in DPD_TOKEN_RE.findall(dpd_string)]
        if not dpd_values:
            continue
        
        # RULE 1: Completely dirty
        has_clean = any(t in CLEAN_STATES for t in dpd_values)
        if not has_clean and len(dpd_values) > 0:
            return f"COMPLETELY_DIRTY: {len(dpd_values)} dirty tokens in {year}"
        
        # RULE 2: Clean↔Dirty transitions
        for i in range(len(dpd_values) - 1):
            prev, curr = dpd_values[i], dpd_values[i+1]
            is_clean_prev = prev in CLEAN_STATES
            is_clean_curr = curr in CLEAN_STATES
            
            if is_clean_prev != is_clean_curr:
                direction = "CLEAN_TO_DIRTY" if not is_clean_curr else "DIRTY_TO_CLEAN"
                return f"{direction}: '{prev}'→'{curr}' in {year}"
        
        # RULE 3: Explicit dirty patterns
        dirty_examples = ['015', '032', '046']
        for dirty in dirty_examples:
            if dirty in dpd_values:
                return f"EXPLICIT_DIRTY: '{dirty}' in {year}"
    
    return ""


def _extract_overdue_summary_from_html_text(full_text: str) -> dict:
    if not full_text:
        return {
            "total_overdue_accounts": None,
            "total_overdue_amount": None,
            "total_current_amount": None
        }

    section_match = re.search(
        r'CONSUMER\s+ACCOUNT\s+SUMMARY(.*?)(?:CONSUMER\s+ACCOUNT\s+DETAILS|CONSUMER\s+ENQUIRY\s+DETAILS|CONSUMER\s+DETAILS|$)',
        full_text,
        flags=re.IGNORECASE | re.DOTALL
    )
    section = section_match.group(1) if section_match else full_text

    normalized = re.sub(r'\s+', ' ', section).strip()

    overdue_count = None
    m_count = re.search(r'\bOverdue\s*:\s*(\d+)\b', normalized, flags=re.IGNORECASE)
    if m_count:
        try:
            overdue_count = int(m_count.group(1))
        except ValueError:
            overdue_count = None

    current_amount = None
    m_current = re.search(r'\bCurrent\s*:\s*₹?\s*([\d,]+)\b', normalized, flags=re.IGNORECASE)
    if m_current:
        cleaned = CIBILExtractor._clean_amount(m_current.group(1))
        current_amount = int(cleaned) if cleaned.isdigit() else None

    overdue_amount = None
    m_over_amt = re.search(r'\bOverdue\s*:\s*₹\s*([\d,]+)\b', normalized, flags=re.IGNORECASE)
    if not m_over_amt:
        m_over_amt = re.search(r'Overdue\s*[:\s]*₹?\s*([\d,]+)', normalized, flags=re.IGNORECASE)
    if m_over_amt:
        cleaned = CIBILExtractor._clean_amount(m_over_amt.group(1))
        overdue_amount = int(cleaned) if cleaned.isdigit() else None

    return {
        "total_overdue_accounts": overdue_count,
        "total_overdue_amount": overdue_amount,
        "total_current_amount": current_amount
    }



def _extract_enquiries_from_html(soup):
    """HTML: Extract all enquiries, then filter for latest month"""
    full_text = soup.get_text(separator='\n', strip=True)
    
    heading_pattern = r'CONSUMER ENQUIRY DETAILS\s*Enquiries'
    match = re.search(heading_pattern, full_text, re.IGNORECASE | re.DOTALL)
    
    if not match:
        return []
    
    start_pos = match.end()
    remaining_text = full_text[start_pos:]
    lines = [line.strip() for line in remaining_text.split('\n') if line.strip()]
    
    if lines and any(h in lines[0].upper() for h in ['MEMBER NAme', 'ENQUIRY date']):
        lines = lines[1:]
    
    all_enquiries = []
    i = 0
    while i < len(lines) - 2:
        current_line = lines[i]
        date_match = re.search(r'(\d{1,2}[/-]\d{1,2}[/-]\d{4})', current_line)
        if date_match:
            date_raw = date_match.group(1)
            parsed_date = CIBILExtractor._parse_date(date_raw)
            
            if parsed_date:
                member_idx = i - 1
                member = None
                while member_idx >= 0 and not lines[member_idx].strip():
                    member_idx -= 1
                if member_idx >= 0:
                    member = lines[member_idx].strip()
                
                purpose_idx = i + 1
                purpose = None
                if purpose_idx < len(lines):
                    purpose = lines[purpose_idx].strip()
                
                amount = "0"
                for j in range(i + 2, min(i + 4, len(lines))):
                    amount_line = lines[j]
                    amount_match = re.search(r'[\d,]+', amount_line)
                    if amount_match:
                        amount = CIBILExtractor._clean_amount(amount_match.group(0))
                        break
                
                all_enquiries.append({
                    'date': date_raw,
                    'parsed_date': parsed_date,
                    'member': member,
                    'purpose': purpose,
                    'amount': amount
                })
        i += 1
    
    return _filter_latest_month_enquiries(all_enquiries)



def _filter_latest_month_enquiries(all_enquiries):
    """Filter to get ONLY enquiries from the latest month"""
    if not all_enquiries:
        return []
    
    month_data = defaultdict(list)
    for enquiry in all_enquiries:
        parsed_date = enquiry['parsed_date']
        if parsed_date:
            year_month = parsed_date[:7]
            month_data[year_month].append(enquiry)
    
    if not month_data:
        return []
    
    latest_month = max(month_data.keys())
    return month_data[latest_month]





# ============== PDF-SPECIFIC LOGIC ==============


class PDFAdvancedExtractor:
    """PDF-specific advanced extraction"""
    
    def __init__(self):
        self.score_patterns = [
            re.compile(r'(\d{3})\s*1\.\s*PRESENCE\s+OF\s+DELINQUENCY.*?CREDITVISION', re.I | re.S),
            re.compile(r'SCORE\s+NAME\s+SCORE.*?(\d{3})', re.I | re.S),
            re.compile(r'SCORING FACTORS\s*\n\s*(\d{3})', re.I | re.S),
            re.compile(r'CREDITVISION.*?(\d{3})\s*\d', re.I | re.S),
        ]

    def extract_deteriorating_accounts_pdf(self, text: str, patterns: Dict) -> list:
        """PDF: Extract ONLY deteriorating accounts (same deterioration logic as HTML)"""


        lines = [line.strip() for line in text.split('\n') if line.strip()]
        deteriorating_accounts = []
        account_idx = 1
        max_accounts = 200  # PDFs like Ajay-B-M have lots of accounts

        i = 0
        while i < len(lines):
            line = lines[i]

            if patterns['dpd_header'].search(line):
                if account_idx > max_accounts:
                    break

                # 1) Capture a generous window for metadata (PDF often has metadata near the header)
                meta_start = max(0, i - 25)
                meta_end = min(len(lines), i + 60)
                account_meta_block = "\n".join(lines[meta_start:meta_end])
                account = _extract_pdf_account_metadata(account_meta_block)
                account["account_index"] = account_idx

                # 2) Capture FULL dpd block until next account starts (or next DPD header)
                j = i + 1
                dpd_parts = [lines[i]]  # include header line
                while j < len(lines):
                    # stop when the next account begins (this pattern is present in your PDF)
                    if ("ACCOUNT" in lines[j] and "DATES" in lines[j] and "AMOUNTS" in lines[j] and "STATUS" in lines[j]):
                        break
                    # stop if another dpd header starts (safety)
                    if patterns['dpd_header'].search(lines[j]):
                        break

                    dpd_parts.append(lines[j])
                    j += 1

                dpd_block = " ".join(dpd_parts)

                dpd_history = _extract_dpd_history_pdf(dpd_block)
                account["dpd_history"] = dpd_history

                reasoning = _get_deterioration_reasoning(dpd_history)
                account["deterioration_reasoning"] = reasoning

                if reasoning:
                    deteriorating_accounts.append(account)

                account_idx += 1
                i = j
                continue

            i += 1

        return deteriorating_accounts

    def extract_score(self, text: str):
        """Enhanced score extraction using multiple patterns"""
        for pattern in self.score_patterns:
            match = pattern.search(text)
            if match:
                try:
                    score = int(match.group(1))
                    if 300 <= score <= 900:
                        return score
                except (ValueError, IndexError):
                    continue
        return None



    def extract_overdue_summary(self, text: str, patterns: Dict) -> dict:
        """Extract overdue summary"""
        count = None
        overdue_amount = None
        current_amount = None
        
        count_match = patterns['overdue_count'].search(text)
        if count_match:
            count = int(count_match.group(1))
        
        amount_matches = list(patterns['overdue_amount'].finditer(text))
        if len(amount_matches) > 1:
            overdue_amount = int(amount_matches[1].group(1).replace(',', ''))
        
        current_matches = list(patterns['current_amount'].finditer(text))
        for match in current_matches:
            val = match.group(1).replace(',', '')
            if val.isdigit():
                current_amount = int(val)
                break
        
        return {
            "total_overdue_accounts": count,
            "total_overdue_amount": overdue_amount,
            "total_current_amount": current_amount
        }



    def extract_dpd_blocks(self, text: str, patterns: Dict) -> dict:
        """Extract DPD blocks (latest 5 accounts only)"""
        accounts = []
        account_averages = []
        lines = [ln.rstrip() for ln in text.split("\n")]
        i = 0
        account_idx = 1
        max_accounts = 5
        
        while i < len(lines):
            line = lines[i]
            
            if patterns['dpd_header'].search(line):
                if account_idx > max_accounts:
                    i += 1
                    continue
                
                i += 1
                if i < len(lines) and "(UP TO" in lines[i].upper():
                    i += 1
                
                status_rows = []
                month_rows = []
                
                while i < len(lines):
                    row = lines[i].strip()
                    if not row:
                        break
                    
                    if patterns['month_token'].search(row):
                        month_rows.append(row)
                    elif patterns['status_token'].search(row):
                        status_rows.append(row)
                    else:
                        break
                    i += 1
                
                status_tokens_rows = [patterns['status_token'].findall(row) for row in status_rows]
                flat_status_tokens = [t for row in status_tokens_rows for t in row]
                
                month_tokens_rows = [patterns['month_token'].findall(row) for row in month_rows]
                flat_month_tokens = [t for row in month_tokens_rows for t in row]
                
                entries = []
                numeric_statuses = []
                total_pairs = min(len(flat_status_tokens), len(flat_month_tokens))
                
                for idx in range(total_pairs):
                    status = flat_status_tokens[idx]
                    month = flat_month_tokens[idx]
                    numeric = CIBILExtractor._dpd_to_number(status)
                    numeric_statuses.append(numeric)
                    entries.append({
                        "status": status,
                        "numeric_status": numeric,
                        "month": month,
                    })
                
                account_avg = round(sum(numeric_statuses) / len(numeric_statuses), 1) if numeric_statuses else None
                if account_avg is not None:
                    account_averages.append(account_avg)
                
                accounts.append({
                    "account_index": account_idx,
                    "total_entries": len(entries),
                    "numeric_status_count": len([n for n in numeric_statuses if n > 0]),
                    "per_account_dpd_average": account_avg,
                    "entries": entries,
                })
                account_idx += 1
                continue
            i += 1
        
        final_dpd_average = round(sum(account_averages) / len(account_averages), 1) if account_averages else None
        
        return {
            "dpd_blocks": accounts,
            "final_dpd_average": final_dpd_average,
            "accounts_processed": len(accounts),
            "max_accounts_limit": max_accounts
        }



    def extract_enquiries_from_pdf(self, text: str, patterns: Dict) -> list:
        """Extract enquiries from latest month only"""
        enquiry_section_match = patterns['enquiry_date'].search(text)
        if not enquiry_section_match:
            return []
        
        enquiry_section = enquiry_section_match.group(1)
        enquiry_dates = re.findall(r'\b(\d{2}-\d{2}-\d{4})\b', enquiry_section)
        
        parsed_dates = []
        for d in enquiry_dates:
            try:
                day, month, year = map(int, d.split('-'))
                parsed_dates.append(datetime(year, month, day))
            except ValueError:
                pass
        
        if not parsed_dates:
            return []
        
        latest = max(parsed_dates)
        latest_month = latest.month
        latest_year = latest.year
        
        enquiries = []
        for d in parsed_dates:
            if d.month == latest_month and d.year == latest_year:
                enquiries.append({
                    "date": d.strftime("%d-%m-%Y"),
                    "parsed_date": d.strftime("%Y-%m-%d"),
                    "member": None,
                    "purpose": None,
                    "amount": None
                })
        
        return enquiries





# ============== UNIFIED EXTRACTOR CLASSES ==============

# ============== PDF-SPECIFIC DETERIORATION LOGIC ==============

def _extract_dpd_history_pdf(dpd_block: str) -> dict:
    """PDF-specific DPD extraction (flat structure with months)"""
    # Extract all DPD tokens + months
    tokens = DPD_TOKEN_RE.findall(dpd_block)
    months = re.findall(r'\b(\d{2}-\d{2})\b', dpd_block)
    
    
    if not tokens:
        return {}
    
    # Group by year from month stamps
    yearly_dpd = {}
    for idx, month in enumerate(months[:len(tokens)]):  # Pair tokens with months
        if idx < len(tokens):
            mm, yy = month.split('-')
            year = f"20{yy}"
            if year not in yearly_dpd:
                yearly_dpd[year] = []
            yearly_dpd[year].append(tokens[idx])
    
    # Convert to string format (HTML-compatible)
    dpd_history = {year: " ".join(dpds) for year, dpds in yearly_dpd.items()}
    if not dpd_history:
        dpd_history = {"UNKNOWN": " ".join(tokens)}
    
    return dpd_history


def _extract_pdf_account_metadata(block: str) -> dict:
    """Extract account fields from PDF block"""
    account = {}
    
    field_patterns = {
        'member_name': r'MEMBER NAME[:\s]*([^\n]+)',
        'date_opened': r'OPENED[:\s]*(\d{2}-\d{2}-\d{4})',
        'date_closed': r'CLOSED[:\s]*(\d{2}-\d{2}-\d{4})',
        'sanctioned_amount': r'SANCTIONED[:\s]*₹?([\d,]+)',
        'current_balance': r'CURRENT BALANCE[:\s]*₹?([\d,]+)',
        'overdue_amount': r'OVERDUE[:\s]*₹?([\d,]+)',
        'account_type': r'TYPE[:\s]*([^\n]+)',
        'date_reported': r'REPORTED AND CERTIFIED[:\s]*(\d{2}-\d{2}-\d{4})',
    }
    
    for field, pattern in field_patterns.items():
        match = re.search(pattern, block, re.I)
        if match:
            if field in ['sanctioned_amount', 'current_balance', 'overdue_amount']:
                account[field] = CIBILExtractor._clean_amount(match.group(1))
            else:
                account[field] = match.group(1).strip()
    
    return account


class HTMLExtractor(CIBILExtractor):
    """HTML format extractor with unified structure"""
    
    def extract(self, html_path):
        with open(html_path, 'r', encoding='utf-8') as f:
            html_content = f.read()
        
        soup = BeautifulSoup(html_content, 'html.parser')
        
        for element in soup(['script', 'style', 'meta', 'link', 'noscript']):
            element.decompose()
        
        text = soup.get_text(separator='\n', strip=True)
        
        basic_info = self._extract_basic_info(text)
        enquiries = _extract_enquiries_from_html(soup)
        accounts = _extract_accounts_from_html(soup)
        overdue_summary_html = _extract_overdue_summary_from_html_text(text)
        unified_accounts = self._convert_html_accounts_to_unified(accounts)
        accounts = [
    acc for acc in accounts
    if acc.get("default_month_number") is not None
]

        
        return self._build_unified_structure(
            basic_info=basic_info,
            enquiries=enquiries,
            accounts=unified_accounts,
            overdue_summary=overdue_summary_html,
            format_type="HTML"
        )
    
    def _extract_basic_info(self, text):
        report_date = self._first_match(text, self._patterns['report_date'])
        return {
            "name": self._first_match(text, self._patterns['name']),
            "pan_card": self._first_match(text, self._patterns['pan_card']),
            "ckyc": self._first_match(text, self._patterns['ckyc']),
            "report_date": report_date,
            "score": self._first_match(text, self._patterns['score']) or 
                    self._first_match(text, self._patterns['fallback_score']),
            "mobile_numbers": list(set(self._patterns['mobile'].findall(text))) or [],
            "emails": list({e.lower() for e in self._patterns['email'].findall(text)}) or []
        }

    def _convert_html_accounts_to_unified(self, accounts):
        unified = []
        for idx, account in enumerate(accounts, 1):
            dpd_history = account.get('dpd_history', {})
            yearly_averages = []
            
            for year, dpd_string in dpd_history.items():
                # **FIXED: Use strict DPD tokens**
                tokens = DPD_TOKEN_RE.findall(dpd_string)
                numeric_dpd = [
                CIBILExtractor._dpd_to_number(t)
                for t in tokens
                if t.upper() not in {"000", "XXX", "-", "STD"}
                    ]
                
                if numeric_dpd:
                    avg_dpd = round(sum(numeric_dpd) / len(numeric_dpd), 1)
                    yearly_averages.append({"year": year, "average_dpd": avg_dpd})
            
            unified_account = {
                "account_index": idx,
                "original_html_index": account.get('original_html_index') or "N/A",
                "deterioration_reasoning": account.get('deterioration_reasoning') or "",
                "date_opened": account.get('date_opened') or "N/A",
                "date_closed": account.get('date_closed') or "N/A",
                "date_reported": account.get('date_reported') or "N/A",
                "status": account.get('status') or "N/A",
                "account_type": account.get('account_type') or "N/A",
                "member_name": account.get('member_name') or "N/A",
                "sanctioned_amount": account.get('sanctioned_amount', "0"),
                "current_balance": account.get('current_balance', "0"),
                "default_month_number": _calculate_default_month_number(dpd_history) or 0.0,
                "dpd_history": dpd_history,
                "dpd_summary": {
                    "yearly_averages": yearly_averages,
                    "monthly_entries": [],
                    "account_dpd_average": self._calculate_account_dpd_average(yearly_averages) or 0.0
                }
            }
            unified.append(unified_account)
        return unified



    def _calculate_account_dpd_average(self, yearly_averages):
        if not yearly_averages:
            return None
        
        total = 0
        count = 0
        for year_data in yearly_averages:
            avg = year_data.get('average_dpd')
            if avg is not None:
                total += avg
                count += 1
        
        return round(total / count, 1) if count > 0 else None
    
    def _build_unified_structure(self, basic_info, enquiries, accounts, overdue_summary=None, format_type=""):
        account_dpd_averages = []
        default_month_numbers = []
        for account in accounts:  # ✅ Proper indent
            avg = account['dpd_summary']['account_dpd_average']
            if avg is not None:
                account_dpd_averages.append(avg)
            
            default_month = account.get('default_month_number')  # ✅ Proper indent
            if default_month is not None:
                default_month_numbers.append(default_month)  # ✅ Proper indent
        
        final_dpd_average = round(sum(account_dpd_averages) / len(account_dpd_averages), 1) if account_dpd_averages else None
        final_default_month_avg = _calculate_dynamic_final_default_month(accounts) if default_month_numbers else None

        return {
            "basic_info": {
                "name": basic_info.get('name'),
                "pan_card": basic_info.get('pan_card'),
                "ckyc": basic_info.get('ckyc'),
                "report_date": basic_info.get('report_date'),
                "score": basic_info.get('score'),
                "mobile_numbers": basic_info.get('mobile_numbers', []),
                "emails": basic_info.get('emails', [])
            },
            "enquiries": {
                "latest_month_enquiries": enquiries,
                "total_count": len(enquiries)
            },
            "accounts": {
                "accounts_list": accounts,
                "total_accounts_extracted": len(accounts),
                "final_dpd_average": final_dpd_average,
                "final_default_month_average": final_default_month_avg  # ← NEW
            },
            "overdue_summary": {
                "total_overdue_accounts": (overdue_summary or {}).get("total_overdue_accounts"),
                "total_overdue_amount": (overdue_summary or {}).get("total_overdue_amount"),
                "total_current_amount": (overdue_summary or {}).get("total_current_amount")
            },
            "metadata": {
                "format": format_type,
                "extraction_timestamp": datetime.now().isoformat()
            }
        }




class PDFExtractor(CIBILExtractor):
    """PDF format extractor with unified structure"""
    
    def __init__(self):
        super().__init__()
        self.advanced_extractor = PDFAdvancedExtractor()
    
    def _calculate_account_dpd_average(self, yearly_averages):
        if not yearly_averages:
            return None
        total = sum(y.get('average_dpd', 0) for y in yearly_averages)
        return round(total / len(yearly_averages), 1)

    def extract(self, pdf_path):
        with pdfplumber.open(pdf_path) as pdf:
            text = "\n".join(p.extract_text() or "" for p in pdf.pages)
            text = re.sub(r'\n\s*\n+', '\n\n', text)
        
        basic_info = self._extract_basic_info(text)
        overdue_summary = self.advanced_extractor.extract_overdue_summary(text, self._patterns)
        enquiries = self.advanced_extractor.extract_enquiries_from_pdf(text, self._patterns)
        
        # **NEW: PDF deterioration filtering (HTML logic)**
        deteriorating_accounts = self.advanced_extractor.extract_deteriorating_accounts_pdf(text, self._patterns)
        unified_accounts = self._convert_pdf_accounts_to_unified(deteriorating_accounts)
        
        return self._build_unified_structure(
            basic_info=basic_info,
            enquiries=enquiries,
            accounts=unified_accounts,
            overdue_summary=overdue_summary,
            format_type="PDF"
        )

    def _extract_basic_info(self, text):
        score = self.advanced_extractor.extract_score(text)
        
        return {
            "name": self._first_match(text, self._patterns['name']),
            "pan_card": self._first_match(text, self._patterns['pan_card']),
            "ckyc": self._first_match(text, self._patterns['ckyc']),
            "report_date": self._first_match(text, self._patterns['report_date']),
            "score": score or self._first_match(text, self._patterns['score']),
            "mobile_numbers": list(set(self._patterns['mobile'].findall(text))) or [],
            "emails": list({e.lower() for e in self._patterns['email'].findall(text)}) or []
        }
    
    def _convert_pdf_accounts_to_unified(self, accounts):
        """Convert PDF deteriorating accounts to unified format"""
        unified = []
        for idx, account in enumerate(accounts, 1):
            dpd_history = account.get('dpd_history', {})
            yearly_averages = []
            
            # SAME HTML LOGIC for averages
            for year, dpd_string in dpd_history.items():
                tokens = DPD_TOKEN_RE.findall(dpd_string)
                numeric_dpd = [
                CIBILExtractor._dpd_to_number(t)
                for t in tokens
                if t.upper() not in {"000", "XXX", "-", "STD"}
            ]
                if numeric_dpd:
                    avg_dpd = round(sum(numeric_dpd) / len(numeric_dpd), 1)
                    yearly_averages.append({"year": year, "average_dpd": avg_dpd})
            
            unified_account = {
                "account_index": account.get('account_index'),
                "deterioration_reasoning": account.get('deterioration_reasoning'),
                "date_opened": account.get('date_opened'),
                "date_closed": account.get('date_closed'),
                "date_reported": account.get('date_reported'),
                "account_type": account.get('account_type'),
                "member_name": account.get('member_name'),
                "sanctioned_amount": account.get('sanctioned_amount', '0'),
                "current_balance": account.get('current_balance', '0'),
                "overdue_amount": account.get('overdue_amount', '0'),
                "default_month_number": _calculate_default_month_number(dpd_history),
                "dpd_history": dpd_history,
                "dpd_summary": {
                    "yearly_averages": yearly_averages,
                    "monthly_entries": [],
                    "account_dpd_average": self._calculate_account_dpd_average(yearly_averages)
                }
            }
            unified.append(unified_account)
        return unified

    
    def _build_unified_structure(self, basic_info, enquiries, accounts, overdue_summary=None, format_type=""):
        account_dpd_averages = []
        default_month_numbers = []
        
        for account in accounts:
            # DPD averages
            avg = account['dpd_summary']['account_dpd_average']
            if avg is not None:
                account_dpd_averages.append(avg)
            
            # Default month numbers
            default_month = account.get('default_month_number')
            if default_month is not None:
                default_month_numbers.append(default_month)
        
        final_dpd_average = round(sum(account_dpd_averages) / len(account_dpd_averages), 1) if account_dpd_averages else None
        final_default_month_avg = _calculate_dynamic_final_default_month(accounts) if default_month_numbers else None
        
        return {
            "basic_info": {
                "name": basic_info.get('name'),
                "pan_card": basic_info.get('pan_card'),
                "ckyc": basic_info.get('ckyc'),
                "report_date": basic_info.get('report_date'),
                "score": basic_info.get('score'),
                "mobile_numbers": basic_info.get('mobile_numbers', []),
                "emails": basic_info.get('emails', [])
            },
            "enquiries": {
                "latest_month_enquiries": enquiries,
                "total_count": len(enquiries)
            },
            "accounts": {
            "accounts_list": accounts,
            "total_accounts_extracted": len(accounts),
            "final_dpd_average": final_dpd_average,          
            "final_default_month_average": final_default_month_avg
        },
            "overdue_summary": {
                "total_overdue_accounts": (overdue_summary or {}).get("total_overdue_accounts"),
                "total_overdue_amount": (overdue_summary or {}).get("total_overdue_amount"),
                "total_current_amount": (overdue_summary or {}).get("total_current_amount")
            },
            "metadata": {
                "format": format_type,
                "extraction_timestamp": datetime.now().isoformat()
            }
        }

def _default_month_for_year(dpd_string: str) -> Optional[int]:
    CLEAN_STATES = {"000", "XXX", "-", "STD"}
    tokens = [t.upper() for t in DPD_TOKEN_RE.findall(dpd_string)]

    for idx, token in enumerate(tokens, start=1):
        if token not in CLEAN_STATES:
            return idx
    return None

def _default_month_for_specific_year(dpd_history: dict, year: str) -> Optional[int]:
    dpd_string = dpd_history.get(year)
    if not dpd_string:
        return None
    return _default_month_for_year(dpd_string)

def _calculate_dynamic_final_default_month(accounts: list) -> Optional[float]:
    current_year = str(datetime.now().year)
    previous_year = str(datetime.now().year - 1)

    current_year_values = []
    fallback_values = []

    for acc in accounts:
        dpd_history = acc.get("dpd_history", {})

        # current year
        cur = _default_month_for_specific_year(dpd_history, current_year)
        if cur is not None:
            current_year_values.append(cur)

        # previous year (used only if needed)
        prev = _default_month_for_specific_year(dpd_history, previous_year)
        if prev is not None:
            fallback_values.append(prev)

    # RULE 1: Enough data in current year
    if len(current_year_values) >= 5:
        return round(sum(current_year_values) / len(current_year_values), 1)

    # RULE 2: Fallback to current + previous year
    combined = current_year_values + fallback_values
    if combined:
        return round(sum(combined) / len(combined), 1)

    return None
def _calculate_default_month_number(dpd_history: dict) -> Optional[float]:
    """
    FINAL (as per your requirement):
    - Each year has its own default month
    - Account default = average of yearly defaults
    """
    yearly_defaults = []

    for dpd_string in dpd_history.values():
        month = _default_month_for_year(dpd_string)
        if month is not None:
            yearly_defaults.append(month)

    if not yearly_defaults:
        return None

    return round(sum(yearly_defaults) / len(yearly_defaults), 1)


# ============== MAIN UNIFIED EXTRACTOR ==============

class CIBILDataExtractor:
    """Main unified extractor for both HTML and PDF formats"""
    
    def __init__(self):
        self._extractors = {
            '.pdf': PDFExtractor(),
            '.html': HTMLExtractor(),
            '.htm': HTMLExtractor()
        }
    
    def extract(self, file_path):
        fp = Path(file_path)
        if not fp.exists():
            raise FileNotFoundError(f"File not found: {file_path}")
        
        ext = fp.suffix.lower()
        if ext not in self._extractors:
            raise ValueError(f"Unsupported format: {ext}")
        
        return self._extractors[ext].extract(file_path)

def main():
    extractor = CIBILDataExtractor()
    input_file = "/Users/mruthunjai_govindaraju/Downloads/Ajay_B_M.pdf"
    try:
        result = extractor.extract(input_file)
        # print(json.dumps(result, indent=2))
        with open("extracted_output.json", "w", encoding="utf-8") as out_file:
            json.dump(result, out_file, indent=2)
        # print(f"\n✅ Extraction complete! Saved to extracted_output.json")
    except Exception as e:
        print(f"Error: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    main()