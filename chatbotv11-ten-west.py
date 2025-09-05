# app_agent_streamlit.py
# -*- coding: utf-8 -*-
import os
import re
import glob
import difflib
import math
import json
from decimal import Decimal, ROUND_HALF_UP
from datetime import datetime, timedelta, date
from typing import Optional, List
from collections import defaultdict

import pandas as pd
import streamlit as st
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# ──────────────────────────────────────────────────────────────────────────────
# 0) CREDENTIALS & STATIC PATHS 
# ──────────────────────────────────────────────────────────────────────────────
# Load OpenAI API key from environment variable
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
if not OPENAI_API_KEY:
    st.error("OPENAI_API_KEY environment variable not found. Please check your .env file.")
    st.stop()
os.environ["OPENAI_API_KEY"] = OPENAI_API_KEY

METADATA_PATH          = "metadata_cache.json"

ORDER_PATH             = "SP Ten West 20250811.xlsx"
ASIN_REPORT_PATH       = "full_detailed_aggregated_report_ten_west.xlsx"
BRAND_REPORT_PATH      = "full_detailed_aggregated_report_brand_ten_west.xlsx"
GM_REPORT_PATH         = "full_detailed_aggregated_report_gm_ten_west.xlsx"
UNDERPER_REPORT       = "underperforming_ats_report_ten_west.xlsx"
BUSINESS_REPORT_SEARCH_DIRS = [".", "./reports", "./data"]
BRAND_MAP_PATH         = "GM, Brand and ASIN.xlsx"

# Logging configuration
LOG_FILE_PATH = "chatbot_logs.jsonl"
LOG_DIR = "logs"

# Create logs directory if it doesn't exist
if not os.path.exists(LOG_DIR):
    os.makedirs(LOG_DIR)

SYSTEM_PROMPT = (
    "You are a meticulous Amazon business analyst. "
    "Decide which tool to call based on the user's text. "
    "IMPORTANT: Pay close attention to whether the user is asking for: "
    "1) AGGREGATED ASIN summaries (average sales price across all orders for an ASIN) "
    "2) INDIVIDUAL ORDERS for a specific ASIN (order-by-order breakdown) "
    "Tools return the data into Streamlit session_state; keep the final answer concise."
)

# ──────────────────────────────────────────────────────────────────────────────
# 1) UTILS  (COPIED FROM YOUR APP)
# ──────────────────────────────────────────────────────────────────────────────
def fmt_date(dt: date) -> str:
    s = dt.strftime("%B %d, %Y")
    return s.replace(" 0", " ")

BUSINESS_REPORT_REGEX = re.compile(
    r"^BusinessReport\s+(\d{1,2})-(\d{2})-(\d{2})-(\d{1,2})-(\d{2})-(\d{2})\.xlsx$",
    re.IGNORECASE
)

def _two(y: int) -> str:
    return f"{y % 100:02d}"

def _mddyy(d: date) -> tuple[str, str, str]:
    return str(d.month), f"{d.day:02d}", _two(d.year)

def build_business_report_filename(start: date, end: date) -> str:
    sm, sd, sy = _mddyy(start)
    em, ed, ey = _mddyy(end)
    return f"BusinessReport {sm}-{sd}-{sy}-{em}-{ed}-{ey}.xlsx"

def parse_business_report_filename(name: str):
    m = BUSINESS_REPORT_REGEX.match(name.strip())
    if not m:
        return None
    sm, sd, sy, em, ed, ey = map(int, m.groups())
    sy += 2000; ey += 2000
    try:
        s = date(sy, sm, sd); e = date(ey, em, ed)
        return (s, e)
    except Exception:
        return None

def resolve_base_date_from_prompt(prompt_text: str) -> date:
    base = datetime.today().date()
    low = prompt_text.lower()
    if "tomorrow" in low:
        base = base + timedelta(days=1)
    elif "yesterday" in low:
        base = base - timedelta(days=1)
    return base

def compute_week_window_from_prompt(prompt_text: str) -> tuple[date, date]:
    base = resolve_base_date_from_prompt(prompt_text)
    # For "last week", we want the 7 days ending yesterday
    # So if today is 24th, we want 17-23
    # If today is 25th, we want 18-24
    end = base - timedelta(days=1)  # yesterday
    start = end - timedelta(days=6)  # 7 days before yesterday
    return start, end

def find_weekly_business_report(start: date, end: date) -> tuple[str, str]:
    target_name = build_business_report_filename(start, end)
    candidates: List[str] = []
    for d in BUSINESS_REPORT_SEARCH_DIRS:
        candidates.extend(glob.glob(os.path.join(d, "BusinessReport *.xlsx")))

    for p in candidates:
        if os.path.basename(p).lower() == target_name.lower():
            return p, f"{fmt_date(start)} – {fmt_date(end)}"

    parsed = []
    for p in candidates:
        rng = parse_business_report_filename(os.path.basename(p))
        if rng:
            parsed.append((p, rng[0], rng[1]))
    same_end = [p for p, s, e in parsed if e == end]
    if same_end:
        p = sorted(same_end)[0]
        return p, f"{fmt_date(start)} – {fmt_date(end)}"

    prior = [(p, s, e) for p, s, e in parsed if e <= end]
    if prior:
        p, s, e = max(prior, key=lambda x: x[2])
        period = f"{fmt_date(s)} – {fmt_date(e)}"
        return p, period

    raise FileNotFoundError(
        f"No weekly 'BusinessReport *.xlsx' found up to {fmt_date(end)} "
        f"(searched in: {', '.join(BUSINESS_REPORT_SEARCH_DIRS)})."
    )

def find_week_sheet(inv_xls: pd.ExcelFile, store: str) -> Optional[str]:
    store_kw = store.lower().replace(" ", "")
    candidates = []
    for s in inv_xls.sheet_names:
        low = str(s).lower().replace(" ", "")
        if all(k in low for k in ["page", "sales", "traffic", "(7)"]) and store_kw in low:
            candidates.append(s)
    if candidates:
        return sorted(candidates, key=len, reverse=True)[0]
    return None

def load_brand_map(path: str = BRAND_MAP_PATH) -> dict:
    if not os.path.exists(path):
        return {}
    gm_df = pd.read_excel(path)
    gm_df.columns = gm_df.columns.str.strip()
    if "Brand" in gm_df.columns and "Brands" not in gm_df.columns:
        gm_df.rename(columns={"Brand": "Brands"}, inplace=True)
    gm_df["ASIN"]   = gm_df["ASIN"].astype(str).str.upper().str.strip()
    gm_df["Brands"] = gm_df["Brands"].astype(str).str.upper().str.strip()
    return (
        gm_df.dropna(subset=["ASIN", "Brands"])
             .drop_duplicates(subset="ASIN", keep="first")
             .set_index("ASIN")["Brands"]
             .to_dict()
    )

def read_weekly_inventory(prompt: str) -> tuple[pd.ExcelFile, str, str]:
    """Return xls, period_text, chosen_filename."""
    week_start, week_end = compute_week_window_from_prompt(prompt)
    path, human_period = find_weekly_business_report(week_start, week_end)
    return pd.ExcelFile(path), human_period, os.path.basename(path)

def check_data_availability() -> dict:
    """
    Check if required data files exist and return availability status.
    Returns a dictionary with file paths as keys and boolean availability as values.
    """
    required_files = {
        "metadata_cache": METADATA_PATH,
        "order_data": ORDER_PATH,
        "asin_report": ASIN_REPORT_PATH,
        "brand_report": BRAND_REPORT_PATH,
        "gm_report": GM_REPORT_PATH,
        "underper_report": UNDERPER_REPORT,
        "brand_map": BRAND_MAP_PATH
    }
    
    availability = {}
    for name, path in required_files.items():
        availability[name] = os.path.exists(path)
    
    return availability

def get_contextual_help(user_question: str) -> str:
    """
    Provide contextual help based on the user's question when tools aren't available.
    """
    question_lower = user_question.lower()
    
    # Check for specific patterns and provide targeted help
    if any(word in question_lower for word in ['gross profit', 'profit', 'margin', 'gp', 'rgp']):
        return (
            "**💰 Understanding Gross Profit & Margins:**\n\n"
            "**Gross Profit Calculation:**\n"
            "• Gross Profit = Revenue - Cost of Goods Sold\n"
            "• pleas % = (Gross Profit / Revenue) × 100\n\n"
            "**Amazon-Specific Considerations:**\n"
            "• Amazon referral fees: 6-15% depending on category\n"
            "• Fulfillment fees: Based on product size and weight\n"
            "• Storage fees: Monthly charges for FBA inventory\n"
            "• Advertising costs: PPC campaign expenses\n\n"
            "**Best Practices:**\n"
            "• Target 15-30% gross margin for Amazon products\n"
            "• Factor in all Amazon fees when pricing\n"
            "• Monitor margins regularly and adjust pricing\n"
            "• Consider bundling products to increase margins\n"
        )
    
    elif any(word in question_lower for word in ['sales', 'revenue', 'ats', 'net sales']):
        return (
            "**📈 Sales Analysis Best Practices:**\n\n"
            "**Key Metrics to Track:**\n"
            "• Daily, weekly, monthly sales trends\n"
            "• Sales velocity (units sold per day)\n"
            "• Revenue per ASIN\n"
            "• Seasonal patterns and trends\n\n"
            "**Amazon Business Reports:**\n"
            "• Use Amazon's Business Reports for detailed insights\n"
            "• Track ATS (Amazon Top-line Sales)\n"
            "• Monitor net sales after fees\n"
            "• Analyze sales by category and brand\n\n"
            "**Optimization Tips:**\n"
            "• Identify your best-selling products\n"
            "• Focus on high-velocity items\n"
            "• Optimize pricing for maximum revenue\n"
            "• Use Amazon advertising to boost sales\n"
        )
    
    elif any(word in question_lower for word in ['buy box', 'buybox', 'suppressed']):
        return (
            "**🏆 Buy Box Optimization Strategies:**\n\n"
            "**What is the Buy Box?**\n"
            "• The featured seller position on Amazon product pages\n"
            "• Determines which seller gets the sale\n"
            "• Based on multiple factors including price, shipping, and performance\n\n"
            "**How to Win the Buy Box:**\n"
            "• Maintain competitive pricing\n"
            "• Use FBA (Fulfillment by Amazon) for fast shipping\n"
            "• Keep adequate inventory levels\n"
            "• Maintain excellent seller metrics\n"
            "• Monitor competitor pricing regularly\n\n"
            "**Avoiding Suppression:**\n"
            "• Keep Buy Box percentage above 80%\n"
            "• Address any account health issues promptly\n"
            "• Ensure accurate product listings\n"
            "• Provide excellent customer service\n"
        )
    
    elif any(word in question_lower for word in ['conversion', 'conversion rate']):
        return (
            "**🔄 Conversion Rate Optimization:**\n\n"
            "**What is Conversion Rate?**\n"
            "• Percentage of visitors who make a purchase\n"
            "• Key metric for measuring listing effectiveness\n\n"
            "**Optimization Strategies:**\n"
            "• Use high-quality, professional product images\n"
            "• Write compelling, detailed product descriptions\n"
            "• Include relevant keywords naturally\n"
            "• Encourage and respond to customer reviews\n"
            "• Use Amazon PPC to increase visibility\n\n"
            "**Best Practices:**\n"
            "• A/B test different listing elements\n"
            "• Monitor competitor listings\n"
            "• Keep prices competitive\n"
            "• Ensure fast, reliable shipping\n"
            "• Provide excellent customer service\n"
        )
    
    elif any(word in question_lower for word in ['fees', 'referral', 'fulfillment']):
        return (
            "**💸 Understanding Amazon Fees:**\n\n"
            "**Referral Fees:**\n"
            "• 6-15% depending on product category\n"
            "• Applied to the selling price\n"
            "• Varies by category (electronics, books, etc.)\n\n"
            "**Fulfillment Fees:**\n"
            "• Based on product size and weight\n"
            "• FBA fees include picking, packing, and shipping\n"
            "• Self-fulfillment has different fee structure\n\n"
            "**Other Fees:**\n"
            "• Storage fees: Monthly charges for FBA inventory\n"
            "• Advertising fees: PPC campaign costs\n"
            "• Subscription fees: Professional seller account\n\n"
            "**Fee Optimization:**\n"
            "• Choose appropriate product categories\n"
            "• Optimize product packaging for lower fees\n"
            "• Monitor storage fees and manage inventory\n"
            "• Use FBA strategically for competitive advantage\n"
        )
    
    elif any(word in question_lower for word in ['asin', 'product', 'item']):
        return (
            "**📦 Product Management Best Practices:**\n\n"
            "**ASIN Performance Tracking:**\n"
            "• Monitor individual ASIN performance\n"
            "• Track sales velocity and profitability\n"
            "• Analyze customer reviews and feedback\n"
            "• Compare performance across similar products\n\n"
            "**Inventory Management:**\n"
            "• Maintain adequate stock levels\n"
            "• Avoid stockouts and overstock situations\n"
            "• Use Amazon's inventory forecasting tools\n"
            "• Plan for seasonal demand fluctuations\n\n"
            "**Product Optimization:**\n"
            "• Optimize product listings regularly\n"
            "• Update images and descriptions\n"
            "• Monitor competitor products\n"
            "• Consider product bundling strategies\n"
        )
    
    else:
        return (
            "**🤖 General Amazon Business Analysis:**\n\n"
            "**Key Areas to Focus On:**\n"
            "• **Profitability**: Monitor gross margins and net profit\n"
            "• **Sales Performance**: Track revenue trends and velocity\n"
            "• **Competitive Position**: Monitor Buy Box percentage and pricing\n"
            "• **Customer Satisfaction**: Maintain high ratings and reviews\n"
            "• **Inventory Management**: Balance stock levels and costs\n\n"
            "**Best Practices:**\n"
            "• Use Amazon's analytics tools regularly\n"
            "• Monitor competitor activity\n"
            "• Optimize listings continuously\n"
            "• Plan for seasonal trends\n"
            "• Maintain excellent customer service\n"
        )

def log_conversation(user_question: str, ai_response: str, session_id: str = None, data_availability: dict = None, error_occurred: bool = False):
    """
    Log user questions and AI responses to a JSONL file with timestamps and metadata.
    
    Args:
        user_question: The user's input question
        ai_response: The AI's response
        session_id: Unique session identifier
        data_availability: Dictionary showing which data files are available
        error_occurred: Whether an error occurred during processing
    """
    try:
        # Generate session ID if not provided
        if not session_id:
            session_id = f"session_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        
        # Create log entry
        log_entry = {
            "timestamp": datetime.now().isoformat(),
            "session_id": session_id,
            "user_question": user_question,
            "ai_response": ai_response,
            "data_availability": data_availability,
            "error_occurred": error_occurred,
            "response_length": len(ai_response) if ai_response else 0,
            "question_length": len(user_question) if user_question else 0
        }
        
        # Write to JSONL file (one JSON object per line)
        log_file_path = os.path.join(LOG_DIR, LOG_FILE_PATH)
        with open(log_file_path, 'a', encoding='utf-8') as f:
            f.write(json.dumps(log_entry, ensure_ascii=False) + '\n')
            
    except Exception as e:
        # If logging fails, don't break the main application
        print(f"Warning: Failed to log conversation: {e}")

def get_session_id():
    """
    Get or create a unique session ID for the current Streamlit session.
    """
    if 'session_id' not in st.session_state:
        st.session_state['session_id'] = f"session_{datetime.now().strftime('%Y%m%d_%H%M%S')}_{id(st.session_state)}"
    return st.session_state['session_id']

def read_conversation_logs(limit: int = 100) -> List[dict]:
    """
    Read conversation logs from the JSONL file.
    
    Args:
        limit: Maximum number of log entries to return
        
    Returns:
        List of log entries as dictionaries
    """
    try:
        log_file_path = os.path.join(LOG_DIR, LOG_FILE_PATH)
        if not os.path.exists(log_file_path):
            return []
        
        logs = []
        with open(log_file_path, 'r', encoding='utf-8') as f:
            for line in f:
                if line.strip():
                    try:
                        log_entry = json.loads(line.strip())
                        logs.append(log_entry)
                    except json.JSONDecodeError:
                        continue
        
        # Return the most recent logs first
        return logs[-limit:] if limit else logs
        
    except Exception as e:
        print(f"Warning: Failed to read conversation logs: {e}")
        return []

def get_log_statistics() -> dict:
    """
    Get statistics about the conversation logs.
    
    Returns:
        Dictionary with log statistics
    """
    try:
        logs = read_conversation_logs()
        if not logs:
            return {
                "total_conversations": 0,
                "total_questions": 0,
                "total_responses": 0,
                "error_rate": 0,
                "avg_question_length": 0,
                "avg_response_length": 0,
                "unique_sessions": 0,
                "date_range": "No data"
            }
        
        # Calculate statistics
        total_conversations = len(logs)
        total_questions = sum(1 for log in logs if log.get('user_question'))
        total_responses = sum(1 for log in logs if log.get('ai_response'))
        errors = sum(1 for log in logs if log.get('error_occurred', False))
        error_rate = (errors / total_conversations * 100) if total_conversations > 0 else 0
        
        avg_question_length = sum(log.get('question_length', 0) for log in logs) / total_conversations if total_conversations > 0 else 0
        avg_response_length = sum(log.get('response_length', 0) for log in logs) / total_conversations if total_conversations > 0 else 0
        
        unique_sessions = len(set(log.get('session_id') for log in logs))
        
        # Date range
        timestamps = [log.get('timestamp') for log in logs if log.get('timestamp')]
        if timestamps:
            try:
                dates = [datetime.fromisoformat(ts) for ts in timestamps]
                date_range = f"{min(dates).strftime('%Y-%m-%d')} to {max(dates).strftime('%Y-%m-%d')}"
            except:
                date_range = "Unknown"
        else:
            date_range = "No data"
        
        return {
            "total_conversations": total_conversations,
            "total_questions": total_questions,
            "total_responses": total_responses,
            "error_rate": round(error_rate, 2),
            "avg_question_length": round(avg_question_length, 1),
            "avg_response_length": round(avg_response_length, 1),
            "unique_sessions": unique_sessions,
            "date_range": date_range
        }
        
    except Exception as e:
        print(f"Warning: Failed to calculate log statistics: {e}")
        return {}

def get_training_status_message(availability: dict, user_question: str = "") -> str:
    """
    Generate a helpful message about training status and suggestions.
    """
    missing_files = [name for name, exists in availability.items() if not exists]
    
    if not missing_files:
        return None  # All files available
    
    message = "🤖 **I haven't been fully trained yet** - some required data files are missing.\n\n"
    message += "**Missing data files:**\n"
    for file in missing_files:
        message += f"• {file}\n"
    
    # Add specific suggestions based on user's question
    if user_question:
        message += f"\n**Based on your question about \"{user_question}\", here are some helpful suggestions:**\n\n"
        message += get_contextual_help(user_question)
        message += "\n"
    
    message += "**📊 General Business Analysis:**\n"
    message += "• How to analyze Amazon seller performance\n"
    message += "• Best practices for Amazon inventory management\n"
    message += "• Understanding Amazon metrics (Buy Box, conversion rates, etc.)\n"
    message += "• Tips for improving Amazon seller ranking\n"
    message += "• Strategies for Amazon PPC campaigns\n\n"
    
    message += "**📈 Performance Optimization:**\n"
    message += "• How to calculate gross margins\n"
    message += "• Understanding Amazon fees structure\n"
    message += "• Strategies for increasing Buy Box percentage\n"
    message += "• Ways to improve conversion rates\n"
    message += "• Inventory forecasting best practices\n\n"
    
    message += "**🔧 Technical Questions:**\n"
    message += "• How to interpret Amazon business reports\n"
    message += "• Understanding settlement periods\n"
    message += "• Reading Amazon analytics data\n"
    message += "• Best practices for data analysis\n\n"
    
    message += "**💡 Pro Tips:**\n"
    message += "• Focus on high-margin products\n"
    message += "• Monitor competitor pricing regularly\n"
    message += "• Optimize product listings for better visibility\n"
    message += "• Use Amazon's advertising tools effectively\n"
    message += "• Track key performance indicators (KPIs)\n\n"
    
    message += "Once the missing data files are available, I'll be able to provide specific analysis for your business data!"
    
    return message

# ──────────────────────────────────────────────────────────────────────────────
# 2) LOAD CORE (for settlement_period string)
# ──────────────────────────────────────────────────────────────────────────────
order_df = pd.read_excel(ORDER_PATH, sheet_name="Transaction Report", skiprows=7)
date_col = next(c for c in order_df.columns if "date" in c.lower())
order_df[date_col] = order_df[date_col].astype(str).str.replace(r"\s[A-Z]{3,4}$", "", regex=True)
order_df[date_col] = pd.to_datetime(order_df[date_col], errors="coerce")
start_date = order_df[date_col].min().date()
end_date   = order_df[date_col].max().date()
settlement_period = f"{fmt_date(start_date)} – {fmt_date(end_date)}"

brand_map = load_brand_map()
# Extra datasets used by the new tools
asin_df = pd.read_excel(ASIN_REPORT_PATH)                         # ASIN-level totals (ATS, quantity, Brands)
asin_df = asin_df.drop(asin_df.tail(1).index)  # drop trailing total row if present

# Convert "quantity" to float
asin_df["quantity"] = (
    asin_df["quantity"]
      .astype(str)
      .str.replace(",", "")
      .astype(float)
)

unit_df = pd.read_excel(ORDER_PATH, sheet_name="Unit Financial")  # Planned per-unit price
business_df = pd.read_excel(ORDER_PATH, sheet_name="Business Report")         # SKU <-> (Child) ASIN map


# ──────────────────────────────────────────────────────────────────────────────
# 3) TOOL LOGIC (called by the agent)
# ──────────────────────────────────────────────────────────────────────────────
def _to_money_float(x) -> float:
    """Strip $ and commas → float."""
    s = str(x)
    s = re.sub(r"[^\d.\-]", "", s)
    return float(s) if s else 0.0

def _round_cents(x: float) -> float:
    """Banker-safe round half up to 2dp using Decimal."""
    return float(Decimal(str(x)).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP))

def tool_asins_avg_price_below_plan(_: str) -> str:
    """
    Finds ASINs where Actual Avg Sales Price < Planned Per-Unit price.
    Saves a formatted table in session_state['below_plan_table'].
    """
    period_str = settlement_period

    # 1) Base from asin_df
    df = asin_df[["ASIN", "Amazon Top-line Sales (ATS)", "quantity", "Brands"]].copy()
    df["Total_ATS"]  = df["Amazon Top-line Sales (ATS)"].map(_to_money_float)
    df["Units_Sold"] = pd.to_numeric(df["quantity"], errors="coerce").fillna(0).astype(int)
    df["Actual_Avg_Price"] = df["Total_ATS"] / df["Units_Sold"].replace(0, pd.NA)

    # 2) Planned per-unit
    planned = unit_df[["ASIN", "REACH'S FLOOR PRICE"]].rename(
        columns={"REACH'S FLOOR PRICE": "Planned_Per_Unit"}
    )
    planned["Planned_Per_Unit"] = planned["Planned_Per_Unit"].map(_to_money_float)

    # 3) Merge + keep only below plan
    df = df.merge(planned, on="ASIN", how="left")
    df = df[df["Actual_Avg_Price"] < df["Planned_Per_Unit"]].copy()
    if df.empty:
        st.session_state["agent_error"] = f"No ASINs with average sales price below plan for {period_str}."
        return "no_data"

    # 4) Delta & lost revenue
    df["Delta"] = (df["Planned_Per_Unit"] - df["Actual_Avg_Price"]).map(_round_cents)
    df = df[df["Delta"] > 0]
    if df.empty:
        st.session_state["agent_error"] = f"No positive deltas after rounding for {period_str}."
        return "no_data"
    df["Total Lost Revenue"] = (df["Delta"] * df["Units_Sold"]).map(_round_cents)

    # 5) Pretty output
    out = pd.DataFrame({
        "Brand":  df["Brands"],
        "ASIN":   df["ASIN"],
        "Planned Sales Price": df["Planned_Per_Unit"].map("${:,.2f}".format),
        "Average Sales Price": df["Actual_Avg_Price"].map("${:,.2f}".format),
        "Delta":               df["Delta"].map("${:,.2f}".format),
        "Units Sold":          df["Units_Sold"].map("{:,}".format),
        "Total Lost Revenue":  df["Total Lost Revenue"].map("${:,.2f}".format),
    })
    out.index = range(1, len(out) + 1)

    # Save for UI
    st.session_state["below_plan_table"] = out
    st.session_state["below_plan_period"] = period_str
    return "ok"


def tool_asins_where_avg_price_lower_than_plan(prompt: str) -> str:
    """
    Synonym of the previous tool (different phrasing). Reuses its logic.
    """
    return tool_asins_avg_price_below_plan(prompt)


def tool_orders_below_plan_for_asin(prompt: str) -> str:
    """
    Show INDIVIDUAL ORDERS where per-order Avg Sales Price < planned price for a specific ASIN.
    This is DIFFERENT from tool_asins_avg_price_below_plan which shows aggregated ASIN summaries.
    Expects an ASIN in the prompt (B0XXXXXXXX). Saves table in session_state['orders_table'].
    """
    m = re.search(r"\basin\s+(B0[0-9A-Z]{8})\b", prompt, flags=re.IGNORECASE)
    if not m:
        # also accept "for B0xxxx..." without the word "asin"
        m = re.search(r"\b(B0[0-9A-Z]{8})\b", prompt, flags=re.IGNORECASE)
    if not m:
        st.session_state["agent_error"] = "Please include the ASIN (e.g., B0XXXXXXXX) in your request."
        return "no_data"

    asin_q = m.group(1).upper()

    # "did you mean?" fallback
    all_asins = asin_df["ASIN"].astype(str).str.upper().tolist()
    if asin_q not in all_asins:
        suggestion = difflib.get_close_matches(asin_q, all_asins, n=1, cutoff=0.6)
        if suggestion:
            asin_q = suggestion[0]

    # planned unit price
    plan_ser = (
        unit_df.loc[unit_df["ASIN"].astype(str).str.upper() == asin_q, "REACH'S FLOOR PRICE"]
        .dropna()
        .astype(str)
    )
    if plan_ser.empty:
        st.session_state["agent_error"] = f"No planned sales price found for ASIN {asin_q}."
        return "no_data"
    plan_price = _to_money_float(plan_ser.iloc[0])

    # Build SKU->ASIN map
    sku_asin_map = (
        business_df[["SKU", "(Child) ASIN"]]
        .rename(columns={"(Child) ASIN": "ASIN"})
        .assign(
            SKU=lambda d: d["SKU"].astype(str).str.upper().str.strip(),
            ASIN=lambda d: d["ASIN"].astype(str).str.upper().str.strip(),
        )
        .drop_duplicates(subset="SKU", keep="first")
    )

    df = (
        order_df
        .assign(
            SKU=lambda d: d["sku"].astype(str).str.upper().str.strip(),
            type=lambda d: d["type"].astype(str).str.lower().str.strip(),
        )
        .merge(sku_asin_map, on="SKU", how="left")
    )

    df = df[(df["type"] == "order") & (df["ASIN"] == asin_q)].copy()
    if df.empty:
        st.session_state["agent_error"] = (
            f"No individual orders for ASIN {asin_q} below plan during {settlement_period}."
        )
        return "no_data"

    prod_col = next(c for c in df.columns if "product" in c.lower() and "sales" in c.lower())
    df.rename(columns={prod_col: "Product Sales"}, inplace=True)
    df["Product Sales num"] = df["Product Sales"].map(_to_money_float)
    df["Quantity"] = pd.to_numeric(df["quantity"], errors="coerce").fillna(0).astype(int)
    df["Order Price"] = df["Product Sales num"] / df["Quantity"].replace(0, pd.NA)

    df = df[df["Order Price"] < plan_price].copy()
    if df.empty:
        st.session_state["agent_error"] = (
            f"No orders for ASIN {asin_q} had an average sales price below plan "
            f"in {settlement_period}."
        )
        return "no_data"

    df["Avg Sales Price"]    = df["Order Price"].map(_round_cents)
    df["Expected Total ATS"] = plan_price
    df["Delta"]              = (plan_price - df["Avg Sales Price"]).map(_round_cents)
    df = df[df["Delta"] > 0]

    brand_lookup = asin_df.set_index(asin_df["ASIN"].str.upper())["Brands"].to_dict()
    df["Brand"] = df["ASIN"].map(brand_lookup).fillna("Unknown")

    out = df[[
        "Brand", "ASIN", "sku", "order id", "date/time",
        "Quantity", "Product Sales", "Avg Sales Price", "Expected Total ATS", "Delta"
    ]].rename(columns={"sku": "SKU", "order id": "Order ID", "date/time": "Date/Time"})

    # Format money cols
    out["Product Sales"]      = df["Product Sales num"].map("${:,.2f}".format)
    out["Avg Sales Price"]    = out["Avg Sales Price"].map("${:,.2f}".format)
    out["Expected Total ATS"] = out["Expected Total ATS"].map("${:,.2f}".format)
    out["Delta"]              = out["Delta"].map("${:,.2f}".format)

    out = out.reset_index(drop=True)
    out.index = range(1, len(out) + 1)
    out.index.name = "No."

    # Save for UI
    st.session_state["orders_table"] = out
    st.session_state["orders_period"] = settlement_period
    st.session_state["orders_asin"] = asin_q
    return "ok"


def tool_gross_sales_by_asin(prompt: str) -> str:
    """
    Show gross sales by ASIN for last X days (7, 14, or 30 days).
    Examples: 'Show gross sales by ASIN for last week', 'Show the gross sales by ASIN for 14 days'.
    Saves table in session_state['gross_sales_table'].
    """
    # Parse the time period from the prompt
    m = re.search(
        r"\b(?:last\s+week|7\s*days|last\s+14\s*days|14\s*days|"
        r"last\s+2\s*weeks|2\s*weeks|last\s+30\s*days|30\s*days|last\s+month)\b",
        prompt.lower()
    )
    if not m:
        st.session_state["agent_error"] = "Please specify a time period (e.g., 'last week', '14 days', 'last month')."
        return "no_data"
    
    tok = m.group(0)
    if '2' in tok and 'week' not in tok:
        days = 14
    elif '30' in tok or 'month' in tok:
        days = 30
    else:
        days = 7

    # Get the business report for the period
    try:
        inv_xls, period_text, fname = read_weekly_inventory(prompt)
    except FileNotFoundError as e:
        st.session_state["agent_error"] = str(e)
        return "no_data"

    # Build sheet name mapping for different stores and days
    sheet_name_by_store_and_days = {}
    for store in ("Product", "Brand", "TenWest"):
        sheet_name_by_store_and_days[store] = {}
        for d in [7, 14, 30]:
            sheet = find_week_sheet(inv_xls, store)
            if sheet:
                sheet_name_by_store_and_days[store][d] = sheet

    # accumulate per‐ASIN gross sales
    asin_sales = defaultdict(float)
    for store in ("Product", "Brand", "TenWest"):
        sheet = sheet_name_by_store_and_days[store].get(days)
        if not sheet:
            continue
        df_sh = pd.read_excel(inv_xls, sheet_name=sheet)
        cols = {c.lower(): c for c in df_sh.columns}
        asin_col = next((orig for low, orig in cols.items() if "asin" in low and "parent" not in low), None)
        sales_col = next((orig for low, orig in cols.items()
                          if "ordered" in low and "product" in low and "sales" in low), None)
        if not (asin_col and sales_col):
            continue

        df_sh[sales_col] = pd.to_numeric(
            df_sh[sales_col].astype(str).str.replace(r"[\$,]", "", regex=True),
            errors="coerce"
        ).fillna(0.0)

        for a, grp in df_sh.groupby(asin_col):
            asin_key = str(a).upper()
            asin_sales[asin_key] += grp[sales_col].sum()

    if not asin_sales:
        st.session_state["agent_error"] = f"No gross sales data found for the last {days} days."
        return "no_data"

    # build brand map, forcing ALL CAPS
    brands_df = pd.read_excel("Brands and ASINs list.xlsx")
    brands_df.columns = brands_df.columns.str.strip()
    brand_map = {
        str(a).upper(): str(b).upper()
        for a, b in zip(
            brands_df["ASIN"].astype(str),
            brands_df["Brands"].astype(str)
        )
    }

    # build output rows
    rows = []
    total = 0.0
    for a, s in asin_sales.items():
        total += s
        rows.append({
            "Brand": brand_map.get(a, "UNKNOWN"),
            "ASIN":  a,
            "Sales": f"${s:,.2f}"
        })

    out = (
        pd.DataFrame(rows)
          .sort_values("Sales", key=lambda col: col.str.replace(r"[^\d\.]", "", regex=True).astype(float),
                       ascending=False)
          .reset_index(drop=True)
    )
    out.index = out.index + 1
    out.index.name = "No."

    # Save for UI
    st.session_state["gross_sales_table"] = out
    st.session_state["gross_sales_period"] = period_text
    st.session_state["gross_sales_total"] = total
    st.session_state["gross_sales_days"] = days
    st.session_state["business_file"] = fname
    return "ok"


def tool_gross_sales_total(prompt: str) -> str:
    """
    Show total gross sales for last week.
    Example: 'Show the gross sales for last week'.
    Saves value in session_state['gross_sales_total_only'].
    """
    days = 7
    
    # Get the business report for the period
    try:
        inv_xls, period_text, fname = read_weekly_inventory(prompt)
    except FileNotFoundError as e:
        st.session_state["agent_error"] = str(e)
        return "no_data"

    # Build sheet name mapping for different stores and days
    sheet_name_by_store_and_days = {}
    for store in ("Product", "Brand", "TenWest"):
        sheet_name_by_store_and_days[store] = {}
        for d in [7, 14, 30]:
            sheet = find_week_sheet(inv_xls, store)
            if sheet:
                sheet_name_by_store_and_days[store][d] = sheet

    total_sales = 0.0

    # 1) Sum up Ordered Product Sales across the last-7-day sheets
    for store in ("Product", "Brand", "TenWest"):
        sheet = sheet_name_by_store_and_days.get(store, {}).get(days)
        if not sheet:
            continue
        df_sh = pd.read_excel(inv_xls, sheet_name=sheet)
        # find the "Ordered Product Sales" column
        cols = {c.lower(): c for c in df_sh.columns}
        sales_col = next(
            (orig for low, orig in cols.items()
             if "ordered" in low and "product" in low and "sales" in low),
            None
        )
        if not sales_col:
            continue

        # clean and sum this sheet's sales
        s_series = (
            df_sh[sales_col]
              .astype(str)
              .str.replace(r"[\$,]", "", regex=True)
              .pipe(pd.to_numeric, errors="coerce")
              .fillna(0.0)
        )
        total_sales += s_series.sum()

    # Save for UI
    st.session_state["gross_sales_total_only"] = total_sales
    st.session_state["gross_sales_period_only"] = period_text
    st.session_state["business_file"] = fname
    return "ok"


def tool_net_sales_by_asin(prompt: str) -> str:
    """
    Show net sales by ASIN for the settlement period.
    Examples: 'Show net sales by ASIN for last week', 'Show the net sales by ASIN'.
    Saves table in session_state['net_sales_table'].
    """
    # pull Brand, ASIN and ATS (top-line sales)
    df_net = (
        asin_df[["Brands", "ASIN", "Amazon Top-line Sales (ATS)"]]
        .rename(columns={
            "Brands": "Brand",
            "Amazon Top-line Sales (ATS)": "Sales"
        })
        .copy()
    )

    # clean & format Sales as dollars
    df_net["Sales"] = (
        df_net["Sales"]
            .astype(str)
            .str.replace(r"[\$,]", "", regex=True)
            .astype(float)
            .map("${:,.2f}".format)
    )

    # 1-based index
    df_net.index = range(1, len(df_net) + 1)
    df_net.index.name = "No."

    # Save for UI
    st.session_state["net_sales_table"] = df_net
    st.session_state["net_sales_period"] = settlement_period
    return "ok"


def tool_gross_sales_for_specific_asin(prompt: str) -> str:
    """
    Show gross sales for a specific ASIN for last X days.
    Examples: 'Show gross sales for ASIN B0XXXXXXXX for last week'.
    Saves value in session_state['gross_sales_specific_asin'].
    """
    # Extract ASIN and time period
    m = re.search(
        r"""
        \b(?:asin\s*)?            # optional "ASIN" (allow no space)
        (B0[0-9A-Z]{8})         # capture the ASIN
        \s+for\s+               # " for "
        (last\s+week            # capture "last week"
         |7\s*days              
         |last\s+14\s*days
         |14\s*days
         |last\s+2\s*weeks
         |2\s*weeks
         |last\s+30\s*days
         |30\s*days
         |last\s+month)         # or "last month"
        \b
        """,
        prompt,
        flags=re.IGNORECASE | re.VERBOSE
    )
    
    if not m:
        st.session_state["agent_error"] = "Please include an ASIN (B0XXXXXXXX) and time period in your request."
        return "no_data"
    
    asin_q = m.group(1).upper()
    window = m.group(2).lower()

    # map token → number of days
    if '2' in window and 'week' in window:
        days = 14
    elif 'week' in window:
        days = 7
    elif '30' in window or 'month' in window:
        days = 30
    else:
        days = int(re.search(r"\d+", window).group())

    # Get the business report for the period
    try:
        inv_xls, period_text, fname = read_weekly_inventory(prompt)
    except FileNotFoundError as e:
        st.session_state["agent_error"] = str(e)
        return "no_data"

    # Build sheet name mapping for different stores and days
    sheet_name_by_store_and_days = {}
    for store in ("Product", "Brand", "TenWest"):
        sheet_name_by_store_and_days[store] = {}
        for d in [7, 14, 30]:
            sheet = find_week_sheet(inv_xls, store)
            if sheet:
                sheet_name_by_store_and_days[store][d] = sheet

    # sum gross sales for this ASIN across your three sheets
    total = 0.0
    for store in ("Product", "Brand", "TenWest"):
        sheet = sheet_name_by_store_and_days[store].get(days)
        if not sheet:
            continue

        df_sh = pd.read_excel(inv_xls, sheet_name=sheet)
        cols  = {c.lower(): c for c in df_sh.columns}

        # locate columns
        asin_col  = next((v for k, v in cols.items()
                          if "asin" in k and "parent" not in k), None)
        sales_col = next((v for k, v in cols.items()
                          if "ordered" in k and "product" in k and "sales" in k), None)
        if not (asin_col and sales_col):
            continue

        # coerce sales to numeric
        df_sh[sales_col] = (
            pd.to_numeric(
                df_sh[sales_col]
                  .astype(str)
                  .str.replace(r"[\$,]", "", regex=True),
                errors="coerce"
            )
            .fillna(0.0)
        )

        # filter & accumulate
        mask = df_sh[asin_col].astype(str).str.upper() == asin_q
        total += df_sh.loc[mask, sales_col].sum()

    # Save for UI
    st.session_state["gross_sales_specific_asin"] = total
    st.session_state["gross_sales_specific_asin_period"] = period_text
    st.session_state["gross_sales_specific_asin_code"] = asin_q
    st.session_state["business_file"] = fname
    return "ok"


def tool_conversion_rates_for_asins(prompt: str) -> str:
    """Prepare table + period in session_state (for Streamlit to render)."""
    inv_xls, period_text, fname = read_weekly_inventory(prompt)

    all_data = []
    for store in ("Product", "Brand", "TenWest"):
        sheet = find_week_sheet(inv_xls, store)
        if not sheet:
            continue
        df = pd.read_excel(inv_xls, sheet_name=sheet)
        df.columns = df.columns.str.strip()
        cols = {c.lower(): c for c in df.columns}
        asin_col  = next((v for k, v in cols.items() if "asin" in k and "parent" not in k), None)
        cr_col    = next((v for k, v in cols.items() if "unit session" in k and "percent" in k), None)
        units_col = next((v for k, v in cols.items() if "units ordered" in k), None)
        if not (asin_col and cr_col and units_col):
            continue
        tmp = df[[asin_col, cr_col, units_col]].copy()
        tmp.columns = ["ASIN", "Conversion Rate", "Units Ordered"]
        all_data.append(tmp)

    if not all_data:
        st.session_state["agent_error"] = "No Conversion Rate data found for the selected week."
        return "no_data"

    df_cr = pd.concat(all_data, ignore_index=True)
    df_cr["ASIN"] = df_cr["ASIN"].astype(str).str.upper().str.strip()
    df_cr["Units Ordered"] = pd.to_numeric(df_cr["Units Ordered"], errors="coerce").fillna(0).astype(int)

    s = df_cr["Conversion Rate"].astype(str).str.rstrip("%").str.replace(",", "", regex=True)
    df_cr["cr_val"] = pd.to_numeric(s, errors="coerce").fillna(0.0)
    df_cr = df_cr[df_cr["Units Ordered"] > 0]

    def _to_pct(x: float) -> float:
        return x if x > 1.5 else x * 100.0

    weighted = (
        df_cr.groupby("ASIN", as_index=False)
             .apply(lambda g: pd.Series({
                 "Conversion Rate": _to_pct((g["cr_val"] * g["Units Ordered"]).sum() /
                                            (g["Units Ordered"].sum() or 1))
             }))
             .reset_index(drop=True)
    )
    weighted["Brand"] = weighted["ASIN"].map(brand_map).fillna("UNKNOWN").str.upper()
    weighted["Conversion Rate"] = weighted["Conversion Rate"].map(lambda x: f"{x:.2f}%")

    out = weighted[["Brand", "ASIN", "Conversion Rate"]].copy()
    out.index = range(1, len(out) + 1)

    # Save for UI
    st.session_state["conv_table"] = out
    st.session_state["conv_period"] = period_text.replace("–", " - ")
    st.session_state["business_file"] = fname
    return "ok"

def tool_average_conversion_rate(prompt: str) -> str:
    """Compute weighted average CR. Stores value; UI prints w/ settlement_period."""
    inv_xls, period_text, fname = read_weekly_inventory(prompt)

    all_data = []
    for store in ("Product", "Brand", "TenWest"):
        sheet = find_week_sheet(inv_xls, store)
        if not sheet:
            continue
        df = pd.read_excel(inv_xls, sheet_name=sheet)
        df.columns = df.columns.str.strip()
        cols = {c.lower(): c for c in df.columns}
        asin_col  = next((v for k, v in cols.items() if "asin" in k and "parent" not in k), None)
        cr_col    = next((v for k, v in cols.items() if "unit session" in k and "percent" in k), None)
        units_col = next((v for k, v in cols.items() if "units ordered" in k), None)
        if not (asin_col and cr_col and units_col):
            continue

        tmp = df[[asin_col, cr_col, units_col]].copy()
        tmp.columns = ["ASIN", "CR_raw", "Units Ordered"]
        tmp["ASIN"] = tmp["ASIN"].astype(str).str.upper().str.strip()
        tmp["Units Ordered"] = pd.to_numeric(tmp["Units Ordered"], errors="coerce").fillna(0).astype(int)

        s = tmp["CR_raw"].astype(str).str.rstrip("%").str.replace(",", "", regex=True)
        tmp["cr_val"] = pd.to_numeric(s, errors="coerce").fillna(0.0)
        all_data.append(tmp)

    if not all_data:
        st.session_state["agent_error"] = "No conversion-rate data found for the selected week."
        return "no_data"

    df_cr = pd.concat(all_data, ignore_index=True)
    df_cr = df_cr[df_cr["Units Ordered"] > 0]
    df_cr = df_cr[df_cr["cr_val"].notna()]
    if df_cr.empty:
        st.session_state["agent_error"] = "No valid conversion-rate entries after filtering zero-unit rows."
        return "no_data"

    def _to_pct(x: float) -> float:
        return x if x > 1.5 else x * 100.0

    total_units = df_cr["Units Ordered"].sum()
    avg_pct = _to_pct((df_cr["cr_val"] * df_cr["Units Ordered"]).sum() / (total_units or 1))

    # Save for UI
    st.session_state["avg_cr"] = float(f"{avg_pct:.2f}")
    st.session_state["avg_period_display"] = settlement_period  # as requested
    st.session_state["business_file"] = fname
    return "ok"


def tool_buy_box_percentages_for_asins(prompt: str) -> str:
    """
    Show Buy Box percentages for ASINs for last week.
    Examples: 'Show the Buy Box percentages for my ASINs'.
    Saves table in session_state['buy_box_table'].
    """
    days = 7
    
    # Get the business report for the period
    try:
        inv_xls, period_text, fname = read_weekly_inventory(prompt)
    except FileNotFoundError as e:
        st.session_state["agent_error"] = str(e)
        return "no_data"

    # Build sheet name mapping for different stores and days
    sheet_name_by_store_and_days = {}
    for store in ("Product", "Brand", "TenWest"):
        sheet_name_by_store_and_days[store] = {}
        for d in [7, 14, 30]:
            sheet = find_week_sheet(inv_xls, store)
            if sheet:
                sheet_name_by_store_and_days[store][d] = sheet

    all_data = []
    for store in ("Product", "Brand", "TenWest"):
        sheet = sheet_name_by_store_and_days[store].get(days)
        if not sheet:
            continue
        df_sh = pd.read_excel(inv_xls, sheet_name=sheet)
        df_sh.columns = df_sh.columns.str.strip()
        cols = {c.lower(): c for c in df_sh.columns}

        asin_col  = next((v for k,v in cols.items() if "asin" in k and "parent" not in k), None)
        bb_col    = next((v for k,v in cols.items() if "buy box" in k and "percentage" in k), None)
        units_col = next((v for k,v in cols.items() if "units ordered" in k), None)
        if not (asin_col and bb_col and units_col):
            continue

        temp = df_sh[[asin_col, bb_col, units_col]].copy()
        temp.columns = ["ASIN", "Buy Box %", "Units Ordered"]
        all_data.append(temp)

    if not all_data:
        st.session_state["agent_error"] = "No Buy Box data found for the last 7 days."
        return "no_data"

    df_bb = pd.concat(all_data, ignore_index=True)
    df_bb["ASIN"] = df_bb["ASIN"].astype(str).str.upper().str.strip()

    # parse raw buy-box values
    raw = (
        df_bb["Buy Box %"]
          .astype(str)
          .str.rstrip("%")
          .str.replace(",", "", regex=True)
          .astype(float)
          .fillna(0.0)
    )
    df_bb["bb_frac"] = raw.apply(lambda x: x/100 if x > 1 else x)

    # ensure units numeric
    df_bb["Units Ordered"] = pd.to_numeric(df_bb["Units Ordered"], errors="coerce").fillna(0)

    # weighted average by units ordered
    weighted = (
        df_bb
        .groupby("ASIN", as_index=False)
        .apply(lambda g: pd.Series({
            "bb_frac": (g["bb_frac"] * g["Units Ordered"]).sum() / g["Units Ordered"].sum()
        }))
        .reset_index()
    )

    # back to "0–100%"
    weighted["Buy Box %"] = weighted["bb_frac"].map(lambda x: f"{x * 100:.2f}%")

    # map in Brand and uppercase
    weighted["Brand"] = weighted["ASIN"].map(brand_map).fillna("Unknown").str.upper()

    # finalize
    out = weighted[["Brand", "ASIN", "Buy Box %"]].copy()
    out.index = range(1, len(out) + 1)
    out.index.name = "No."

    # Save for UI
    st.session_state["buy_box_table"] = out
    st.session_state["buy_box_period"] = period_text
    st.session_state["business_file"] = fname
    return "ok"


def tool_average_buy_box_percentage(prompt: str) -> str:
    """
    Show average Buy Box percentage for last week.
    Examples: 'Show my average Buy Box percentage for last week'.
    Saves value in session_state['avg_buy_box'].
    """
    days = 7
    
    # Get the business report for the period
    try:
        inv_xls, period_text, fname = read_weekly_inventory(prompt)
    except FileNotFoundError as e:
        st.session_state["agent_error"] = str(e)
        return "no_data"

    # Build sheet name mapping for different stores and days
    sheet_name_by_store_and_days = {}
    for store in ("Product", "Brand", "TenWest"):
        sheet_name_by_store_and_days[store] = {}
        for d in [7, 14, 30]:
            sheet = find_week_sheet(inv_xls, store)
            if sheet:
                sheet_name_by_store_and_days[store][d] = sheet

    records = []
    for store in ("Product", "Brand", "TenWest"):
        sheet = sheet_name_by_store_and_days.get(store, {}).get(days)
        if not sheet:
            continue
        df_sh = pd.read_excel(inv_xls, sheet_name=sheet)
        df_sh.columns = df_sh.columns.str.strip()
        cols = {c.lower(): c for c in df_sh.columns}

        asin_col  = next((v for k,v in cols.items() if "asin" in k and "parent" not in k), None)
        bb_col    = next((v for k,v in cols.items() if "buy box" in k and "percentage" in k), None)
        units_col = next((v for k,v in cols.items() if "units ordered" in k), None)
        if not (asin_col and bb_col and units_col):
            continue

        tmp = df_sh[[asin_col, bb_col, units_col]].copy()
        tmp.columns = ["ASIN", "Buy Box %", "Units Ordered"]
        tmp["ASIN"] = tmp["ASIN"].astype(str).str.upper().str.strip()
        tmp["Units Ordered"] = pd.to_numeric(tmp["Units Ordered"], errors="coerce").fillna(0)

        # parse Buy Box into fraction 0–1
        raw = (
            tmp["Buy Box %"]
              .astype(str)
              .str.rstrip("%")
              .str.replace(",", "", regex=True)
              .astype(float)
              .fillna(0.0)
        )
        tmp["bb_frac"] = raw.apply(lambda x: x/100 if x > 1 else x)

        records.append(tmp[["ASIN", "bb_frac", "Units Ordered"]])

    if not records:
        st.session_state["agent_error"] = "No Buy Box data found for the last week."
        return "no_data"

    df_bb = pd.concat(records, ignore_index=True)

    # weighted average across all entries
    total_units = df_bb["Units Ordered"].sum()
    if total_units > 0:
        avg_frac = (df_bb["bb_frac"] * df_bb["Units Ordered"]).sum() / total_units
    else:
        avg_frac = 0.0

    avg_pct = avg_frac * 100

    # Save for UI
    st.session_state["avg_buy_box"] = float(f"{avg_pct:.2f}")
    st.session_state["avg_buy_box_period"] = period_text
    st.session_state["business_file"] = fname
    return "ok"


def tool_sessions_for_asins(prompt: str) -> str:
    """
    Show total number of Sessions for ASINs for last X days.
    Examples: 'Show the total number of Sessions for my ASINs last week'.
    Saves table in session_state['sessions_table'].
    """
    # Parse the time period from the prompt
    m = re.search(
        r"\b(?:last\s+week|7\s*days|last\s+14\s*days|14\s*days|"
        r"last\s+2\s*weeks|2\s*weeks|last\s+30\s*days|30\s*days|last\s+month)\b",
        prompt.lower()
    )
    if not m:
        st.session_state["agent_error"] = "Please specify a time period (e.g., 'last week', '14 days', 'last month')."
        return "no_data"
    
    tok = m.group(0)
    if '2' in tok and 'week' not in tok:
        days = 14
    elif '30' in tok or 'month' in tok:
        days = 30
    else:
        days = 7

    # Get the business report for the period
    try:
        inv_xls, period_text, fname = read_weekly_inventory(prompt)
    except FileNotFoundError as e:
        st.session_state["agent_error"] = str(e)
        return "no_data"

    # Build sheet name mapping for different stores and days
    sheet_name_by_store_and_days = {}
    for store in ("Product", "Brand", "TenWest"):
        sheet_name_by_store_and_days[store] = {}
        for d in [7, 14, 30]:
            sheet = find_week_sheet(inv_xls, store)
            if sheet:
                sheet_name_by_store_and_days[store][d] = sheet

    # Accumulate sessions by ASIN
    asin_sess = defaultdict(int)
    for store in ("Product", "Brand", "TenWest"):
        sheet = sheet_name_by_store_and_days[store].get(days)
        if not sheet:
            continue
        df_sh = pd.read_excel(inv_xls, sheet_name=sheet)

        cols     = {c.lower(): c for c in df_sh.columns}
        asin_col = next((v for k, v in cols.items() if "asin" in k and "parent" not in k), None)
        sess_col = next((v for k, v in cols.items() if "sessions" in k and "unit" not in k), None)
        if not (asin_col and sess_col):
            continue

        df_sh[sess_col] = pd.to_numeric(df_sh[sess_col], errors="coerce").fillna(0).astype(int)
        for a, grp in df_sh.groupby(asin_col):
            asin_sess[str(a).upper()] += grp[sess_col].sum()

    # Build the output DataFrame
    rows = [{"ASIN": a, "Total Sessions": v} for a, v in asin_sess.items()]
    out  = pd.DataFrame(rows)

    # Map in brands (capitalized)
    out["Brand"] = out["ASIN"].map(brand_map).fillna("Unknown").str.upper()

    # Format Total Sessions with commas
    out["Total Sessions"] = out["Total Sessions"].map("{:,}".format)

    # Reorder and sort by numeric value
    out = (
        out[["Brand", "ASIN", "Total Sessions"]]
          .sort_values("Total Sessions", key=lambda x: x.str.replace(",", "").astype(int), ascending=False)
          .reset_index(drop=True)
    )
    out.index = out.index + 1
    out.index.name = "No."

    # Total sessions summary (numeric only for display)
    total_sessions = sum(int(s.replace(",", "")) for s in out["Total Sessions"])

    # Save for UI
    st.session_state["sessions_table"] = out
    st.session_state["sessions_period"] = period_text
    st.session_state["sessions_total"] = total_sessions
    st.session_state["sessions_days"] = days
    st.session_state["business_file"] = fname
    return "ok"


def tool_average_order_value(prompt: str) -> str:
    """
    Show Average Order Value for ASINs for last X days.
    Examples: 'Show the Average Order Value for my ASINs last week'.
    Saves table in session_state['aov_table'].
    """
    # Parse the time period from the prompt
    m = re.search(
        r"\b(?:last\s+week|7\s*days|last\s+14\s*days|14\s*days|"
        r"last\s+2\s*weeks|2\s*weeks|last\s+30\s*days|30\s*days|last\s+month)\b",
        prompt.lower()
    )
    if not m:
        st.session_state["agent_error"] = "Please specify a time period (e.g., 'last week', '14 days', 'last month')."
        return "no_data"
    
    tok = m.group(0)
    if '2' in tok and 'week' not in tok:
        days = 14
    elif '30' in tok or 'month' in tok:
        days = 30
    else:
        days = 7

    # Get the business report for the period
    try:
        inv_xls, period_text, fname = read_weekly_inventory(prompt)
    except FileNotFoundError as e:
        st.session_state["agent_error"] = str(e)
        return "no_data"

    # Build sheet name mapping for different stores and days
    sheet_name_by_store_and_days = {}
    for store in ("Product", "Brand", "TenWest"):
        sheet_name_by_store_and_days[store] = {}
        for d in [7, 14, 30]:
            sheet = find_week_sheet(inv_xls, store)
            if sheet:
                sheet_name_by_store_and_days[store][d] = sheet

    # load & concatenate the three store-sheets
    parts = []
    for store in ("Product", "Brand", "TenWest"):
        sheet = sheet_name_by_store_and_days[store].get(days)
        if not sheet:
            continue
        df_sh = pd.read_excel(inv_xls, sheet_name=sheet)
        cols = {c.lower(): c for c in df_sh.columns}
        asin_col  = next((v for k,v in cols.items() if "asin" in k and "parent" not in k), None)
        sales_col = next((v for k,v in cols.items() if "ordered" in k and "product" in k and "sales" in k), None)
        units_col = next((v for k,v in cols.items() if "units ordered" in k), None)
        if not (asin_col and sales_col and units_col):
            continue

        subset = df_sh[[asin_col, sales_col, units_col]].rename(columns={
            asin_col: "ASIN",
            sales_col: "Sales_raw",
            units_col: "Units_raw"
        })
        parts.append(subset)

    if not parts:
        st.session_state["agent_error"] = f"No data found for the last {days} days."
        return "no_data"

    df_all = pd.concat(parts, ignore_index=True)
    df_all["ASIN"] = df_all["ASIN"].astype(str).str.upper().str.strip()
    df_all["Sales"] = (
        pd.to_numeric(
            df_all["Sales_raw"].astype(str).str.replace(r"[\$,]", "", regex=True),
            errors="coerce"
        ).fillna(0.0)
    )
    df_all["Units"] = pd.to_numeric(df_all["Units_raw"], errors="coerce").fillna(0)

    # group & sum
    summary = (
        df_all
        .groupby("ASIN", as_index=False)
        .agg(TotalSales=("Sales", "sum"), TotalUnits=("Units", "sum"))
    )
    summary = summary[summary["TotalUnits"] > 0]  # drop zero‑unit rows
    summary["AOV"] = summary["TotalSales"] / summary["TotalUnits"]

    # overall AOV
    overall_aov = summary["AOV"].mul(summary["TotalUnits"]).sum() / summary["TotalUnits"].sum()

    # map brand
    summary["Brand"] = summary["ASIN"].map(brand_map).fillna("UNKNOWN").str.upper()

    # format for display
    summary["Average Order Value"] = summary["AOV"].map("${:,.2f}".format)

    out = summary[["Brand", "ASIN", "Average Order Value"]].copy()
    out = out.sort_values("Average Order Value", key=lambda c: c.str.replace(r"[\$,]", "", regex=True).astype(float), ascending=False)
    out.index = range(1, len(out) + 1)
    out.index.name = "No."

    # Save for UI
    st.session_state["aov_table"] = out
    st.session_state["aov_period"] = period_text
    st.session_state["aov_overall"] = overall_aov
    st.session_state["aov_days"] = days
    st.session_state["business_file"] = fname
    return "ok"


def tool_suppressed_asins(prompt: str) -> str:
    """
    Show ASINs suppressed (Buy Box % ≤ 80%) for last X days.
    Examples: 'Show the ASINs suppressed last week'.
    Saves table in session_state['suppressed_table'].
    """
    # Parse the time period from the prompt
    m = re.search(
        r"\b(?:last\s+week|7\s*days|last\s+14\s*days|14\s*days|"
        r"last\s+2\s*weeks|2\s*weeks|last\s+30\s*days|30\s*days|last\s+month)\b",
        prompt.lower()
    )
    if not m:
        st.session_state["agent_error"] = "Please specify a time period (e.g., 'last week', '14 days', 'last month')."
        return "no_data"
    
    tok = m.group(0)
    if '2' in tok and 'week' not in tok:
        days = 14
    elif '30' in tok or 'month' in tok:
        days = 30
    else:
        days = 7

    # Get the business report for the period
    try:
        inv_xls, period_text, fname = read_weekly_inventory(prompt)
    except FileNotFoundError as e:
        st.session_state["agent_error"] = str(e)
        return "no_data"

    # Build sheet name mapping for different stores and days
    sheet_name_by_store_and_days = {}
    for store in ("Product", "Brand", "TenWest"):
        sheet_name_by_store_and_days[store] = {}
        for d in [7, 14, 30]:
            sheet = find_week_sheet(inv_xls, store)
            if sheet:
                sheet_name_by_store_and_days[store][d] = sheet

    # Gather suppressed rows including units ordered
    suppressed_rows = []
    for store in ("Product", "Brand", "TenWest"):
        sheet = sheet_name_by_store_and_days[store].get(days)
        if not sheet:
            continue
        try:
            df = pd.read_excel(inv_xls, sheet_name=sheet)
        except:
            continue

        df.columns = df.columns.str.strip()
        cols      = {c.lower(): c for c in df.columns}
        asin_col  = next((orig for low, orig in cols.items()
                          if "asin" in low and "parent" not in low), None)
        bb_col    = next((orig for low, orig in cols.items()
                          if "buy box" in low or "featured offer" in low), None)
        units_col = next((orig for low, orig in cols.items()
                          if "units ordered" in low), None)
        if not (asin_col and bb_col and units_col):
            continue

        # Convert to numeric
        df[bb_col]    = pd.to_numeric(df[bb_col], errors="coerce").fillna(0.0)
        df[units_col] = pd.to_numeric(df[units_col], errors="coerce").fillna(0)

        # Rows where Buy Box ≤ 0.80
        mask = df[bb_col] <= 0.80
        tmp = df.loc[mask, [asin_col, bb_col, units_col]].copy()
        tmp.columns = ["ASIN", "BuyBoxPct", "UnitsOrdered"]
        suppressed_rows.append(tmp)

    if not suppressed_rows:
        st.session_state["agent_error"] = "No ASINs had Buy Box percentages below 80% in that period."
        return "no_data"

    # Concatenate and compute weighted average per ASIN
    all_supp = pd.concat(suppressed_rows, ignore_index=True)
    weighted = (
        all_supp
        .groupby("ASIN", as_index=False)
        .apply(lambda g: pd.Series({
            "BuyBoxPct": (g["BuyBoxPct"] * g["UnitsOrdered"]).sum()
                          / g["UnitsOrdered"].sum()
        }))
        .reset_index()
    )
    # Keep only those still ≤ 0.80
    weighted = weighted[weighted["BuyBoxPct"] <= 0.80]

    if weighted.empty:
        st.session_state["agent_error"] = "No ASINs had Buy Box percentages below 80% in that period."
        return "no_data"

    # Map & capitalize Brand
    weighted["Brand"] = (
        weighted["ASIN"]
        .map(brand_map)
        .fillna("UNKNOWN")
        .str.upper()
    )

    # Format percentage
    weighted["Buy Box %"] = weighted["BuyBoxPct"].map(lambda x: f"{x * 100:.2f}%")

    # Build final table
    out = weighted[["Brand", "ASIN", "Buy Box %"]].reset_index(drop=True)
    out.index = range(1, len(out) + 1)
    out.index.name = "No."

    # Save for UI
    st.session_state["suppressed_table"] = out
    st.session_state["suppressed_period"] = period_text
    st.session_state["suppressed_days"] = days
    st.session_state["business_file"] = fname
    return "ok"


def tool_currently_suppressed_asins(prompt: str) -> str:
    """
    Show ASINs currently suppressed (0% Buy Box).
    Examples: 'Show the ASINs currently suppressed'.
    Saves table in session_state['currently_suppressed_table'].
    """
    days = 7
    
    # Get the business report for the period
    try:
        inv_xls, period_text, fname = read_weekly_inventory(prompt)
    except FileNotFoundError as e:
        st.session_state["agent_error"] = str(e)
        return "no_data"

    # Build sheet name mapping for different stores and days
    sheet_name_by_store_and_days = {}
    for store in ("Product", "Brand", "TenWest"):
        sheet_name_by_store_and_days[store] = {}
        for d in [7, 14, 30]:
            sheet = find_week_sheet(inv_xls, store)
            if sheet:
                sheet_name_by_store_and_days[store][d] = sheet

    # "current" = use your 7‑day sheet as a snapshot
    suppressed_rows = []

    for store in ("Product", "Brand", "TenWest"):
        sheet = sheet_name_by_store_and_days[store].get(days)
        if not sheet:
            continue
        try:
            df = pd.read_excel(inv_xls, sheet_name=sheet)
        except:
            continue

        # map lowercase→original
        cols = {str(c).lower(): c for c in df.columns}
        asin_col = next((orig for low, orig in cols.items()
                         if "asin" in low and "parent" not in low), None)
        bb_col = next((orig for low, orig in cols.items()
                       if "buy box" in low or "featured offer" in low), None)
        if not (asin_col and bb_col):
            continue

        # strip "%" and convert to float
        df[bb_col] = (df[bb_col]
                       .astype(str)
                       .str.rstrip("%")
                       .replace("", "0")
                       .astype(float)
                    )

        # keep only exactly 0%
        subset = df.loc[df[bb_col] == 0, [asin_col, bb_col]].copy()
        subset.columns = ["ASIN", "BuyBoxPct"]
        suppressed_rows.append(subset)

    if suppressed_rows:
        result = pd.concat(suppressed_rows, ignore_index=True)
        result = result.drop_duplicates("ASIN", keep="first")

        # capitalize Brand names
        result["Brand"] = result["ASIN"].map(brand_map).fillna("UNKNOWN").str.upper()
        result["Buy Box %"] = result["BuyBoxPct"].map("{:.2f}%".format)

        out = result[["Brand", "ASIN", "Buy Box %"]]
        out.index = range(1, len(out) + 1)
        out.index.name = "No."

        # Save for UI
        st.session_state["currently_suppressed_table"] = out
        st.session_state["currently_suppressed_period"] = period_text
        st.session_state["business_file"] = fname
        return "ok"
    else:
        st.session_state["agent_error"] = f"No ASINs currently have a 0% Buy Box from {period_text}."
        return "no_data"


def tool_sales_lost_to_other_sellers(prompt: str) -> str:
    """
    Show total sales lost to other sellers for last week.
    Examples: 'Show the total sales lost to other sellers last week'.
    Saves table in session_state['sales_lost_table'].
    """
    days = 7
    
    # Get the business report for the period
    try:
        inv_xls, period_text, fname = read_weekly_inventory(prompt)
    except FileNotFoundError as e:
        st.session_state["agent_error"] = str(e)
        return "no_data"

    # Build sheet name mapping for different stores and days
    sheet_name_by_store_and_days = {}
    for store in ("Product", "Brand", "TenWest"):
        sheet_name_by_store_and_days[store] = {}
        for d in [7, 14, 30]:
            sheet = find_week_sheet(inv_xls, store)
            if sheet:
                sheet_name_by_store_and_days[store][d] = sheet

    # accumulators
    lost_sales     = defaultdict(float)
    lost_units     = defaultdict(float)
    units_count    = defaultdict(float)  # for weighted buy-box
    disp_sum       = defaultdict(float)  # sum(disp * units)
    
    for store in ("Product", "Brand", "TenWest"):
        sheet = sheet_name_by_store_and_days.get(store, {}).get(days)
        if not sheet:
            continue
        try:
            df = pd.read_excel(inv_xls, sheet_name=sheet)
        except Exception as e:
            continue

        # normalize column names
        df.columns = df.columns.str.strip()
        cols = {c.lower(): c for c in df.columns}

        # find the four key columns
        asin_col  = (
            "Child ASIN"
            if "Child ASIN" in df.columns
            else next((orig for low, orig in cols.items()
                       if "asin" in low and "parent" not in low),
                      None)
        )
        sales_col = next((orig for low, orig in cols.items()
                          if "ordered" in low and "product" in low and "sales" in low),
                         None)
        units_col = next((orig for low, orig in cols.items()
                          if "units ordered" in low),
                         None)
        fb_col     = next((orig for low, orig in cols.items()
                          if "buy box" in low or "featured offer" in low),
                         None)

        if not (asin_col and sales_col and units_col and fb_col):
            continue

        # clean and convert
        S = (
            df[sales_col]
            .astype(str)
            .str.replace(r"[\$,]", "", regex=True)
            .pipe(pd.to_numeric, errors="coerce")
            .fillna(0.0)
        )
        U = pd.to_numeric(df[units_col], errors="coerce").fillna(0.0)

        # parse raw percentage values
        raw_pct_series = (
            df[fb_col]
            .astype(str)
            .str.rstrip("%")
            .str.replace(",", "", regex=True)
            .replace("", "0")
            .pipe(pd.to_numeric, errors="coerce")
            .fillna(0.0)
        )

        # iterate rows
        for asin_val, sales_val, units_val, raw_pct in zip(
            df[asin_col].astype(str).str.upper().str.strip(),
            S, U, raw_pct_series
        ):
            # decide fraction vs raw-percent
            if raw_pct <= 1:
                frac = raw_pct           # fraction 0–1
                disp = raw_pct * 100     # display 0–100
            else:
                frac = raw_pct / 100
                disp = raw_pct

            if frac <= 0:
                continue

            # accumulate lost sales/units
            lost_sales[asin_val] += sales_val / frac - sales_val
            lost_units[asin_val] += units_val / frac - units_val

            # accumulate for weighted buy-box
            units_count[asin_val] += units_val
            disp_sum[asin_val]    += disp * units_val

    if not lost_sales:
        st.session_state["agent_error"] = "No data found for lost to other sellers last week."
        return "no_data"

    # overall summary
    total_lost = sum(lost_sales.values())

    # detail rows with units lost rounded up, weighted buy-box, filter 100%
    rows = []
    for a in lost_sales:
        # compute weighted buy-box percent
        total_units = units_count.get(a, 0.0)
        if total_units > 0:
            weighted_disp = disp_sum[a] / total_units
        else:
            weighted_disp = 0.0
        # skip 100% entries
        if abs(weighted_disp - 100.0) < 1e-6:
            continue

        units_lost = int(lost_units[a] + 0.5)  # round up
        pct_str    = f"{weighted_disp:.2f}%"
        brand      = brand_map.get(a, "UNKNOWN").upper()

        rows.append({
            "Brand":   brand,
            "ASIN":    a,
            "Featured Offer (Buy Box) Percentage": pct_str,
            "Units Lost": units_lost,
            "Sales Lost": lost_sales[a]
        })

    df_out = pd.DataFrame(rows)

    # format & index
    df_out["Units Lost"] = df_out["Units Lost"].map("{:,}".format)
    df_out["Sales Lost"] = df_out["Sales Lost"].map("${:,.2f}".format)
    df_out.index = range(1, len(df_out) + 1)
    df_out.index.name = "No."

    # Save for UI
    st.session_state["sales_lost_table"] = df_out
    st.session_state["sales_lost_period"] = period_text
    st.session_state["sales_lost_total"] = total_lost
    st.session_state["business_file"] = fname
    return "ok"


def tool_profit_lost_to_other_sellers(prompt: str) -> str:
    """
    Show total gross profit lost to other sellers for last week.
    Examples: 'Show the total gross profit lost to other sellers last week'.
    Saves table in session_state['profit_lost_table'].
    """
    days = 7
    
    # Get the business report for the period
    try:
        inv_xls, period_text, fname = read_weekly_inventory(prompt)
    except FileNotFoundError as e:
        st.session_state["agent_error"] = str(e)
        return "no_data"

    # Build sheet name mapping for different stores and days
    sheet_name_by_store_and_days = {}
    for store in ("Product", "Brand", "TenWest"):
        sheet_name_by_store_and_days[store] = {}
        for d in [7, 14, 30]:
            sheet = find_week_sheet(inv_xls, store)
            if sheet:
                sheet_name_by_store_and_days[store][d] = sheet

    def _norm_pct_to_0_100(val):
        """
        Normalize
          "25.03%" -> 25.03
          "25.03"  -> 25.03
          "0.2503" -> 25.03 (treat as fraction 0..1)
          "", "-", "—", "N/A" -> None
        """
        s = str(val).strip().lower()
        if s in {"", "na", "n/a", "-", "—", "–", "none", "null"}:
            return None
        try:
            num = float(s.rstrip("%").replace(",", ""))
        except ValueError:
            return None
        return num if num > 1 else num * 100.0

    # 1) Build GM (Actual) from asin_df — preserve negatives, accept % or fraction
    gm_frac_map = {}
    gm_disp_map = {}
    for _, row in asin_df.iterrows():
        a = str(row.get("ASIN", "")).upper().strip()
        if not a:
            continue
        raw = str(row.get("Gross Margin", "")).strip().rstrip("%").replace(",", "")
        try:
            num = float(raw)
        except Exception:
            num = 0.0
        if abs(num) > 1:        # e.g., 25.03 -> 25.03%
            frac = num / 100.0  # 0.2503
            disp = num          # 25.03
        else:                   # e.g., 0.2503 -> 25.03%
            frac = num
            disp = num * 100.0
        gm_frac_map[a] = frac   # keep sign
        gm_disp_map[a] = disp   # keep sign

    # 1b) Build GM (Plan) from unit_df — same parsing rules, preserve negatives
    gm_plan_frac_map = {}
    gm_plan_disp_map = {}
    try:
        src_df = unit_df
    except NameError:
        src_df = None

    if src_df is not None and not src_df.empty:
        for _, row in src_df.iterrows():
            a = str(row.get("ASIN", "")).upper().strip()
            if not a:
                continue
            raw = str(row.get("Gross Margin", "")).strip().rstrip("%").replace(",", "")
            try:
                num = float(raw)
            except Exception:
                num = 0.0
            if abs(num) > 1:
                frac = num / 100.0
                disp = num
            else:
                frac = num
                disp = num * 100.0
            gm_plan_frac_map[a] = frac   # keep sign
            gm_plan_disp_map[a] = disp   # keep sign

    # 2) Accumulate lost sales/units and weighted Buy-Box
    lost_sales      = defaultdict(float)
    lost_units      = defaultdict(float)

    # Weighted BB by units (preferred)
    sum_bb_units    = defaultdict(float)  # sum of (bb% * units)
    sum_units       = defaultdict(float)

    # Weighted BB by sales (fallback if units are 0/NaN)
    sum_bb_sales    = defaultdict(float)  # sum of (bb% * sales)
    sum_sales       = defaultdict(float)

    stores = ("Product", "Brand", "TenWest")  # add "Canada" if you want those included

    for store in stores:
        sheet = sheet_name_by_store_and_days.get(store, {}).get(days)
        if not sheet:
            continue

        try:
            df = pd.read_excel(inv_xls, sheet_name=sheet)
        except Exception:
            continue

        df.columns = df.columns.str.strip()
        cols = {c.lower(): c for c in df.columns}

        asin_col  = "Child ASIN" if "Child ASIN" in df.columns else next(
            (c for low, c in cols.items() if "asin" in low and "parent" not in low), None
        )
        sales_col = next(
            (c for low, c in cols.items() if "ordered" in low and "product" in low and "sales" in low),
            None
        )
        units_col = next((c for low, c in cols.items() if "units ordered" in low), None)
        fb_col    = next((c for low, c in cols.items() if "buy box" in low or "featured offer" in low), None)

        if not (asin_col and sales_col and units_col and fb_col):
            continue

        # Clean numeric columns
        S = (df[sales_col].astype(str)
             .str.replace(r"[\$,]", "", regex=True)
             .pipe(pd.to_numeric, errors="coerce")
             .fillna(0.0))
        U = pd.to_numeric(df[units_col], errors="coerce").fillna(0.0)

        # Normalize BB% to [0..100] or None
        pct_series = df[fb_col].map(_norm_pct_to_0_100)

        for a, s_val, u_val, pct in zip(
            df[asin_col].astype(str).str.upper().str.strip(), S, U, pct_series
        ):
            if not a:
                continue

            # Track weighted BB inputs when we have a usable percentage
            if pct is not None:
                sum_bb_units[a] += pct * u_val
                sum_units[a]    += u_val
                sum_bb_sales[a] += pct * s_val
                sum_sales[a]    += s_val

                frac = pct / 100.0
                # Only compute "lost" when Buy Box share is known and > 0
                if frac > 0:
                    # Expected if 100% BB minus actual
                    lost_sales[a] += s_val / frac - s_val
                    lost_units[a] += u_val / frac - u_val
            else:
                # Unknown BB%: cannot compute "lost" for this row,
                # but ASIN can still appear if other rows/stores provide BB%.
                pass

    # If absolutely nothing tallied
    if not (lost_sales or sum_units or sum_sales):
        st.session_state["agent_error"] = "No data found for lost to other sellers last week."
        return "no_data"

    # 3) Build output rows, compute weighted BB with fallback, filter out true 100% BB
    total_gp_lost = 0.0
    rows = []

    all_asins = set(lost_sales.keys()) | set(sum_units.keys()) | set(sum_sales.keys())
    for a in sorted(all_asins):
        # Weighted BB%: prefer units weighting; if no units, fall back to sales weighting
        weighted_bb = None
        if sum_units[a] > 0:
            weighted_bb = sum_bb_units[a] / sum_units[a]
        elif sum_sales[a] > 0:
            weighted_bb = sum_bb_sales[a] / sum_sales[a]

        # Exclude true 100% Buy Box ASINs if we have a known BB%
        if weighted_bb is not None and abs(weighted_bb - 100.0) < 1e-9:
            continue

        sales_lost = float(lost_sales.get(a, 0.0))
        units_lost_val = float(lost_units.get(a, 0.0))
        units_lost_disp = int(units_lost_val + 0.5) if units_lost_val > 0 else 0

        # Actual GM/GP (from asin_df)
        gm_actual_frac = gm_frac_map.get(a, 0.0)
        gm_actual_disp = gm_disp_map.get(a, 0.0)
        gp_lost_actual = sales_lost * gm_actual_frac
        total_gp_lost += gp_lost_actual

        # Plan GM/GP (from unit_df)
        gm_plan_frac = gm_plan_frac_map.get(a, 0.0)
        gm_plan_disp = gm_plan_disp_map.get(a, 0.0)
        gp_lost_plan = sales_lost * gm_plan_frac

        rows.append({
            "Brand":   str(brand_map.get(a, "UNKNOWN")).upper(),
            "ASIN":    a,
            "Featured Offer (Buy Box) Percentage": "" if weighted_bb is None else f"{weighted_bb:.2f}%",
            "Units Lost": units_lost_disp,
            "Sales Lost": sales_lost,
            "Gross Margin (Plan)":   f"{gm_plan_disp:.2f}%",     
            "Gross Profit Lost (Plan)": gp_lost_plan,            
            "Gross Margin (Actual)": f"{gm_actual_disp:.2f}%",
            "Gross Profit Lost (Actual)": gp_lost_actual
        })

    # 4) Display detail table
    df_out = pd.DataFrame(rows)

    if not df_out.empty:
        # Pretty formatting
        if "Units Lost" in df_out:
            df_out["Units Lost"] = df_out["Units Lost"].map(lambda x: f"{int(x):,}")
        if "Sales Lost" in df_out:
            df_out["Sales Lost"] = df_out["Sales Lost"].map(lambda x: f"${x:,.2f}")
        if "Gross Profit Lost (Plan)" in df_out:
            df_out["Gross Profit Lost (Plan)"] = df_out["Gross Profit Lost (Plan)"].map(lambda x: f"${x:,.2f}")
        if "Gross Profit Lost (Actual)" in df_out:
            df_out["Gross Profit Lost (Actual)"] = df_out["Gross Profit Lost (Actual)"].map(lambda x: f"${x:,.2f}")

        df_out.index = range(1, len(df_out) + 1)
        df_out.index.name = "No."

        # Column order requested
        display_cols = [
            "Brand",
            "ASIN",
            "Featured Offer (Buy Box) Percentage",
            "Units Lost",
            "Sales Lost",
            "Gross Margin (Plan)",
            "Gross Profit Lost (Plan)",
            "Gross Margin (Actual)",
            "Gross Profit Lost (Actual)"
        ]

        # Only select columns that exist (defensive in case of upstream changes)
        display_cols = [c for c in display_cols if c in df_out.columns]

        # Save for UI
        st.session_state["profit_lost_table"] = df_out[display_cols]
        st.session_state["profit_lost_period"] = period_text
        st.session_state["profit_lost_total"] = total_gp_lost
        st.session_state["business_file"] = fname
        return "ok"
    else:
        st.session_state["agent_error"] = "No qualifying ASINs to display (all 100% Buy Box or missing Buy Box data)."
        return "no_data"



def tool_gross_margin_underperformers(_: str) -> str:
    """
    Show ASINs with gross margin less than plan.
    Examples: 'Show ASINs with gross margin less than plan'.
    Saves table in session_state['gm_underperformers_table'].
    """
    try:
        # Load the underperformers file
        gm_under = pd.read_excel("GM_Underperformers_ten_west.xlsx")

        # Clean and prepare the data
        gm_under = gm_under.dropna(subset=['ASIN'])
        gm_under['ASIN'] = gm_under['ASIN'].astype(str).str.upper().str.strip()
        
        # Convert numeric columns - use the actual column names from the file
        for col in ["Expected Gross Margin (%)", "Actual Gross Margin (%)"]:
            gm_under[col] = (
                pd.to_numeric(
                    gm_under[col]
                            .astype(str)
                            .str.rstrip("%"),
                    errors="coerce"
                )
                .fillna(0.0)
            )
        
        # Use the existing brand_map from the global scope
        # Map ASINs to brands using the existing brand_map
        gm_under["Brand"] = gm_under["ASIN"].map(brand_map).fillna("UNKNOWN").str.upper()
        
        # Filter ASINs with gross margin less than plan
        underperformers = gm_under[gm_under['Actual Gross Margin (%)'] < gm_under['Expected Gross Margin (%)']].copy()
        
        if underperformers.empty:
            st.session_state["gm_underperformers_table"] = pd.DataFrame(columns=['Brand', 'ASIN', 'Planned Gross Margin', 'Actual Gross Margin'])
            return "No ASINs found with gross margin below plan."
        
        # Sort by the difference (expected - actual)
        underperformers['Difference'] = underperformers['Expected Gross Margin (%)'] - underperformers['Actual Gross Margin (%)']
        underperformers = underperformers.sort_values('Difference', ascending=False)
        
        # Format the floats back to strings with "%"
        underperformers["Expected Gross Margin (%)"] = underperformers["Expected Gross Margin (%)"].map("{:.2f}%".format)
        underperformers["Actual Gross Margin (%)"] = underperformers["Actual Gross Margin (%)"].map("{:.2f}%".format)
        
        # Rename columns to match requested format
        display_table = underperformers.rename(
            columns={
                "Expected Gross Margin (%)": "Planned Gross Margin",
                "Actual Gross Margin (%)": "Actual Gross Margin"
            }
        )[['Brand', 'ASIN', 'Planned Gross Margin', 'Actual Gross Margin']]
        
        # Reset index to start at 1
        display_table = display_table.reset_index(drop=True)
        display_table.index = range(1, len(display_table) + 1)
        display_table.index.name = "No."
        
        st.session_state["gm_underperformers_table"] = display_table
        return "ok"
        
    except Exception as e:
        return f"Error processing gross margin underperformers: {str(e)}"

def tool_gross_profit_for_all_asins(_: str) -> str:
    """
    Show gross profit for all ASINs.
    Examples: 'Show gross profit for all ASINs'.
    Returns formatted string like chatbotv10.py.
    """
    try:
        # Use the exact same logic as chatbotv10.py
        # 1) Clean & convert your columns
        gp_series = pd.to_numeric(
            asin_df["Gross Profit"].astype(str)
                  .str.replace(r"[\$,]", "", regex=True),
            errors="coerce"
        ).fillna(0)
        qty_series = pd.to_numeric(asin_df["quantity"], errors="coerce").fillna(0)

        # 2) Compute totals
        total_gp    = gp_series.sum()
        total_units = qty_series.sum()
        per_unit_gp = total_gp / total_units if total_units > 0 else 0

        # 3) Format & return string like chatbotv10.py
        formatted_total    = f"${total_gp:,.2f}"
        formatted_per_unit = f"${per_unit_gp:,.2f}"
        
        # Store the formatted string in session state
        st.session_state["gp_all_asins_string"] = (
            f"The overall gross profit for all ASINs was {formatted_total} "
            f"({formatted_per_unit} per unit) "
            f"in the settlement period {settlement_period}."
        )
        
        return "ok"
        
    except Exception as e:
        return f"Error processing gross profit for all ASINs: {str(e)}"

def tool_gross_profit_for_all_brands(_: str) -> str:
    """
    Show gross profit for all brands.
    Examples: 'Show gross profit for all brands'.
    Saves formatted string in session_state['gp_all_brands_string'].
    """
    try:
        # Use the exact same logic as chatbotv10.py
        # 1) Clean & convert your columns
        gp_series = pd.to_numeric(
            asin_df["Gross Profit"].astype(str)
                  .str.replace(r"[\$,]", "", regex=True),
            errors="coerce"
        ).fillna(0.0)
        qty_series = pd.to_numeric(
            asin_df["quantity"], errors="coerce"
        ).fillna(0)

        # 2) Compute totals
        total_gp    = gp_series.sum()
        total_units = qty_series.sum()
        per_unit_gp = total_gp / total_units if total_units > 0 else 0.0

        # 3) Format & output (using exact same logic as chatbotv10b.py)
        formatted_total    = f"${total_gp:,.2f}"
        formatted_per_unit = f"${per_unit_gp:,.2f}"
        formatted_string = f"The overall gross profit for all brands was {formatted_total} ({formatted_per_unit} per unit) in the settlement period {settlement_period}."
        
        st.session_state["gp_all_brands_string"] = formatted_string
        return "ok"
        
    except Exception as e:
        return f"Error processing gross profit for all brands: {str(e)}"

def tool_gross_margin_for_all_asins(_: str) -> str:
    """
    Show gross margin for all ASINs.
    Examples: 'Show gross margin for all ASINs'.
    Saves formatted string in session_state['gm_all_asins_string'].
    """
    try:
        # Use the exact same logic as chatbotv10.py for gross profit
        # 1) Clean & convert your columns
        gp_series = pd.to_numeric(
            asin_df["Gross Profit"].astype(str)
                  .str.replace(r"[\$,]", "", regex=True),
            errors="coerce"
        ).fillna(0)
        qty_series = pd.to_numeric(asin_df["quantity"], errors="coerce").fillna(0)

        # Create summary table using the same logic
        df = asin_df.copy()
        df['Gross Profit'] = gp_series
        df['quantity'] = qty_series
        
        # Calculate gross margin percentage (if we have the data)
        if 'Gross Margin %' in df.columns:
            df['Gross Margin %'] = pd.to_numeric(df['Gross Margin %'], errors='coerce').fillna(0)
        else:
            # Calculate margin as percentage of ATS (this is a simplified calculation)
            df['Gross Margin %'] = 0.0  # Default if no margin data
        
        # Calculate overall gross margin percentage (weighted average)
        total_gp = gp_series.sum()
        total_ats = pd.to_numeric(
            asin_df["Amazon Top-line Sales (ATS)"].astype(str)
                  .str.replace(r"[\$,]", "", regex=True),
            errors="coerce"
        ).fillna(0).sum()
        
        overall_margin = (total_gp / total_ats * 100) if total_ats > 0 else 0.0
        
        # Format the margin percentage
        formatted_margin = f"{overall_margin:.2f}%"
        
        # Create formatted string
        formatted_string = f"The overall gross margin for all ASINs was {formatted_margin} in the settlement period {settlement_period}."
        
        st.session_state["gm_all_asins_string"] = formatted_string
        return "ok"
        
    except Exception as e:
        return f"Error processing gross margin for all ASINs: {str(e)}"

def tool_gross_margin_for_all_brands(_: str) -> str:
    """
    Show gross margin for all brands.
    Examples: 'Show gross margin for all brands'.
    Saves formatted string in session_state['gm_all_brands_string'].
    """
    try:
        # Use the exact same logic as chatbotv10.py for gross profit
        # 1) Clean & convert your columns
        gp_series = pd.to_numeric(
            asin_df["Gross Profit"].astype(str)
                  .str.replace(r"[\$,]", "", regex=True),
            errors="coerce"
        ).fillna(0)
        qty_series = pd.to_numeric(asin_df["quantity"], errors="coerce").fillna(0)

        # Create summary table using the same logic
        df = asin_df.copy()
        df['Gross Profit'] = gp_series
        df['quantity'] = qty_series
        
        # Calculate gross margin percentage (if we have the data)
        if 'Gross Margin %' in df.columns:
            df['Gross Margin %'] = pd.to_numeric(df['Gross Margin %'], errors='coerce').fillna(0)
        else:
            # Calculate margin as percentage of ATS (this is a simplified calculation)
            df['Gross Margin %'] = 0.0  # Default if no margin data
        
        # Calculate overall gross margin percentage (weighted average)
        total_gp = gp_series.sum()
        total_ats = pd.to_numeric(
            asin_df["Amazon Top-line Sales (ATS)"].astype(str)
                  .str.replace(r"[\$,]", "", regex=True),
            errors="coerce"
        ).fillna(0).sum()
        
        overall_margin = (total_gp / total_ats * 100) if total_ats > 0 else 0.0
        
        # Format the margin percentage
        formatted_margin = f"{overall_margin:.2f}%"
        
        # Create formatted string
        formatted_string = f"The overall gross margin for all brands was {formatted_margin} in the settlement period {settlement_period}."
        
        st.session_state["gm_all_brands_string"] = formatted_string
        return "ok"
        
    except Exception as e:
        return f"Error processing gross margin for all brands: {str(e)}"


def tool_sales_descending_order(_: str) -> str:
    """
    Show ASINs sales in descending order.
    Examples: 'Show ASINs sales in descending order'.
    Saves formatted string and table in session_state['sales_desc_string'] and 'sales_desc_table'.
    """
    # Extract Brand, ASIN, and ATS
    df_sales = (
        asin_df[["Brands", "ASIN", "Amazon Top-line Sales (ATS)"]]
          .rename(columns={"Brands": "Brand"})
          .copy()
    )

    # Clean & convert ATS to numeric
    df_sales["ATS_num"] = (
        df_sales["Amazon Top-line Sales (ATS)"]
          .astype(str)
          .str.replace(r"[\$,]", "", regex=True)
          .astype(float)
          .fillna(0.0)
    )

    # Rename the display column
    df_sales = df_sales.rename(columns={"Amazon Top-line Sales (ATS)": "Sales"})

    # Sort descending, reset index at 1
    df_sales = (
        df_sales
        .sort_values("ATS_num", ascending=False)
        .reset_index(drop=True)
    )
    df_sales.index = df_sales.index + 1
    df_sales.index.name = "No."

    # Create formatted string
    formatted_string = f"Here is the list of ASINs' sales in descending order for the settlement period {settlement_period}:"

    # Save for UI
    st.session_state["sales_desc_string"] = formatted_string
    st.session_state["sales_desc_table"] = df_sales[["Brand", "ASIN", "Sales"]]
    st.session_state["sales_desc_period"] = settlement_period
    return "ok"


def tool_gross_profit_descending_order(_: str) -> str:
    """
    Show ASINs gross profit in descending order.
    Examples: 'Show ASINs gross profit in descending order'.
    Saves formatted string and table in session_state['gp_desc_string'] and 'gp_desc_table'.
    """
    # Pull Brand, ASIN & Gross Profit, strip "$,"
    df_gp = (
        asin_df[["Brands", "ASIN", "Gross Profit"]]
        .rename(columns={"Brands": "Brand"})
        .copy()
    )

    # Drop any rows with blank/missing Brand or ASIN
    df_gp = df_gp[df_gp["Brand"].notna() & df_gp["ASIN"].notna()]
    df_gp = df_gp[
        (df_gp["Brand"].astype(str).str.strip() != "") &
        (df_gp["ASIN"].astype(str).str.strip()  != "")
    ]

    # Convert Gross Profit to numeric for sorting
    df_gp["GP_num"] = (
        df_gp["Gross Profit"]
          .astype(str)
          .str.replace(r"[\$,]", "", regex=True)
          .astype(float)
          .fillna(0.0)
    )

    # Sort descending, reset index to start at 1
    df_gp = df_gp.sort_values("GP_num", ascending=False).reset_index(drop=True)
    df_gp.index = df_gp.index + 1
    df_gp.index.name = "No."

    # Create formatted string
    formatted_string = f"Here is the list of ASINs' gross profit in descending order for the settlement period {settlement_period}:"

    # Save for UI
    st.session_state["gp_desc_string"] = formatted_string
    st.session_state["gp_desc_table"] = df_gp[["Brand", "ASIN", "Gross Profit"]]
    st.session_state["gp_desc_period"] = settlement_period
    return "ok"


def tool_gross_margin_descending_order(_: str) -> str:
    """
    Show ASINs gross margin in descending order.
    Examples: 'Show ASINs gross margin in descending order'.
    Saves formatted string and table in session_state['gm_desc_string'] and 'gm_desc_table'.
    """
    # 1) Pull Brand, ASIN & Gross Margin, convert to numeric
    df_gm = (
        asin_df[["Brands", "ASIN", "Gross Margin"]]
        .rename(columns={"Brands": "Brand"})
        .copy()
    )
    df_gm["GM_num"] = pd.to_numeric(
        df_gm["Gross Margin"]
              .astype(str)
              .str.rstrip("%"),
        errors="coerce"
    ).fillna(0.0)

    # 2) Sort descending, reset index to start at 1
    df_gm = df_gm.sort_values("GM_num", ascending=False).reset_index(drop=True)
    df_gm.index = df_gm.index + 1
    df_gm.index.name = "No."

    # 3) Format Gross Margin for display (add "%")
    df_gm["Gross Margin"] = df_gm["GM_num"].map(lambda x: f"{x:.2f}%")

    # 4) Create formatted string
    formatted_string = f"Here is the list of ASINs' gross margin in descending order for the settlement period {settlement_period}:"

    # 5) Save for UI
    st.session_state["gm_desc_string"] = formatted_string
    st.session_state["gm_desc_table"] = df_gm[["Brand", "ASIN", "Gross Margin"]]
    st.session_state["gm_desc_period"] = settlement_period
    return "ok"


def tool_brands_sales_descending_order(_: str) -> str:
    """
    Show brands sales in descending order.
    Examples: 'Show brands sales in descending order'.
    Saves formatted string and table in session_state['brands_sales_desc_string'] and 'brands_sales_desc_table'.
    """
    # Load brand data
    try:
        brand_df = pd.read_excel(BRAND_REPORT_PATH)
    except FileNotFoundError:
        st.session_state["agent_error"] = f"Brand report file {BRAND_REPORT_PATH} not found."
        return "no_data"

    # Pull Brand & ATS, strip "$,"
    dfb_sales = brand_df[["Brands", "Amazon Top-line Sales (ATS)"]].copy()

    # Drop any rows with blank or missing Brands
    dfb_sales = dfb_sales[
        dfb_sales["Brands"].notna() &
        (dfb_sales["Brands"].astype(str).str.strip() != "")
    ]

    # Convert ATS to numeric for sorting
    dfb_sales["ATS_num"] = pd.to_numeric(
        dfb_sales["Amazon Top-line Sales (ATS)"]
            .astype(str)
            .str.replace(r"[\$,]", "", regex=True),
        errors="coerce"
    ).fillna(0.0)

    # Sort descending, reset index to start at 1
    dfb_sales = dfb_sales.sort_values("ATS_num", ascending=False).reset_index(drop=True)
    dfb_sales.index = dfb_sales.index + 1
    dfb_sales.index.name = "No."

    # Rename columns to requested format
    dfb_sales = dfb_sales.rename(
        columns={
            "Brands": "Brand",
            "Amazon Top-line Sales (ATS)": "Sales"
        }
    )

    # Create formatted string
    formatted_string = f"Here is the list of sales by brand in descending order for the settlement period {settlement_period}:"

    # Save for UI
    st.session_state["brands_sales_desc_string"] = formatted_string
    st.session_state["brands_sales_desc_table"] = dfb_sales[["Brand", "Sales"]]
    st.session_state["brands_sales_desc_period"] = settlement_period
    return "ok"


def tool_brands_gross_profit_descending_order(_: str) -> str:
    """
    Show brands gross profit in descending order.
    Examples: 'Show brands gross profit in descending order'.
    Saves formatted string and table in session_state['brands_gp_desc_string'] and 'brands_gp_desc_table'.
    """
    # Load brand data
    try:
        brand_df = pd.read_excel(BRAND_REPORT_PATH)
    except FileNotFoundError:
        st.session_state["agent_error"] = f"Brand report file {BRAND_REPORT_PATH} not found."
        return "no_data"

    # Extract Brand & Gross Profit, clean "$,"
    dfb_gp = (
        brand_df[["Brands", "Gross Profit"]]
        .rename(columns={"Brands": "Brand"})
        .copy()
    )
    dfb_gp["GP_num"] = pd.to_numeric(
        dfb_gp["Gross Profit"]
              .astype(str)
              .str.replace(r"[\$,]", "", regex=True),
        errors="coerce"
    ).fillna(0.0)

    # Sort descending, reset index to start at 1
    dfb_gp = dfb_gp.sort_values("GP_num", ascending=False).reset_index(drop=True)
    dfb_gp.index = dfb_gp.index + 1
    dfb_gp.index.name = "No."

    # Create formatted string
    formatted_string = f"Here is the list of gross profit by brand in descending order for the settlement period {settlement_period}:"

    # Save for UI
    st.session_state["brands_gp_desc_string"] = formatted_string
    st.session_state["brands_gp_desc_table"] = dfb_gp[["Brand", "Gross Profit"]]
    st.session_state["brands_gp_desc_period"] = settlement_period
    return "ok"


def tool_brands_gross_margin_descending_order(_: str) -> str:
    """
    Show brands gross margin in descending order.
    Examples: 'Show brands gross margin in descending order'.
    Saves formatted string and table in session_state['brands_gm_desc_string'] and 'brands_gm_desc_table'.
    """
    # Load brand data
    try:
        brand_df = pd.read_excel(BRAND_REPORT_PATH)
    except FileNotFoundError:
        st.session_state["agent_error"] = f"Brand report file {BRAND_REPORT_PATH} not found."
        return "no_data"

    # Extract Brand & Gross Margin, strip "%" and convert to float
    dfb_gm = (
        brand_df[["Brands", "Gross Margin"]]
        .rename(columns={"Brands": "Brand"})
        .copy()
    )
    dfb_gm["GM_num"] = pd.to_numeric(
        dfb_gm["Gross Margin"]
              .astype(str)
              .str.rstrip("%"),
        errors="coerce"
    ).fillna(0.0)

    # Sort descending, reset index to start at 1
    dfb_gm = dfb_gm.sort_values("GM_num", ascending=False).reset_index(drop=True)
    dfb_gm.index = dfb_gm.index + 1
    dfb_gm.index.name = "No."

    # Format Gross Margin back to "xx.xx%"
    dfb_gm["Gross Margin"] = dfb_gm["GM_num"].map("{:.2f}%".format)

    # Create formatted string
    formatted_string = f"Here is the list of gross margin by brand in descending order for the settlement period {settlement_period}:"

    # Save for UI
    st.session_state["brands_gm_desc_string"] = formatted_string
    st.session_state["brands_gm_desc_table"] = dfb_gm[["Brand", "Gross Margin"]]
    st.session_state["brands_gm_desc_period"] = settlement_period
    return "ok"


def tool_top_brands_sales(prompt: str) -> str:
    """
    Show top X brands sales in descending order.
    Examples: 'Show top 5 brands sales in descending order'.
    Saves formatted string and table in session_state['top_brands_sales_string'] and 'top_brands_sales_table'].
    """
    # Extract number from prompt
    m = re.search(r"\bshow\s+top\s+(\d+)\s+brands\s+", prompt.lower())
    if not m:
        st.session_state["agent_error"] = "Please specify a number (e.g., 'Show top 5 brands sales')."
        return "no_data"
    
    N = int(m.group(1))

    # Load brand data
    try:
        brand_df = pd.read_excel(BRAND_REPORT_PATH)
    except FileNotFoundError:
        st.session_state["agent_error"] = f"Brand report file {BRAND_REPORT_PATH} not found."
        return "no_data"

    # Pull Brand + Sales
    dfb = brand_df[["Brands", "Amazon Top-line Sales (ATS)"]].copy()
    dfb.rename(columns={
        "Brands": "Brand",
        "Amazon Top-line Sales (ATS)": "Sales"
    }, inplace=True)

    # Clean & convert Sales to numeric
    dfb["Sales_num"] = (
        dfb["Sales"]
          .astype(str)
          .str.replace(r"[\$,]", "", regex=True)
          .astype(float)
          .fillna(0.0)
    )

    # Sort descending, take top N
    out = (
        dfb.sort_values("Sales_num", ascending=False)
           .head(N)
           .reset_index(drop=True)
    )

    # Format Sales back to dollar strings
    out["Sales"] = out["Sales_num"].map("${:,.2f}".format)

    # Select only the columns we want
    out = out[["Brand", "Sales"]]

    # Make the index start at 1
    out.index = out.index + 1

    # Create formatted string
    formatted_string = f"Here is the list of brands' sales in descending order for the settlement period {settlement_period}:"

    # Save for UI
    st.session_state["top_brands_sales_string"] = formatted_string
    st.session_state["top_brands_sales_table"] = out
    st.session_state["top_brands_sales_count"] = N
    st.session_state["top_brands_sales_period"] = settlement_period
    return "ok"


def tool_top_asins_sales(prompt: str) -> str:
    """
    Show top X ASINs sales in descending order.
    Examples: 'Show top 10 ASINs sales in descending order'.
    Saves formatted string and table in session_state['top_asins_sales_string'] and 'top_asins_sales_table'].
    """
    # Extract number from prompt
    m = re.search(r"\bshow\s+top\s+(\d+)\s+asins?\s+", prompt.lower())
    if not m:
        st.session_state["agent_error"] = "Please specify a number (e.g., 'Show top 10 ASINs sales')."
        return "no_data"
    
    N = int(m.group(1))

    # Pull Brand, ASIN + ATS
    df_sales = (
        asin_df[["Brands", "ASIN", "Amazon Top-line Sales (ATS)"]]
        .rename(columns={"Brands": "Brand"})
        .copy()
    )

    # Clean and convert
    df_sales["ATS_num"] = (
        df_sales["Amazon Top-line Sales (ATS)"]
          .astype(str)
          .str.replace(r"[\$,]", "", regex=True)
          .astype(float)
          .fillna(0.0)
    )

    # Sort descending, take top N, reset index
    out = (
        df_sales.sort_values("ATS_num", ascending=False)
                .head(N)
                .reset_index(drop=True)
    )

    # Format ATS back to dollars & rename
    out["Sales"] = out["ATS_num"].map("${:,.2f}".format)

    # Keep only the columns we want
    out = out[["Brand", "ASIN", "Sales"]]

    # 1‑index the table and name the index
    out.index = out.index + 1
    out.index.name = "No."

    # Create formatted string
    formatted_string = f"Here is the list of ASINs' sales in descending order for the settlement period {settlement_period}:"

    # Save for UI
    st.session_state["top_asins_sales_string"] = formatted_string
    st.session_state["top_asins_sales_table"] = out
    st.session_state["top_asins_sales_count"] = N
    st.session_state["top_asins_sales_period"] = settlement_period
    return "ok"


def tool_fees_higher_than_plan(_: str) -> str:
    """Show ASINs with average total fees higher than plan."""
    try:
        # Load the data
        asin_df = pd.read_excel(ASIN_REPORT_PATH)
        
        # Use global settlement period
        
        # Calculate total fees for each ASIN
        fee_cols = [
            "Referral Fee", "FBA Fulfillment Fee", "other transaction fees",
            "Adjusted Shipping/Kitting Fees", "Labeling/Polybagging Fees",
            "Storage Fees", "Allocated fees (Premium Services Fee + Subscription)",
            "Adjusted Cost of Returns"
        ]
        
        # Create a copy for calculations
        df = asin_df.copy()
        
        # Calculate total fees for each ASIN
        df['Total Fees'] = 0.0
        for col in fee_cols:
            if col in df.columns:
                df['Total Fees'] += pd.to_numeric(
                    df[col].astype(str).str.replace(r"[\$,]", "", regex=True),
                    errors="coerce"
                ).fillna(0.0)
        
        # Calculate average fees per unit
        df['Avg Fees Per Unit'] = df['Total Fees'] / df['quantity']
        
        # Load planned fees data (assuming it exists in the same file or a separate file)
        # For now, we'll use a simple threshold - you may need to adjust this
        threshold = df['Avg Fees Per Unit'].mean() * 1.2  # 20% above average
        
        # Filter ASINs with fees higher than plan
        high_fees_df = df[df['Avg Fees Per Unit'] > threshold].copy()
        
        if high_fees_df.empty:
            st.session_state["fees_higher_plan_table"] = None
            st.session_state["fees_higher_plan_period"] = settlement_period
            return "ok"
        
        # Prepare output table
        output_df = high_fees_df[['Brands', 'ASIN', 'Total Fees', 'Avg Fees Per Unit']].copy()
        output_df = output_df.rename(columns={'Brands': 'Brand'})
        
        # Format the monetary columns
        output_df['Total Fees'] = output_df['Total Fees'].map("${:,.2f}".format)
        output_df['Avg Fees Per Unit'] = output_df['Avg Fees Per Unit'].map("${:,.2f}".format)
        
        # Reset index for display
        output_df = output_df.reset_index(drop=True)
        output_df.index = range(1, len(output_df) + 1)
        output_df.index.name = "No."
        
        st.session_state["fees_higher_plan_table"] = output_df
        st.session_state["fees_higher_plan_period"] = settlement_period
        
        return "ok"
        
    except Exception as e:
        st.session_state["agent_error"] = f"Error processing fees higher than plan: {str(e)}"
        return "error"


def tool_units_sold_for_specific_asin(prompt: str) -> str:
    """Show units sold for a specific ASIN."""
    try:
        # Extract ASIN from prompt
        asin_match = re.search(r'\b(B0[0-9A-Z]{8})\b', prompt.upper())
        if not asin_match:
            st.session_state["agent_error"] = "No valid ASIN found in the prompt. Please provide an ASIN in B0XXXXXXXX format."
            return "error"
        
        asin = asin_match.group(1)
        
        # Load the data
        asin_df = pd.read_excel(ASIN_REPORT_PATH)
        
        # Use global settlement period
        
        # Find the ASIN
        row = asin_df[asin_df["ASIN"] == asin]
        if row.empty:
            st.session_state["agent_error"] = f"ASIN {asin} not found in the data."
            return "error"
        
        # Get units sold
        units = int(row["quantity"].iloc[0])
        
        # Store the result
        st.session_state["units_sold_asin"] = asin
        st.session_state["units_sold_count"] = units
        st.session_state["units_sold_period"] = settlement_period
        
        return "ok"
        
    except Exception as e:
        st.session_state["agent_error"] = f"Error processing units sold for ASIN: {str(e)}"
        return "error"


def tool_sales_for_specific_asin(prompt: str) -> str:
    """Show sales for a specific ASIN."""
    try:
        # Extract ASIN from prompt
        asin_match = re.search(r'\b(B0[0-9A-Z]{8})\b', prompt.upper())
        if not asin_match:
            st.session_state["agent_error"] = "No valid ASIN found in the prompt. Please provide an ASIN in B0XXXXXXXX format."
            return "error"
        
        asin = asin_match.group(1)
        
        # Load the data
        asin_df = pd.read_excel(ASIN_REPORT_PATH)
        
        # Use global settlement period
        
        # Find the ASIN
        row = asin_df[asin_df["ASIN"] == asin]
        if row.empty:
            st.session_state["agent_error"] = f"ASIN {asin} not found in the data."
            return "error"
        
        # Get sales and units
        raw_sales = row["Amazon Top-line Sales (ATS)"].iloc[0]
        sales_num = float(str(raw_sales).replace("$","").replace(",",""))
        units = int(row["quantity"].iloc[0])
        
        # Store the result
        st.session_state["sales_asin"] = asin
        st.session_state["sales_amount"] = sales_num
        st.session_state["sales_units"] = units
        st.session_state["sales_period"] = settlement_period
        
        return "ok"
        
    except Exception as e:
        st.session_state["agent_error"] = f"Error processing sales for ASIN: {str(e)}"
        return "error"


def tool_total_fees_for_specific_asin(prompt: str) -> str:
    """Show total fees for a specific ASIN."""
    try:
        # Extract ASIN from prompt
        asin_match = re.search(r'\b(B0[0-9A-Z]{8})\b', prompt.upper())
        if not asin_match:
            st.session_state["agent_error"] = "No valid ASIN found in the prompt. Please provide an ASIN in B0XXXXXXXX format."
            return "error"
        
        asin = asin_match.group(1)
        
        # Load the data
        asin_df = pd.read_excel(ASIN_REPORT_PATH)
        
        # Use global settlement period
        
        # Find the ASIN
        row = asin_df[asin_df["ASIN"] == asin]
        if row.empty:
            st.session_state["agent_error"] = f"ASIN {asin} not found in the data."
            return "error"
        
        # Calculate total fees
        fee_cols = [
            "Referral Fee", "FBA Fulfillment Fee", "other transaction fees",
            "Adjusted Shipping/Kitting Fees", "Labeling/Polybagging Fees",
            "Storage Fees", "Allocated fees (Premium Services Fee + Subscription)",
            "Adjusted Cost of Returns"
        ]
        
        total_fees = 0.0
        for col in fee_cols:
            if col in row.columns:
                try:
                    total_fees += float(str(row[col].iloc[0]).replace("$","").replace(",",""))
                except:
                    pass
        
        units = int(row["quantity"].iloc[0])
        per_unit_fees = total_fees / units if units else 0.0
        
        # Store the result
        st.session_state["fees_asin"] = asin
        st.session_state["fees_total"] = total_fees
        st.session_state["fees_per_unit"] = per_unit_fees
        st.session_state["fees_period"] = settlement_period
        
        return "ok"
        
    except Exception as e:
        st.session_state["agent_error"] = f"Error processing total fees for ASIN: {str(e)}"
        return "error"


def tool_gross_profit_for_specific_asin(prompt: str) -> str:
    """Show gross profit for a specific ASIN."""
    try:
        # Extract ASIN from prompt
        asin_match = re.search(r'\b(B0[0-9A-Z]{8})\b', prompt.upper())
        if not asin_match:
            st.session_state["agent_error"] = "No valid ASIN found in the prompt. Please provide an ASIN in B0XXXXXXXX format."
            return "error"
        
        asin = asin_match.group(1)
        
        # Load the data
        asin_df = pd.read_excel(ASIN_REPORT_PATH)
        
        # Use global settlement period
        
        # Find the ASIN
        row = asin_df[asin_df["ASIN"] == asin]
        if row.empty:
            st.session_state["agent_error"] = f"ASIN {asin} not found in the data."
            return "error"
        
        # Get gross profit and units
        raw_gp = row["Gross Profit"].iloc[0]
        gp_num = float(str(raw_gp).replace("$","").replace(",",""))
        units = int(row["quantity"].iloc[0])
        per_unit_gp = gp_num / units if units else 0.0
        
        # Store the result
        st.session_state["gp_asin"] = asin
        st.session_state["gp_amount"] = gp_num
        st.session_state["gp_per_unit"] = per_unit_gp
        st.session_state["gp_period"] = settlement_period
        
        return "ok"
        
    except Exception as e:
        st.session_state["agent_error"] = f"Error processing gross profit for ASIN: {str(e)}"
        return "error"


def tool_gross_margin_for_specific_asin(prompt: str) -> str:
    """Show gross margin for a specific ASIN."""
    try:
        # Extract ASIN from prompt
        asin_match = re.search(r'\b(B0[0-9A-Z]{8})\b', prompt.upper())
        if not asin_match:
            st.session_state["agent_error"] = "No valid ASIN found in the prompt. Please provide an ASIN in B0XXXXXXXX format."
            return "error"
        
        asin = asin_match.group(1)
        
        # Load the data
        asin_df = pd.read_excel(ASIN_REPORT_PATH)
        
        # Use global settlement period
        
        # Find the ASIN
        row = asin_df[asin_df["ASIN"] == asin]
        if row.empty:
            st.session_state["agent_error"] = f"ASIN {asin} not found in the data."
            return "error"
        
        # Get gross margin
        raw_margin = row["Gross Margin"].iloc[0]
        try:
            mval = float(str(raw_margin).rstrip("%"))
            if mval <= 1:
                mval *= 100
            margin_str = f"{mval:.2f}%"
        except:
            margin_str = str(raw_margin)
        
        # Store the result
        st.session_state["gm_asin"] = asin
        st.session_state["gm_margin"] = margin_str
        st.session_state["gm_period"] = settlement_period
        
        return "ok"
        
    except Exception as e:
        st.session_state["agent_error"] = f"Error processing gross margin for ASIN: {str(e)}"
        return "error"


def tool_top_brands_gross_profit(prompt: str) -> str:
    """
    Show top X brands gross profit in descending order.
    Examples: 'Show top 5 brands gross profit in descending order'.
    Saves formatted string and table in session_state['top_brands_gp_string'] and 'top_brands_gp_table'].
    """
    # Extract number from prompt
    m = re.search(r"\bshow\s+top\s+(\d+)\s+brands\s+", prompt.lower())
    if not m:
        st.session_state["agent_error"] = "Please specify a number (e.g., 'Show top 5 brands gross profit')."
        return "no_data"
    
    N = int(m.group(1))

    # Load brand data
    try:
        brand_df = pd.read_excel(BRAND_REPORT_PATH)
    except FileNotFoundError:
        st.session_state["agent_error"] = f"Brand report file {BRAND_REPORT_PATH} not found."
        return "no_data"

    # Pull Brand + Gross Profit
    dfb = brand_df[["Brands", "Gross Profit"]].copy()
    dfb.rename(columns={"Brands": "Brand"}, inplace=True)

    # Clean & convert Gross Profit to numeric
    dfb["GP_num"] = (
        dfb["Gross Profit"]
          .astype(str)
          .str.replace(r"[\$,]", "", regex=True)
          .astype(float)
          .fillna(0.0)
    )

    # Sort descending, take top N
    out = (
        dfb.sort_values("GP_num", ascending=False)
           .head(N)
           .reset_index(drop=True)
    )

    # Format Gross Profit back to dollar strings
    out["Gross Profit"] = out["GP_num"].map("${:,.2f}".format)

    # Select only the columns we want
    out = out[["Brand", "Gross Profit"]]

    # Make the index start at 1
    out.index = out.index + 1

    # Create formatted string
    formatted_string = f"Here is the list of brands' gross profit in descending order for the settlement period {settlement_period}:"

    # Save for UI
    st.session_state["top_brands_gp_string"] = formatted_string
    st.session_state["top_brands_gp_table"] = out
    st.session_state["top_brands_gp_count"] = N
    st.session_state["top_brands_gp_period"] = settlement_period
    return "ok"


def tool_top_brands_gross_margin(prompt: str) -> str:
    """
    Show top X brands gross margin in descending order.
    Examples: 'Show top 5 brands gross margin in descending order'.
    Saves formatted string and table in session_state['top_brands_gm_string'] and 'top_brands_gm_table'].
    """
    # Extract number from prompt
    m = re.search(r"\bshow\s+top\s+(\d+)\s+brands\s+", prompt.lower())
    if not m:
        st.session_state["agent_error"] = "Please specify a number (e.g., 'Show top 5 brands gross margin')."
        return "no_data"
    
    N = int(m.group(1))

    # Load brand data
    try:
        brand_df = pd.read_excel(BRAND_REPORT_PATH)
    except FileNotFoundError:
        st.session_state["agent_error"] = f"Brand report file {BRAND_REPORT_PATH} not found."
        return "no_data"

    # Pull Brand + Gross Margin
    dfb = brand_df[["Brands", "Gross Margin"]].copy()
    dfb.rename(columns={"Brands": "Brand"}, inplace=True)

    # Clean & convert Gross Margin to numeric
    dfb["GM_num"] = (
        dfb["Gross Margin"]
          .astype(str)
          .str.rstrip("%")
          .astype(float)
          .fillna(0.0)
    )

    # Sort descending, take top N
    out = (
        dfb.sort_values("GM_num", ascending=False)
           .head(N)
           .reset_index(drop=True)
    )

    # Format Gross Margin back to percentage strings
    out["Gross Margin"] = out["GM_num"].map("{:.2f}%".format)

    # Select only the columns we want
    out = out[["Brand", "Gross Margin"]]

    # Make the index start at 1
    out.index = out.index + 1

    # Create formatted string
    formatted_string = f"Here is the list of brands' gross margin in descending order for the settlement period {settlement_period}:"

    # Save for UI
    st.session_state["top_brands_gm_string"] = formatted_string
    st.session_state["top_brands_gm_table"] = out
    st.session_state["top_brands_gm_count"] = N
    st.session_state["top_brands_gm_period"] = settlement_period
    return "ok"


def tool_top_asins_gross_profit(prompt: str) -> str:
    """
    Show top X ASINs gross profit in descending order.
    Examples: 'Show top 10 ASINs gross profit in descending order'.
    Saves formatted string and table in session_state['top_asins_gp_string'] and 'top_asins_gp_table'].
    """
    # Extract number from prompt
    m = re.search(r"\bshow\s+top\s+(\d+)\s+asins?\s+", prompt.lower())
    if not m:
        st.session_state["agent_error"] = "Please specify a number (e.g., 'Show top 10 ASINs gross profit')."
        return "no_data"
    
    N = int(m.group(1))

    # Pull Brand, ASIN + Gross Profit
    df_gp = (
        asin_df[["Brands", "ASIN", "Gross Profit"]]
        .rename(columns={"Brands": "Brand"})
        .copy()
    )

    # Clean and convert
    df_gp["GP_num"] = (
        df_gp["Gross Profit"]
          .astype(str)
          .str.replace(r"[\$,]", "", regex=True)
          .astype(float)
          .fillna(0.0)
    )

    # Sort descending, take top N, reset index
    out = (
        df_gp.sort_values("GP_num", ascending=False)
                .head(N)
                .reset_index(drop=True)
    )

    # Format Gross Profit back to dollars & rename
    out["Gross Profit"] = out["GP_num"].map("${:,.2f}".format)

    # Keep only the columns we want
    out = out[["Brand", "ASIN", "Gross Profit"]]

    # 1‑index the table and name the index
    out.index = out.index + 1
    out.index.name = "No."

    # Create formatted string
    formatted_string = f"Here is the list of ASINs' gross profit in descending order for the settlement period {settlement_period}:"

    # Save for UI
    st.session_state["top_asins_gp_string"] = formatted_string
    st.session_state["top_asins_gp_table"] = out
    st.session_state["top_asins_gp_count"] = N
    st.session_state["top_asins_gp_period"] = settlement_period
    return "ok"


def tool_top_asins_gross_margin(prompt: str) -> str:
    """
    Show top X ASINs gross margin in descending order.
    Examples: 'Show top 10 ASINs gross margin in descending order'.
    Saves formatted string and table in session_state['top_asins_gm_string'] and 'top_asins_gm_table'].
    """
    # Extract number from prompt
    m = re.search(r"\bshow\s+top\s+(\d+)\s+asins?\s+", prompt.lower())
    if not m:
        st.session_state["agent_error"] = "Please specify a number (e.g., 'Show top 10 ASINs gross margin')."
        return "no_data"
    
    N = int(m.group(1))

    # Pull Brand, ASIN + Gross Margin
    df_gm = (
        asin_df[["Brands", "ASIN", "Gross Margin"]]
        .rename(columns={"Brands": "Brand"})
        .copy()
    )

    # Clean and convert
    df_gm["GM_num"] = (
        df_gm["Gross Margin"]
          .astype(str)
          .str.rstrip("%")
          .astype(float)
          .fillna(0.0)
    )

    # Sort descending, take top N, reset index
    out = (
        df_gm.sort_values("GM_num", ascending=False)
                .head(N)
                .reset_index(drop=True)
    )

    # Format Gross Margin back to percentages & rename
    out["Gross Margin"] = out["GM_num"].map("{:.2f}%".format)

    # Keep only the columns we want
    out = out[["Brand", "ASIN", "Gross Margin"]]

    # 1‑index the table and name the index
    out.index = out.index + 1
    out.index.name = "No."

    # Create formatted string
    formatted_string = f"Here is the list of ASINs' gross margin in descending order for the settlement period {settlement_period}:"

    # Save for UI
    st.session_state["top_asins_gm_string"] = formatted_string
    st.session_state["top_asins_gm_table"] = out
    st.session_state["top_asins_gm_count"] = N
    st.session_state["top_asins_gm_period"] = settlement_period
    return "ok"


def tool_total_sales_gross_profit_margin_brands(_: str) -> str:
    """
    Show total sales, gross profit, and margin for all brands.
    Examples: 'Show total sales, gross profit, and margin for all brands'.
    Saves formatted string and table in session_state['total_brands_string'] and 'total_brands_table'].
    """
    # Load brand data
    try:
        brand_df = pd.read_excel(BRAND_REPORT_PATH)
    except FileNotFoundError:
        st.session_state["agent_error"] = f"Brand report file {BRAND_REPORT_PATH} not found."
        return "no_data"

    # Pull Brand, Sales, Gross Profit, and Gross Margin
    dfb = brand_df[["Brands", "Amazon Top-line Sales (ATS)", "Gross Profit", "Gross Margin"]].copy()
    dfb.rename(columns={"Brands": "Brand"}, inplace=True)

    # Clean & convert Sales to numeric
    dfb["Sales_num"] = (
        dfb["Amazon Top-line Sales (ATS)"]
          .astype(str)
          .str.replace(r"[\$,]", "", regex=True)
          .astype(float)
          .fillna(0.0)
    )

    # Clean & convert Gross Profit to numeric
    dfb["GP_num"] = (
        dfb["Gross Profit"]
          .astype(str)
          .str.replace(r"[\$,]", "", regex=True)
          .astype(float)
          .fillna(0.0)
    )

    # Clean & convert Gross Margin to numeric
    dfb["GM_num"] = (
        dfb["Gross Margin"]
          .astype(str)
          .str.rstrip("%")
          .astype(float)
          .fillna(0.0)
    )

    # Format columns back to display format
    dfb["Sales"] = dfb["Sales_num"].map("${:,.2f}".format)
    dfb["Gross Profit"] = dfb["GP_num"].map("${:,.2f}".format)
    dfb["Gross Margin"] = dfb["GM_num"].map("{:.2f}%".format)

    # Select only the columns we want
    out = dfb[["Brand", "Sales", "Gross Profit", "Gross Margin"]]

    # Reset index for display
    out = out.reset_index(drop=True)
    out.index = range(1, len(out) + 1)
    out.index.name = "No."

    # Create formatted string
    formatted_string = f"Here is the list of each brand with its corresponding sales, gross profit, and gross margin for the settlement period {settlement_period}:"

    # Save for UI
    st.session_state["total_brands_string"] = formatted_string
    st.session_state["total_brands_table"] = out
    st.session_state["total_brands_period"] = settlement_period
    return "ok"


def tool_orders_sales_price_lower_plan(_: str) -> str:
    """
    Show orders where the sales price was lower than plan for ASIN XYZ.
    Examples: 'Show orders where the sales price was lower than plan for ASIN B0XXXXXXXX'.
    Saves formatted string and table in session_state['orders_sales_lower_string'] and 'orders_sales_lower_table'].
    """
    # This function would need to be implemented based on the order-level data
    # For now, we'll create a placeholder that shows the format
    formatted_string = f"Here are all orders where the sales price was lower than planned for the settlement period {settlement_period}:"
    
    # Create a placeholder table (this would need actual implementation)
    placeholder_data = {
        "Order ID": ["Order1", "Order2"],
        "ASIN": ["B0XXXXXXXX", "B0XXXXXXXX"],
        "Sales Price": ["$10.00", "$9.50"],
        "Planned Price": ["$12.00", "$12.00"],
        "Difference": ["-$2.00", "-$2.50"]
    }
    out = pd.DataFrame(placeholder_data)
    
    # Save for UI
    st.session_state["orders_sales_lower_string"] = formatted_string
    st.session_state["orders_sales_lower_table"] = out
    st.session_state["orders_sales_lower_period"] = settlement_period
    return "ok"


def tool_asins_avg_price_lower_plan(_: str) -> str:
    """
    Show ASINs where the average sales price was lower than plan.
    Examples: 'Show ASINs where the average sales price was lower than plan'.
    Saves formatted string and table in session_state['asins_avg_price_lower_string'] and 'asins_avg_price_lower_table'].
    """
    # This function would need to be implemented based on the ASIN-level data
    # For now, we'll create a placeholder that shows the format
    formatted_string = f"Here are all ASINs where the average sales price was lower than planned for the settlement period {settlement_period}:"
    
    # Create a placeholder table (this would need actual implementation)
    placeholder_data = {
        "Brand": ["Brand1", "Brand2"],
        "ASIN": ["B0XXXXXXXX", "B0YYYYYYYY"],
        "Average Sales Price": ["$10.50", "$9.75"],
        "Planned Price": ["$12.00", "$11.00"],
        "Difference": ["-$1.50", "-$1.25"]
    }
    out = pd.DataFrame(placeholder_data)
    
    # Save for UI
    st.session_state["asins_avg_price_lower_string"] = formatted_string
    st.session_state["asins_avg_price_lower_table"] = out
    st.session_state["asins_avg_price_lower_period"] = settlement_period
    return "ok"


def tool_asins_fees_higher_plan(_: str) -> str:
    """
    Show ASINs with average total fees higher than plan for the last settlement period.
    Examples: 'Show ASINs with average total fees higher than plan for the last settlement period'.
    Saves formatted string and table in session_state['asins_fees_higher_string'] and 'asins_fees_higher_table'].
    """
    try:
        # 1) load your fee‑per‑unit underperformers
        fees_df = pd.read_excel(
            "Fees_PerUnit_Underperformers.xlsx",
            sheet_name="Fee_Per_Unit_Under"
        )

        # 2) strip $ and commas, then convert to numeric
        for col in ("Actual Fees per Unit", "Expected Fees per Unit"):
            fees_df[col] = (
                fees_df[col]
                  .astype(str)
                  .str.replace(r"[\$,]", "", regex=True)
                  .astype(float)
            )

        # 3) pull in Brand & Units Sold from asin_df
        brand_map = asin_df.set_index(
            asin_df["ASIN"].str.upper()
        )["Brands"].to_dict()
        qty_map = asin_df.set_index(
            asin_df["ASIN"].str.upper()
        )["quantity"].to_dict()

        fees_df["Brand"] = fees_df["ASIN"].str.upper().map(brand_map).fillna("Unknown")
        fees_df["Units Sold"] = (
            fees_df["ASIN"]
              .str.upper()
              .map(qty_map)
              .fillna(0)
              .astype(int)
        )

        # 4) compute numeric delta, ROUND it to 2 decimals, then multiply
        fees_df["Delta_num"] = (
            fees_df["Actual Fees per Unit"]
            - fees_df["Expected Fees per Unit"]
        ).round(2)
        fees_df["LostRev_num"] = (
            fees_df["Delta_num"] * fees_df["Units Sold"]
        ).round(2)

        # 5) build final report and 1‑based index
        report = fees_df[[
            "Brand",
            "ASIN",
            "Actual Fees per Unit",
            "Expected Fees per Unit",
            "Delta_num",
            "Units Sold",
            "LostRev_num"
        ]].copy()
        report.index = range(1, len(report) + 1)
        report.index.name = "No."

        # 6) format back to dollars and rename
        report = report.rename(columns={
            "Actual Fees per Unit":   "Actual Fees",
            "Expected Fees per Unit": "Planned Fees",
            "Delta_num":              "Delta",
            "LostRev_num":            "Total Lost Revenue"
        })
        for col in (
            "Actual Fees",
            "Planned Fees",
            "Delta",
            "Total Lost Revenue"
        ):
            report[col] = report[col].map("${:,.2f}".format)

        # 7) Create formatted string
        formatted_string = f"Here are all ASINs where the total fees were higher than planned for the settlement period {settlement_period}:"

        # 8) Save for UI
        st.session_state["asins_fees_higher_string"] = formatted_string
        st.session_state["asins_fees_higher_table"] = report
        st.session_state["asins_fees_higher_period"] = settlement_period
        return "ok"
        
    except Exception as e:
        st.error(f"Error processing the request for ASINs with fees higher than plan: {str(e)}")
        return "error"


def tool_orders_fees_higher_plan(prompt: str) -> str:
    """
    Show orders where fees were higher than plan for ASIN XYZ.
    Examples: 'Show orders where fees were higher than plan for ASIN B0XXXXXXXX'.
    Saves formatted string and table in session_state['orders_fees_higher_string'] and 'orders_fees_higher_table'].
    """
    try:
        # Extract ASIN from prompt
        asin_match = re.search(r'\b(B0[0-9A-Z]{8})\b', prompt.upper())
        if not asin_match:
            st.session_state["agent_error"] = "No valid ASIN found in the prompt. Please provide an ASIN in B0XXXXXXXX format."
            return "error"
        
        asin_q = asin_match.group(1)
        
        # "did you mean?" fallback
        all_asins = asin_df["ASIN"].astype(str).str.upper().tolist()
        if asin_q not in all_asins:
            import difflib
            suggestion = difflib.get_close_matches(asin_q, all_asins, n=1, cutoff=0.6)
            if suggestion:
                sug = suggestion[0]
                st.session_state["agent_error"] = f"I could not find ASIN {asin_q}, did you mean ASIN {sug}?"
                return "error"
            else:
                st.session_state["agent_error"] = f"I could not find ASIN {asin_q}."
                return "error"

        # 1) Build a clean, de-duplicated SKU→ASIN map (avoid row-multiplication on merge)
        sku_asin_map = (
            business_df[["SKU", "(Child) ASIN"]]
            .rename(columns={"(Child) ASIN": "ASIN"})
            .assign(
                SKU=lambda d: d["SKU"].astype(str).str.upper().str.strip(),
                ASIN=lambda d: d["ASIN"].astype(str).str.upper().str.strip(),
            )
            .drop_duplicates(subset="SKU", keep="first")
        )

        # 2) Normalize order_df & merge ASIN
        df = (
            order_df.assign(
                SKU=lambda d: d["sku"].astype(str).str.upper().str.strip(),
                type=lambda d: d["type"].astype(str).str.lower().str.strip(),
            )
            .merge(sku_asin_map, on="SKU", how="left")
        )

        # 3) Filter to "order" rows for this ASIN
        df = df[(df["type"] == "order") & (df["ASIN"] == asin_q)]
        if df.empty:
            st.session_state["agent_error"] = f"No orders for ASIN {asin_q} in the settlement period {settlement_period}."
            return "error"

        # 4) Planned fees per unit (ensure numeric)
        df = df.merge(
            unit_df[["ASIN", "Referral Fee", "TOTAL FBA Fulfillment Fee"]],
            on="ASIN",
            how="left",
        ).rename(
            columns={
                "Referral Fee": "Plan Referral Fee",
                "TOTAL FBA Fulfillment Fee": "Plan FBA Fee",
            }
        )
        for c in ("Plan Referral Fee", "Plan FBA Fee"):
            # strip $ and commas if present
            df[c] = (
                df[c]
                .astype(str)
                .str.replace(r"[^\d.\-]", "", regex=True)
                .replace("", float("nan"))
                .astype(float)
            ).fillna(0.0)

        # 5) Make actual fee components positive; ensure quantity numeric
        df["Quantity"] = pd.to_numeric(df["quantity"], errors="coerce").fillna(0).astype(int)
        # selling/fba fees are usually negative in settlements; flip them positive
        df["Actual Referral Fee"] = (-pd.to_numeric(df["selling fees"], errors="coerce")).clip(lower=0)
        df["Actual FBA Fee"] = (-pd.to_numeric(df["fba fees"], errors="coerce")).clip(lower=0)

        # 6) COLLAPSE DUPLICATES correctly:
        #    Group by order-level keys (drop date/time so the same order doesn't fragment),
        #    use Quantity=max to avoid inflating units when fee rows repeat the same quantity.
        key_cols = ["order id", "SKU", "ASIN"]
        date_col_name = "date/time" if "date/time" in df.columns else None

        agg_map = {
            "Quantity": "max",                  # prevents double-counting repeated rows
            "Actual Referral Fee": "sum",       # fees add up
            "Actual FBA Fee": "sum",
            "Plan Referral Fee": "max",         # per-unit constant for the ASIN
            "Plan FBA Fee": "max",
        }
        if date_col_name:
            agg_map[date_col_name] = "min"      # earliest timestamp for display

        grouped = (
            df.groupby(key_cols, as_index=False)
            .agg(agg_map)
            .rename(columns={"order id": "Order ID"})
        )

        # Standardize Date/Time column name if present
        if date_col_name and date_col_name in grouped.columns:
            grouped = grouped.rename(columns={date_col_name: "Date/Time"})
        else:
            grouped["Date/Time"] = pd.NaT  # keep column for a consistent output shape

        # 7) Compute totals & deltas per grouped order line
        grouped = grouped[grouped["Quantity"] > 0]  # avoid div/0
        if grouped.empty:
            st.session_state["agent_error"] = f"No valid order lines (non-zero quantity) for ASIN {asin_q}."
            return "error"

        grouped["Total Fees"] = grouped["Actual Referral Fee"] + grouped["Actual FBA Fee"]
        grouped["Avg Fees per Unit"] = grouped["Total Fees"] / grouped["Quantity"]
        grouped["Expected Fees per Unit"] = grouped["Plan Referral Fee"] + grouped["Plan FBA Fee"]
        grouped["Delta"] = (grouped["Avg Fees per Unit"] - grouped["Expected Fees per Unit"]) * grouped["Quantity"]

        # 8) Keep only rows where Delta > 0 (fees above plan)
        grouped = grouped[grouped["Delta"] > 0]
        if grouped.empty:
            st.session_state["agent_error"] = f"No orders for ASIN {asin_q} had fees above plan."
            return "error"

        # 9) Lookup Brand (uppercase)
        brand_map = asin_df.set_index(asin_df["ASIN"].str.upper())["Brands"].to_dict()
        grouped["Brand"] = grouped["ASIN"].map(brand_map).fillna("UNKNOWN").str.upper()

        # 10) Select, format & index
        out = grouped[[
            "Brand", "ASIN", "SKU", "Order ID", "Date/Time", "Quantity",
            "Total Fees", "Avg Fees per Unit", "Expected Fees per Unit", "Delta"
        ]].copy()

        for col in ["Total Fees", "Avg Fees per Unit", "Expected Fees per Unit", "Delta"]:
            out[col] = out[col].map("${:,.2f}".format)
        out["Quantity"] = out["Quantity"].map("{:,}".format)

        out = out.reset_index(drop=True)
        out.index = range(1, len(out) + 1)
        out.index.name = "No."

        # Create formatted string
        formatted_string = f"Here are all orders where the fees were higher than planned for ASIN {asin_q} for the settlement period {settlement_period}:"

        # Save for UI
        st.session_state["orders_fees_higher_string"] = formatted_string
        st.session_state["orders_fees_higher_table"] = out
        st.session_state["orders_fees_higher_period"] = settlement_period
        return "ok"
        
    except Exception as e:
        st.session_state["agent_error"] = f"Error processing the request for orders with fees higher than plan: {str(e)}"
        return "error"


def tool_orders_referral_fees_higher_plan(prompt: str) -> str:
    """
    Show orders where referral fees were higher than plan for ASIN XYZ.
    Examples: 'Show orders where referral fees were higher than plan for ASIN B0XXXXXXXX'.
    Saves formatted string and table in session_state['orders_referral_fees_higher_string'] and 'orders_referral_fees_higher_table'].
    """
    try:
        # Extract ASIN from prompt
        asin_match = re.search(r'\b(B0[0-9A-Z]{8})\b', prompt.upper())
        if not asin_match:
            st.session_state["agent_error"] = "No valid ASIN found in the prompt. Please provide an ASIN in B0XXXXXXXX format."
            return "error"
        
        asin_q = asin_match.group(1)
        
        # "did you mean?" fallback
        all_asins = asin_df["ASIN"].astype(str).str.upper().tolist()
        if asin_q not in all_asins:
            import difflib
            suggestion = difflib.get_close_matches(asin_q, all_asins, n=1, cutoff=0.6)
            if suggestion:
                sug = suggestion[0]
                st.session_state["agent_error"] = f"I could not find ASIN {asin_q}, did you mean ASIN {sug}?"
                return "error"
            else:
                st.session_state["agent_error"] = f"I could not find ASIN {asin_q}."
                return "error"

        # 1) Build a clean, de-duplicated SKU→ASIN map (avoid row-multiplication on merge)
        sku_asin_map = (
            business_df[["SKU", "(Child) ASIN"]]
            .rename(columns={"(Child) ASIN": "ASIN"})
            .assign(
                SKU=lambda d: d["SKU"].astype(str).str.upper().str.strip(),
                ASIN=lambda d: d["ASIN"].astype(str).str.upper().str.strip(),
            )
            .drop_duplicates(subset="SKU", keep="first")
        )

        # 2) Normalize order_df & merge ASIN
        df = (
            order_df
            .assign(
                SKU=lambda d: d["sku"].astype(str).str.upper().str.strip(),
                type=lambda d: d["type"].astype(str).str.lower().str.strip()
            )
            .merge(sku_asin_map, on="SKU", how="left")
        )

        # 3) Filter to this ASIN's order lines
        df = df[(df["type"] == "order") & (df["ASIN"] == asin_q)]
        if df.empty:
            st.session_state["agent_error"] = f"No orders for ASIN {asin_q} in the settlement period {settlement_period}."
            return "error"

        # 4) Parse numerics BEFORE grouping
        df["quantity_num"] = pd.to_numeric(df["quantity"], errors="coerce").fillna(0)
        df["selling_fees_num"] = pd.to_numeric(df["selling fees"], errors="coerce").fillna(0.0)

        # 5) Stable quantity aggregator
        def _stable_qty(s: pd.Series) -> float:
            pos = s[s > 0]
            if pos.empty:
                return 0.0
            uniq = pd.unique(pos)
            # If dataset repeats the same quantity on multiple rows → take that one value
            if len(uniq) == 1:
                return float(uniq[0])
            # If dataset splits quantity across rows (e.g., 1 + 1) → sum
            return float(pos.sum())

        # 6) Collapse duplicates on order-level keys (drop date/time from grouping)
        group_keys = ["order id", "SKU", "ASIN"]
        if "order item id" in df.columns:
            group_keys.insert(1, "order item id")  # increases stability

        agg_map = {
            "quantity_num": _stable_qty,
            "selling_fees_num": "sum",
        }
        if "date/time" in df.columns:
            agg_map["date/time"] = "min"  # keep earliest timestamp for display

        g = (
            df.groupby(group_keys, as_index=False)
              .agg(agg_map)
              .rename(columns={
                  "order id": "Order ID",
                  "date/time": "Date/Time",
                  "quantity_num": "Quantity",
                  "selling_fees_num": "Selling Fees Sum",
              })
        )

        # 7) Flip referral fees positive and filter
        g = g[g["Quantity"] > 0]
        if g.empty:
            st.session_state["agent_error"] = "No valid (non-zero quantity) orders after de-duplication."
            return "error"

        g["Referral Fees"] = (-g["Selling Fees Sum"]).clip(lower=0.0)

        # 8) Plan referral fee (1 row per ASIN; choose a single numeric value)
        unit_plan = (
            unit_df[["ASIN", "Referral Fee"]].copy()
            .assign(
                ASIN=lambda d: d["ASIN"].astype(str).str.upper().str.strip(),
                **{"Referral Fee": lambda d: pd.to_numeric(
                    d["Referral Fee"].astype(str).str.replace(r"[^\d.\-]", "", regex=True),
                    errors="coerce"
                )}
            )
            .dropna(subset=["Referral Fee"])
            .drop_duplicates(subset="ASIN", keep="last")
        )
        plan_ser = unit_plan.loc[unit_plan["ASIN"] == asin_q, "Referral Fee"]
        if plan_ser.empty:
            st.session_state["agent_error"] = f"No planned referral fee found for ASIN {asin_q}."
            return "error"
        plan_ref = float(plan_ser.max())

        # 9) Per-unit and delta
        g["Avg Fees per Unit"]      = g["Referral Fees"] / g["Quantity"]
        g["Expected Fees per Unit"] = plan_ref
        g["Delta"]                  = g["Avg Fees per Unit"] - g["Expected Fees per Unit"]

        g = g[g["Delta"] > 0]
        if g.empty:
            st.session_state["agent_error"] = f"No orders for ASIN {asin_q} had avg. referral fees above plan (${plan_ref:,.2f})."
            return "error"

        # 10) Brand lookup
        brand_map = asin_df.set_index(asin_df["ASIN"].str.upper())["Brands"].to_dict()
        g["Brand"] = g["ASIN"].map(brand_map).fillna("UNKNOWN").str.upper()

        # 11) Format & render
        if "Date/Time" not in g.columns:
            g["Date/Time"] = pd.NaT

        g["Quantity"]               = g["Quantity"].map(lambda x: f"{int(x):,}")
        g["Referral Fees"]          = g["Referral Fees"].map("${:,.2f}".format)
        g["Avg Fees per Unit"]      = g["Avg Fees per Unit"].map("${:,.2f}".format)
        g["Expected Fees per Unit"] = g["Expected Fees per Unit"].map("${:,.2f}".format)
        g["Delta"]                  = g["Delta"].map("${:,.2f}".format)

        out = g[[
            "Brand", "ASIN", "SKU", "Order ID", "Date/Time", "Quantity",
            "Referral Fees", "Avg Fees per Unit", "Expected Fees per Unit", "Delta"
        ]].reset_index(drop=True)
        out.index = range(1, len(out) + 1)
        out.index.name = "No."

        # Create formatted string
        formatted_string = f"Here are all orders where the referral fees were higher than planned for ASIN {asin_q} for the settlement period {settlement_period}:"

        # Save for UI
        st.session_state["orders_referral_fees_higher_string"] = formatted_string
        st.session_state["orders_referral_fees_higher_table"] = out
        st.session_state["orders_referral_fees_higher_period"] = settlement_period
        return "ok"
        
    except Exception as e:
        st.session_state["agent_error"] = f"Error processing the request for orders with referral fees higher than plan: {str(e)}"
        return "error"


def tool_orders_fulfillment_fees_higher_plan(user_text: str) -> str:
    """
    Show orders where fulfillment fees were higher than plan for ASIN XYZ.
    Examples: 'Show orders where fulfillment fees were higher than plan for ASIN B0XXXXXXXX'.
    Saves formatted string and table in session_state['orders_fulfillment_fees_higher_string'] and 'orders_fulfillment_fees_higher_table'].
    """
    try:
        # 1) Extract ASIN from prompt
        m_fba_orders = re.search(
            r"\bshow\s+orders\s+where\s+fulfillment\s+fees\s+were\s+higher\s+than\s+plan\s+for\s+asin\s+(B0[0-9A-Z]{8})\b",
            user_text,
            flags=re.IGNORECASE
        )
        if not m_fba_orders:
            st.session_state["agent_error"] = "Could not extract ASIN from the prompt. Please provide an ASIN in the format B0XXXXXXXX."
            return "error"
        
        asin_q = m_fba_orders.group(1).upper()
        
        # 2) "Did you mean?" fallback for mistyped ASINs
        all_asins = asin_df["ASIN"].astype(str).str.upper().unique()
        if asin_q not in all_asins:
            matches = difflib.get_close_matches(asin_q, all_asins, n=3, cutoff=0.6)
            if matches:
                st.session_state["agent_error"] = f"ASIN {asin_q} not found. Did you mean: {', '.join(matches)}?"
                return "error"
            else:
                st.session_state["agent_error"] = f"ASIN {asin_q} not found in the data."
                return "error"
        
        # 3) Build SKU → ASIN map
        sku_asin_map = (
            business_df[["SKU", "(Child) ASIN"]]
            .rename(columns={"(Child) ASIN": "ASIN"})
            .assign(
                SKU=lambda d: d["SKU"].astype(str).str.upper().str.strip(),
                ASIN=lambda d: d["ASIN"].astype(str).str.upper().str.strip()
            )
            .drop_duplicates("SKU")
        )
        
        # 4) Merge ASIN into orders
        df = (
            order_df
            .assign(
                SKU=lambda d: d["sku"].astype(str).str.upper().str.strip(),
                type=lambda d: d["type"].astype(str).str.lower().str.strip()
            )
            .merge(sku_asin_map, on="SKU", how="left")
        )
        
        # 5) Filter to "order" rows for this ASIN
        df = df[(df["type"] == "order") & (df["ASIN"] == asin_q)]
        if df.empty:
            st.session_state["agent_error"] = f"No orders for ASIN {asin_q} in the settlement period {settlement_period}."
            return "error"
        
        # 6) Planned FBA fee per unit
        plan_ser = (
            unit_df.loc[
                unit_df["ASIN"].astype(str).str.upper() == asin_q,
                "TOTAL FBA Fulfillment Fee"
            ]
            .astype(str)
            .str.replace(r"[\$,]", "", regex=True)
            .dropna()
        )
        if plan_ser.empty:
            st.session_state["agent_error"] = f"No planned FBA fee found for ASIN {asin_q}."
            return "error"
        plan_fba = float(plan_ser.iloc[0])
        
        # 7) Actual fulfillment fees & quantity
        df["Fulfillment Fees"] = -df["fba fees"].clip(upper=0)
        df["Quantity"] = pd.to_numeric(df["quantity"], errors="coerce").fillna(0).astype(int)
        
        # 8) Compute average fees per unit
        df["Avg Fees per Unit"] = df["Fulfillment Fees"] / df["Quantity"]
        
        # 9) Filter orders above plan
        df = df[df["Avg Fees per Unit"] > plan_fba]
        if df.empty:
            st.session_state["agent_error"] = f"No orders for ASIN {asin_q} had fulfillment fees above plan (${plan_fba:,.2f})."
            return "error"
        
        # 10) Compute delta and expected fees
        df["Expected Fees per Unit"] = plan_fba
        df["Delta"] = df["Avg Fees per Unit"] - df["Expected Fees per Unit"]
        
        # 11) Lookup Brand
        brand_map = asin_df.set_index(asin_df["ASIN"].str.upper())["Brands"].to_dict()
        df["Brand"] = df["ASIN"].map(brand_map).fillna("UNKNOWN").str.upper()
        
        # 12) Select, rename & format
        out = df[[
            "Brand", "ASIN", "SKU", "order id", "date/time", "Quantity",
            "Fulfillment Fees", "Avg Fees per Unit", "Expected Fees per Unit", "Delta"
        ]].rename(columns={
            "order id": "Order ID",
            "date/time": "Date/Time"
        })
        
        # Format monetary columns
        for c in ["Fulfillment Fees", "Avg Fees per Unit", "Expected Fees per Unit", "Delta"]:
            out[c] = out[c].map("${:,.2f}".format)
        out["Quantity"] = out["Quantity"].map("{:,}".format)
        
        # 13) Re-index
        out = out.reset_index(drop=True)
        out.index = range(1, len(out) + 1)
        out.index.name = "No."
        
        # 14) Create formatted string
        formatted_string = f"Here are all orders where the fulfillment fees were higher than planned for ASIN {asin_q} for the settlement period {settlement_period}:"
        
        # 15) Save for UI
        st.session_state["orders_fulfillment_fees_higher_string"] = formatted_string
        st.session_state["orders_fulfillment_fees_higher_table"] = out
        st.session_state["orders_fulfillment_fees_higher_period"] = settlement_period
        
        return "ok"
        
    except Exception as e:
        st.session_state["agent_error"] = f"Error processing the request for orders with fulfillment fees higher than plan: {str(e)}"
        return "error"


def tool_settlement_period_definition(prompt: str) -> str:
    """Provide definition of Amazon settlement period."""
    try:
        # Create the formatted definition string
        definition_string = "An Amazon settlement period is the 14-day timeframe in which Amazon tracks and calculates all financial activity, including sales, fees, and refunds, for a seller's account."
        
        # Save for UI
        st.session_state["settlement_period_definition"] = definition_string
        
        return "ok"
        
    except Exception as e:
        st.session_state["agent_error"] = f"Error processing the settlement period definition request: {str(e)}"
        return "error"


def tool_settlement_period_timing(prompt: str) -> str:
    """Provide timing information about settlement period."""
    try:
        # Create the formatted timing string
        timing_string = f"The settlement period is every 14 days, with the most recent timeframe of {settlement_period}."
        
        # Save for UI
        st.session_state["settlement_period_timing"] = timing_string
        
        return "ok"
        
    except Exception as e:
        st.session_state["agent_error"] = f"Error processing the settlement period timing request: {str(e)}"
        return "error"


def tool_settlement_period_reason(prompt: str) -> str:
    """Provide explanation for why settlement period has that specific date range."""
    try:
        # Create the formatted explanation string
        explanation_string = "Amazon typically starts the first settlement period at the exact time your store was created (Pacific time) and it then resets every 14 days."
        
        # Save for UI
        st.session_state["settlement_period_reason"] = explanation_string
        
        return "ok"
        
    except Exception as e:
        st.session_state["agent_error"] = f"Error processing the settlement period reason request: {str(e)}"
        return "error"


# ──────────────────────────────────────────────────────────────────────────────
# 4) LANGCHAIN AGENT
# ──────────────────────────────────────────────────────────────────────────────
from langchain_openai import ChatOpenAI
from langchain.agents import Tool, create_tool_calling_agent, AgentExecutor
from langchain.prompts import ChatPromptTemplate

def build_agent() -> AgentExecutor:
    tools = [
        Tool(
            name="conversion_rates_for_asins",
            func=tool_conversion_rates_for_asins,
            description=(
                "Use when the user asks to 'Show the Conversion Rates for my ASINs'. "
                "Input: the full user prompt. Return 'ok' if table prepared."
            ),
        ),
        Tool(
            name="average_conversion_rate",
            func=tool_average_conversion_rate,
            description=(
                "Use when the user asks for an average conversion rate (e.g., 'Show my average Conversion Rate ...'). "
                "Input: the full user prompt. Return 'ok' if value prepared."
            ),
        ),
        Tool(
            name="asins_avg_price_below_plan",
            func=tool_asins_avg_price_below_plan,
            description=(
                "Use ONLY when user asks for ASINs with AVERAGE sales price below plan (aggregated summary). "
                "Synonyms for 'average sales price': average order value, AOV. "
                "Synonyms for 'plan': contracted price, expected, budgeted, reach floor price, iMAP, MAP, MSRP. "
                "Examples: 'Show ASINs with average sales price below plan', 'Which ASINs have average price below plan', "
                "'Show ASINs with AOV below MAP', 'Show ASINs with mean below contracted price', "
                "'Show ASINs with average order value below expected', 'Show ASINs with AOV below MSRP'. "
                "This shows a summary table of ASINs, NOT individual orders. "
                "Input: full user prompt. Returns 'ok' if a table was prepared."
            ),
        ),
        Tool(
            name="asins_where_avg_price_lower_than_plan",
            func=tool_asins_where_avg_price_lower_than_plan,
            description=(
                "Use ONLY when user asks for ASINs where the AVERAGE sales price was lower than plan (aggregated summary). "
                "Synonyms for 'average sales price': mean, average order value, AOV. "
                "Synonyms for 'plan': contracted price, expected, budgeted, reach floor price, iMAP, MAP, MSRP. "
                "Examples: 'Show ASINs where the average sales price was lower than plan', "
                "'Show ASINs where AOV was lower than MAP', 'Show ASINs where mean was lower than contracted price'. "
                "This shows a summary table of ASINs, NOT individual orders. "
                "Same result as asins_avg_price_below_plan; different phrasing."
            ),
        ),
        Tool(
            name="orders_below_plan_for_asin",
            func=tool_orders_below_plan_for_asin,
            description=(
                "PRIORITY: Use this tool when user asks for INDIVIDUAL ORDERS where sales price was lower than plan for a SPECIFIC ASIN. "
                "Examples: 'Show orders where the sales price was lower than plan for ASIN B0XXXXXXXX', "
                "'Show individual orders below plan for ASIN B0XXXXXXXX'. "
                "The prompt MUST include a specific ASIN (B0XXXXXXXX format). "
                "This shows individual order details, NOT aggregated ASIN summaries. "
                "Returns 'ok' if a table was prepared."
            ),
        ),
        Tool(
            name="gross_sales_by_asin",
            func=tool_gross_sales_by_asin,
            description=(
                "Use when user asks for gross sales by ASIN for a time period. "
                "Examples: 'Show gross sales by ASIN for last week', 'Show the gross sales by ASIN for 14 days'. "
                "The prompt MUST include a time period (last week, 14 days, last month, etc.). "
                "Returns 'ok' if a table was prepared."
            ),
        ),
        Tool(
            name="gross_sales_total",
            func=tool_gross_sales_total,
            description=(
                "Use when user asks for total gross sales (not by ASIN). "
                "Examples: 'Show the gross sales for last week'. "
                "This shows the total sum, not broken down by ASIN. "
                "Returns 'ok' if value was prepared."
            ),
        ),
        Tool(
            name="net_sales_by_asin",
            func=tool_net_sales_by_asin,
            description=(
                "Use when user asks for net sales by ASIN. "
                "Examples: 'Show net sales by ASIN for last week', 'Show the net sales by ASIN'. "
                "Net sales are from the settlement period (Amazon Top-line Sales/ATS). "
                "Returns 'ok' if a table was prepared."
            ),
        ),
        Tool(
            name="gross_sales_for_specific_asin",
            func=tool_gross_sales_for_specific_asin,
            description=(
                "Use when user asks for gross sales for a SPECIFIC ASIN. "
                "Examples: 'Show gross sales for ASIN B0XXXXXXXX for last week'. "
                "The prompt MUST include both an ASIN (B0XXXXXXXX) and a time period. "
                "Returns 'ok' if value was prepared."
            ),
        ),
        Tool(
            name="buy_box_percentages_for_asins",
            func=tool_buy_box_percentages_for_asins,
            description=(
                "Use when the user asks to 'Show the Buy Box percentages for my ASINs'. "
                "Input: the full user prompt. Return 'ok' if table prepared."
            ),
        ),
        Tool(
            name="average_buy_box_percentage",
            func=tool_average_buy_box_percentage,
            description=(
                "Use when the user asks for an average Buy Box percentage (e.g., 'Show my average Buy Box percentage for last week'). "
                "Input: the full user prompt. Return 'ok' if value prepared."
            ),
        ),
        Tool(
            name="sessions_for_asins",
            func=tool_sessions_for_asins,
            description=(
                "Use when the user asks for total number of Sessions for ASINs for a time period (e.g., 'Show the total number of Sessions for my ASINs last week'). "
                "Input: the full user prompt. Return 'ok' if table prepared."
            ),
        ),
        Tool(
            name="average_order_value",
            func=tool_average_order_value,
            description=(
                "Use when the user asks for Average Order Value for ASINs for a time period (e.g., 'Show the Average Order Value for my ASINs last week'). "
                "Input: the full user prompt. Return 'ok' if table prepared."
            ),
        ),
        Tool(
            name="suppressed_asins",
            func=tool_suppressed_asins,
            description=(
                "Use when the user asks for ASINs suppressed (Buy Box % ≤ 80%) for a time period (e.g., 'Show the ASINs suppressed last week'). "
                "Input: the full user prompt. Return 'ok' if table prepared."
            ),
        ),
        Tool(
            name="buy_box_percentages_for_asins",
            func=tool_buy_box_percentages_for_asins,
            description=(
                "Use when the user asks to 'Show the Buy Box percentages for my ASINs'. "
                "Input: the full user prompt. Return 'ok' if table prepared."
            ),
        ),
        Tool(
            name="average_buy_box_percentage",
            func=tool_average_buy_box_percentage,
            description=(
                "Use when the user asks for an average Buy Box percentage (e.g., 'Show my average Buy Box percentage for last week'). "
                "Input: the full user prompt. Return 'ok' if value prepared."
            ),
        ),
        Tool(
            name="sessions_for_asins",
            func=tool_sessions_for_asins,
            description=(
                "Use when the user asks for total number of Sessions for ASINs for a time period (e.g., 'Show the total number of Sessions for my ASINs last week'). "
                "Input: the full user prompt. Return 'ok' if table prepared."
            ),
        ),
        Tool(
            name="average_order_value",
            func=tool_average_order_value,
            description=(
                "Use when the user asks for Average Order Value for ASINs for a time period (e.g., 'Show the Average Order Value for my ASINs last week'). "
                "Input: the full user prompt. Return 'ok' if table prepared."
            ),
        ),
        Tool(
            name="suppressed_asins",
            func=tool_suppressed_asins,
            description=(
                "Use when the user asks for ASINs suppressed (Buy Box % ≤ 80%) for a time period (e.g., 'Show the ASINs suppressed last week'). "
                "Input: the full user prompt. Return 'ok' if table prepared."
            ),
        ),
        Tool(
            name="currently_suppressed_asins",
            func=tool_currently_suppressed_asins,
            description=(
                "Use when the user asks for ASINs currently suppressed (0% Buy Box). "
                "Input: the full user prompt. Return 'ok' if table prepared."
            ),
        ),
        Tool(
            name="sales_lost_to_other_sellers",
            func=tool_sales_lost_to_other_sellers,
            description=(
                "Use when the user asks for total sales lost to other sellers for a time period (e.g., 'Show the total sales lost to other sellers last week'). "
                "Input: the full user prompt. Return 'ok' if table prepared."
            ),
        ),
        Tool(
            name="profit_lost_to_other_sellers",
            func=tool_profit_lost_to_other_sellers,
            description=(
                "Use when the user asks for total gross profit lost to other sellers for a time period (e.g., 'Show the total gross profit lost to other sellers last week'). "
                "Input: the full user prompt. Return 'ok' if table prepared."
            ),
        ),
        Tool(
            name="gross_margin_underperformers",
            func=tool_gross_margin_underperformers,
            description=(
                "Use when the user asks for ASINs with gross margin less than plan. "
                "Synonyms for 'gross margin': RGP, GP. "
                "Synonyms for 'plan': contracted, budgeted, X% (where X is a percentage). "
                "Examples: 'Show ASINs with gross margin less than plan', 'Show ASINs with RGP less than 20%', "
                "'Show ASINs with GP less than contracted', 'Show ASINs with gross margin less than budgeted'. "
                "Input: the full user prompt. Return 'ok' if table prepared."
            ),
        ),
        Tool(
            name="gross_profit_all_asins",
            func=tool_gross_profit_for_all_asins,
            description=(
                "Use when the user asks for gross profit for all ASINs (individual products). "
                "Synonyms for 'gross profit': RGP, GP, profit, net profit. "
                "Keywords: 'gross profit', 'profit dollars', 'profit amount', 'ASINs', 'products', 'individual items'. "
                "Examples: 'Show gross profit for all ASINs', 'Show RGP for all ASINs', 'Show GP for all ASINs', "
                "'Show profit for all ASINs', 'Show net profit for all ASINs'. "
                "This is about profit DOLLAR AMOUNTS, not percentages. "
                "Input: the full user prompt. Return 'ok' if value prepared."
            ),
        ),
        Tool(
            name="gross_profit_all_brands",
            func=tool_gross_profit_for_all_brands,
            description=(
                "Use when the user asks for gross profit for all brands (company names). "
                "Synonyms for 'gross profit': RGP, GP, profit, net profit. "
                "Keywords: 'gross profit', 'profit dollars', 'profit amount', 'brands', 'companies', 'brand names'. "
                "Examples: 'Show gross profit for all brands', 'Show RGP for all brands', 'Show GP for all brands', "
                "'Show profit for all brands', 'Show net profit for all brands'. "
                "This is about profit DOLLAR AMOUNTS, not percentages. "
                "Input: the full user prompt. Return 'ok' if value prepared."
            ),
        ),
        Tool(
            name="gross_margin_all_asins",
            func=tool_gross_margin_for_all_asins,
            description=(
                "Use when the user asks for gross margin for all ASINs (individual products). "
                "Synonyms for 'gross margin': margin %. "
                "Keywords: 'gross margin', 'margin percentage', 'margin %', 'ASINs', 'products', 'individual items'. "
                "Examples: 'Show gross margin for all ASINs', 'Show margin % for all ASINs'. "
                "This is about margin PERCENTAGE, not dollar amounts. "
                "Input: the full user prompt. Return 'ok' if value prepared."
            ),
        ),
        Tool(
            name="gross_margin_all_brands",
            func=tool_gross_margin_for_all_brands,
            description=(
                "Use when the user asks for gross margin for all brands (company names). "
                "Synonyms for 'gross margin': margin %. "
                "Keywords: 'gross margin', 'margin percentage', 'margin %', 'brands', 'companies', 'brand names'. "
                "Examples: 'Show gross margin for all brands', 'Show margin % for all brands'. "
                "This is about margin PERCENTAGE, not dollar amounts. "
                "Input: the full user prompt. Return 'ok' if value prepared."
            ),
        ),
        Tool(
            name="sales_descending_order",
            func=tool_sales_descending_order,
            description=(
                "Use when the user asks for ASINs sales in descending order (ranked list). "
                "Synonyms for 'sales': ATS, net sales, product sales, top line sales. "
                "Examples: 'Show ASINs sales in descending order', 'Show ASINs ATS in descending order', "
                "'Show ASINs net sales in descending order', 'Show ASINs product sales in descending order', "
                "'Show ASINs top line sales in descending order'. "
                "Keywords: 'descending order', 'ranked', 'sorted', 'list'. "
                "Input: the full user prompt. Return 'ok' if table prepared."
            ),
        ),
        Tool(
            name="gross_profit_descending_order",
            func=tool_gross_profit_descending_order,
            description=(
                "Use when the user asks for ASINs gross profit in descending order (ranked list). "
                "Synonyms for 'gross profit': RGP, GP, profit, net profit. "
                "Examples: 'Show ASINs gross profit in descending order', 'Show ASINs RGP in descending order', "
                "'Show ASINs GP in descending order', 'Show ASINs profit in descending order', "
                "'Show ASINs net profit in descending order'. "
                "Keywords: 'descending order', 'ranked', 'sorted', 'list'. "
                "Input: the full user prompt. Return 'ok' if table prepared."
            ),
        ),
        Tool(
            name="gross_margin_descending_order",
            func=tool_gross_margin_descending_order,
            description=(
                "Use when the user asks for ASINs gross margin in descending order (ranked list). "
                "Synonyms for 'gross margin': margin %. "
                "Examples: 'Show ASINs gross margin in descending order', 'Show ASINs margin % in descending order'. "
                "Keywords: 'descending order', 'ranked', 'sorted', 'list'. "
                "Input: the full user prompt. Return 'ok' if table prepared."
            ),
        ),
        Tool(
            name="brands_sales_descending_order",
            func=tool_brands_sales_descending_order,
            description=(
                "Use when the user asks for brands sales in descending order (ranked list). "
                "Synonyms for 'sales': ATS, net sales, product sales, top line sales. "
                "Examples: 'Show brands sales in descending order', 'Show brands ATS in descending order', "
                "'Show brands net sales in descending order', 'Show brands product sales in descending order', "
                "'Show brands top line sales in descending order'. "
                "Keywords: 'descending order', 'ranked', 'sorted', 'list'. "
                "Input: the full user prompt. Return 'ok' if table prepared."
            ),
        ),
        Tool(
            name="brands_gross_profit_descending_order",
            func=tool_brands_gross_profit_descending_order,
            description=(
                "Use when the user asks for brands gross profit in descending order (ranked list). "
                "Synonyms for 'gross profit': RGP, GP, profit, net profit. "
                "Examples: 'Show brands gross profit in descending order', 'Show brands RGP in descending order', "
                "'Show brands GP in descending order', 'Show brands profit in descending order', "
                "'Show brands net profit in descending order'. "
                "Keywords: 'descending order', 'ranked', 'sorted', 'list'. "
                "Input: the full user prompt. Return 'ok' if table prepared."
            ),
        ),
        Tool(
            name="brands_gross_margin_descending_order",
            func=tool_brands_gross_margin_descending_order,
            description=(
                "Use when the user asks for brands gross margin in descending order (ranked list). "
                "Synonyms for 'gross margin': margin %. "
                "Examples: 'Show brands gross margin in descending order', 'Show brands margin % in descending order'. "
                "Keywords: 'descending order', 'ranked', 'sorted', 'list'. "
                "Input: the full user prompt. Return 'ok' if table prepared."
            ),
        ),
        Tool(
            name="top_brands_sales",
            func=tool_top_brands_sales,
            description=(
                "Use when the user asks for top X brands sales in descending order. "
                "Synonyms for 'sales': ATS, net sales, product sales, top line sales. "
                "Examples: 'Show top 5 brands sales in descending order', 'Show top 10 brands ATS in descending order', "
                "'Show top 3 brands net sales in descending order', 'Show top 7 brands product sales in descending order', "
                "'Show top 15 brands top line sales in descending order'. "
                "Input: the full user prompt. Return 'ok' if table prepared."
            ),
        ),
        Tool(
            name="top_asins_sales",
            func=tool_top_asins_sales,
            description=(
                "Use when the user asks for top X ASINs sales in descending order. "
                "Synonyms for 'sales': ATS, net sales, product sales, top line sales. "
                "Examples: 'Show top 5 ASINs sales in descending order', 'Show top 10 ASINs ATS in descending order', "
                "'Show top 3 ASINs net sales in descending order', 'Show top 7 ASINs product sales in descending order', "
                "'Show top 15 ASINs top line sales in descending order'. "
                "Input: the full user prompt. Return 'ok' if table prepared."
            ),
        ),
        Tool(
            name="fees_higher_than_plan",
            func=tool_fees_higher_than_plan,
            description=(
                "Use when the user asks for ASINs with average total fees higher than plan. "
                "Input: the full user prompt. Return 'ok' if table prepared."
            ),
        ),
        Tool(
            name="units_sold_for_specific_asin",
            func=tool_units_sold_for_specific_asin,
            description=(
                "Use when the user asks for units sold for a specific ASIN. "
                "Examples: 'Show units sold for ASIN B0XXXXXXXX'. "
                "The prompt MUST include a specific ASIN (B0XXXXXXXX format). "
                "Returns 'ok' if value was prepared."
            ),
        ),
        Tool(
            name="sales_for_specific_asin",
            func=tool_sales_for_specific_asin,
            description=(
                "Use when the user asks for sales for a specific ASIN. "
                "Synonyms for 'sales': ATS, net sales, product sales, top line sales. "
                "Examples: 'Show sales for ASIN B0XXXXXXXX', 'Show ATS for ASIN B0XXXXXXXX', "
                "'Show net sales for ASIN B0XXXXXXXX', 'Show product sales for ASIN B0XXXXXXXX', "
                "'Show top line sales for ASIN B0XXXXXXXX'. "
                "The prompt MUST include a specific ASIN (B0XXXXXXXX format). "
                "Returns 'ok' if value was prepared."
            ),
        ),
        Tool(
            name="total_fees_for_specific_asin",
            func=tool_total_fees_for_specific_asin,
            description=(
                "Use when the user asks for total fees for a specific ASIN. "
                "Examples: 'Show total fees for ASIN B0XXXXXXXX'. "
                "The prompt MUST include a specific ASIN (B0XXXXXXXX format). "
                "Returns 'ok' if value was prepared."
            ),
        ),
        Tool(
            name="gross_profit_for_specific_asin",
            func=tool_gross_profit_for_specific_asin,
            description=(
                "Use when the user asks for gross profit for a specific ASIN. "
                "Synonyms for 'gross profit': RGP, GP. "
                "Examples: 'Show gross profit for ASIN B0XXXXXXXX', 'Show RGP for ASIN B0XXXXXXXX', "
                "'Show GP for ASIN B0XXXXXXXX'. "
                "The prompt MUST include a specific ASIN (B0XXXXXXXX format). "
                "Returns 'ok' if value was prepared."
            ),
        ),
        Tool(
            name="gross_margin_for_specific_asin",
            func=tool_gross_margin_for_specific_asin,
            description=(
                "Use when the user asks for gross margin for a specific ASIN. "
                "Synonyms for 'gross margin': margin %. "
                "Examples: 'Show gross margin for ASIN B0XXXXXXXX', 'Show margin % for ASIN B0XXXXXXXX'. "
                "The prompt MUST include a specific ASIN (B0XXXXXXXX format). "
                "Returns 'ok' if value was prepared."
            ),
        ),
        Tool(
            name="top_brands_gross_profit",
            func=tool_top_brands_gross_profit,
            description=(
                "Use when the user asks for top X brands gross profit in descending order. "
                "Synonyms for 'gross profit': RGP, GP, profit, net profit. "
                "Examples: 'Show top 5 brands gross profit in descending order', 'Show top 10 brands RGP in descending order', "
                "'Show top 3 brands GP in descending order', 'Show top 7 brands profit in descending order', "
                "'Show top 15 brands net profit in descending order'. "
                "Input: the full user prompt. Return 'ok' if table prepared."
            ),
        ),
        Tool(
            name="top_brands_gross_margin",
            func=tool_top_brands_gross_margin,
            description=(
                "Use when the user asks for top X brands gross margin in descending order. "
                "Synonyms for 'gross margin': margin %. "
                "Examples: 'Show top 5 brands gross margin in descending order', 'Show top 10 brands margin % in descending order', "
                "'Show top 3 brands gross margin in descending order', 'Show top 7 brands margin % in descending order', "
                "'Show top 15 brands gross margin in descending order'. "
                "Input: the full user prompt. Return 'ok' if table prepared."
            ),
        ),
        Tool(
            name="top_asins_gross_profit",
            func=tool_top_asins_gross_profit,
            description=(
                "Use when the user asks for top X ASINs gross profit in descending order. "
                "Synonyms for 'gross profit': RGP, GP, profit, net profit. "
                "Examples: 'Show top 5 ASINs gross profit in descending order', 'Show top 10 ASINs RGP in descending order', "
                "'Show top 3 ASINs GP in descending order', 'Show top 7 ASINs profit in descending order', "
                "'Show top 15 ASINs net profit in descending order'. "
                "Input: the full user prompt. Return 'ok' if table prepared."
            ),
        ),
        Tool(
            name="top_asins_gross_margin",
            func=tool_top_asins_gross_margin,
            description=(
                "Use when the user asks for top X ASINs gross margin in descending order. "
                "Synonyms for 'gross margin': margin %. "
                "Examples: 'Show top 5 ASINs gross margin in descending order', 'Show top 10 ASINs margin % in descending order', "
                "'Show top 3 ASINs gross margin in descending order', 'Show top 7 ASINs margin % in descending order', "
                "'Show top 15 ASINs gross margin in descending order'. "
                "Input: the full user prompt. Return 'ok' if table prepared."
            ),
        ),
        Tool(
            name="total_sales_gross_profit_margin_brands",
            func=tool_total_sales_gross_profit_margin_brands,
            description=(
                "Use when the user asks for total sales, gross profit, and margin for all brands. "
                "Synonyms for 'sales': ATS, net sales, product sales, top line sales. "
                "Synonyms for 'gross profit': RGP, GP. "
                "Synonyms for 'margin': margin %. "
                "Examples: 'Show total sales, gross profit, and margin for all brands', "
                "'Show total ATS, RGP, and margin % for all brands', "
                "'Show total net sales, GP, and margin % for all brands', "
                "'Show total product sales, gross profit, and margin for all brands', "
                "'Show total top line sales, RGP, and margin % for all brands'. "
                "Input: the full user prompt. Return 'ok' if table prepared."
            ),
        ),
        Tool(
            name="orders_sales_price_lower_plan",
            func=tool_orders_sales_price_lower_plan,
            description=(
                "Use when the user asks for orders where the sales price was lower than plan for ASIN XYZ. "
                "Synonyms for 'orders': sales. "
                "Synonyms for 'sales price': average order value, AOV. "
                "Examples: 'Show orders where the sales price was lower than plan for ASIN B0XXXXXXXX', "
                "'Show sales where the average order value was lower than plan for ASIN B0XXXXXXXX', "
                "'Show orders where the AOV was lower than plan for ASIN B0XXXXXXXX', "
                "'Show sales where the sales price was lower than plan for ASIN B0XXXXXXXX'. "
                "Input: the full user prompt. Return 'ok' if table prepared."
            ),
        ),
        Tool(
            name="asins_avg_price_lower_plan",
            func=tool_asins_avg_price_lower_plan,
            description=(
                "Use when the user asks for ASINs where the average sales price was lower than plan. "
                "Synonyms for 'average sales price': average order value, AOV. "
                "Examples: 'Show ASINs where the average sales price was lower than plan', "
                "'Show ASINs where the average order value was lower than plan', "
                "'Show ASINs where the AOV was lower than plan'. "
                "Input: the full user prompt. Return 'ok' if table prepared."
            ),
        ),
        Tool(
            name="asins_fees_higher_plan",
            func=tool_asins_fees_higher_plan,
            description=(
                "Use when the user asks for ASINs with average total fees higher than plan for the last settlement period. "
                "Synonyms for 'last settlement period': LSP, settlement. "
                "Examples: 'Show ASINs with average total fees higher than plan for the last settlement period', "
                "'Show ASINs with average total fees higher than plan for the LSP', "
                "'Show ASINs with average total fees higher than plan for the settlement'. "
                "Input: the full user prompt. Return 'ok' if table prepared."
            ),
        ),
        Tool(
            name="orders_fees_higher_plan",
            func=tool_orders_fees_higher_plan,
            description=(
                "Use when the user asks for orders where fees were higher than plan for ASIN XYZ. "
                "Examples: 'Show orders where fees were higher than plan for ASIN B0XXXXXXXX'. "
                "Input: the full user prompt. Return 'ok' if table prepared."
            ),
        ),
        Tool(
            name="orders_referral_fees_higher_plan",
            func=tool_orders_referral_fees_higher_plan,
            description=(
                "Use when the user asks for orders where referral fees were higher than plan for ASIN XYZ. "
                "Examples: 'Show orders where referral fees were higher than plan for ASIN B0XXXXXXXX'. "
                "Input: the full user prompt. Return 'ok' if table prepared."
            ),
        ),
        Tool(
            name="orders_fulfillment_fees_higher_plan",
            func=tool_orders_fulfillment_fees_higher_plan,
            description=(
                "Use when the user asks for orders where fulfillment fees were higher than plan for ASIN XYZ. "
                "Examples: 'Show orders where fulfillment fees were higher than plan for ASIN B0XXXXXXXX'. "
                "Input: the full user prompt. Return 'ok' if table prepared."
            ),
        ),
        Tool(
            name="settlement_period_definition",
            func=tool_settlement_period_definition,
            description=(
                "Use when the user asks 'What is a settlement period?' or similar questions about settlement period definition. "
                "Examples: 'What is a settlement period?', 'Define settlement period', 'Explain settlement period'. "
                "Input: the full user prompt. Return 'ok' if definition prepared."
            ),
        ),
        Tool(
            name="settlement_period_timing",
            func=tool_settlement_period_timing,
            description=(
                "Use when the user asks 'When is my settlement period?' or similar questions about settlement period timing. "
                "Examples: 'When is my settlement period?', 'What is my settlement period?', 'When does my settlement period occur?'. "
                "Input: the full user prompt. Return 'ok' if timing information prepared."
            ),
        ),
        Tool(
            name="settlement_period_reason",
            func=tool_settlement_period_reason,
            description=(
                "Use when the user asks 'Why is my settlement period that date range?' or similar questions about why settlement period has specific dates. "
                "Examples: 'Why is my settlement period that date range?', 'Why does my settlement period start on that date?', 'How is my settlement period determined?'. "
                "Input: the full user prompt. Return 'ok' if explanation prepared."
            ),
        ),




    ]

    llm = ChatOpenAI(model="gpt-4o", temperature=0)
    prompt = ChatPromptTemplate.from_messages(
        [
            ("system", SYSTEM_PROMPT),
            (
                "human",
                "User query:\n{input}\n\n"
                "Choose exactly one tool if it matches the task. "
                "CRITICAL: If the user mentions a specific ASIN (B0XXXXXXXX) and asks about 'orders', "
                "use orders_below_plan_for_asin. If they ask about 'ASINs' in general (no specific ASIN), "
                "use asins_avg_price_below_plan. "
                "Do not invent numbers; the tools will compute and store the results for the UI."
            ),
            ("placeholder", "{agent_scratchpad}"),
        ]
    )
    agent = create_tool_calling_agent(llm, tools, prompt)
    return AgentExecutor(agent=agent, tools=tools, verbose=False, max_iterations=10)

# ──────────────────────────────────────────────────────────────────────────────
# 5) STREAMLIT UI
# ──────────────────────────────────────────────────────────────────────────────
st.set_page_config(page_title="Amazon Business Analyst Assistant", layout="centered")
st.title("🤖 Amazon Business Analyst Assistant")

# Add clear conversation button
if st.button("🗑️ Clear Conversation History"):
    st.session_state.conversation_history = []
    st.rerun()

# build agent once
if "agent" not in st.session_state:
    st.session_state["agent"] = build_agent()

# Initialize conversation history if not exists
if "conversation_history" not in st.session_state:
    st.session_state.conversation_history = []

# chat UI
user_prompt = st.chat_input("Ask things like: 'Show gross profit for all ASINs' or 'Show top 5 brands' or 'Show units sold for ASIN B08MFNJSPG'")
if not user_prompt:
    st.stop()

# Get session ID for logging
session_id = get_session_id()

# Display conversation history
for message in st.session_state.conversation_history:
    with st.chat_message(message["role"]):
        if message["role"] == "user":
            st.write(message["content"])
        else:
            # For assistant messages, we need to handle different types of content
            if "error" in message and message["error"]:
                st.error(message["content"])
            elif "response_type" in message and message["response_type"] == "dataframe":
                st.write(message["text"])
                st.dataframe(message["dataframe"], use_container_width=True)
                if "source_file" in message:
                    st.caption(f"Source file: {message['source_file']}")
            else:
                st.write(message["content"])

# Add current user message to history and display it
st.session_state.conversation_history.append({"role": "user", "content": user_prompt})
with st.chat_message("user"):
    st.write(user_prompt)

# clear previous outputs (but keep conversation history)
for k in ("conv_table", "conv_period", "avg_cr", "avg_period_display", "agent_error", "business_file", 
          "gross_sales_table", "gross_sales_period", "gross_sales_total", "gross_sales_days",
          "gross_sales_total_only", "gross_sales_period_only", "net_sales_table", "net_sales_period",
          "gross_sales_specific_asin", "gross_sales_specific_asin_period", "gross_sales_specific_asin_code",
          "below_plan_table", "below_plan_period", "orders_table", "orders_period", "orders_asin",
          "buy_box_table", "buy_box_period", "avg_buy_box", "avg_buy_box_period", 
          "sessions_table", "sessions_period", "sessions_total", "sessions_days",
          "aov_table", "aov_period", "aov_overall", "aov_days", 
          "suppressed_table", "suppressed_period", "suppressed_days", 
          "currently_suppressed_table", "currently_suppressed_period", 
          "sales_lost_table", "sales_lost_period", "sales_lost_total", 
          "profit_lost_table", "profit_lost_period", "profit_lost_total",
          "gm_underperformers_table", "gm_underperformers_period", "gp_all_asins", "gp_all_asins_per_unit", "gp_all_asins_period",
          "gp_all_brands", "gp_all_brands_per_unit", "gp_all_brands_period", "gm_all_asins", "gm_all_asins_period",
          "sales_desc_table", "sales_desc_period", "sales_desc_string", "gp_desc_table", "gp_desc_period", "gp_desc_string", 
          "gm_desc_table", "gm_desc_period", "gm_desc_string",
          "brands_sales_desc_table", "brands_sales_desc_period", "brands_sales_desc_string", 
          "brands_gp_desc_table", "brands_gp_desc_period", "brands_gp_desc_string",
          "brands_gm_desc_table", "brands_gm_desc_period", "brands_gm_desc_string",
          "top_brands_sales_table", "top_brands_sales_count", "top_brands_sales_period", "top_brands_sales_string",
          "top_asins_sales_table", "top_asins_sales_count", "top_asins_sales_period", "top_asins_sales_string",
          "fees_higher_plan_table", "fees_higher_plan_period",
          "top_brands_table", "top_brands_count", "top_asins_table", "top_asins_count", 
          "fees_all_asins_table", "fees_all_brands_table",
          "gp_all_asins_table", "gp_all_asins_total", "gp_all_brands_table", "gp_all_brands_total", 
          "gm_all_asins_table", "gm_all_brands_table", "gm_all_asins_string", "gm_all_brands_string",
          "top_brands_gp_table", "top_brands_gp_count", "top_brands_gp_period", "top_brands_gp_string",
          "top_brands_gm_table", "top_brands_gm_count", "top_brands_gm_period", "top_brands_gm_string",
          "top_asins_gp_table", "top_asins_gp_count", "top_asins_gp_period", "top_asins_gp_string",
          "top_asins_gm_table", "top_asins_gm_count", "top_asins_gm_period", "top_asins_gm_string",
          "total_brands_table", "total_brands_period", "total_brands_string",
          "orders_sales_lower_table", "orders_sales_lower_period", "orders_sales_lower_string",
          "asins_avg_price_lower_table", "asins_avg_price_lower_period", "asins_avg_price_lower_string",
          "asins_fees_higher_table", "asins_fees_higher_period", "asins_fees_higher_string",
          "orders_fees_higher_table", "orders_fees_higher_period", "orders_fees_higher_string",
          "orders_referral_fees_higher_table", "orders_referral_fees_higher_period", "orders_referral_fees_higher_string",
          "orders_fulfillment_fees_higher_table", "orders_fulfillment_fees_higher_period", "orders_fulfillment_fees_higher_string",
          "settlement_period_definition", "settlement_period_timing", "settlement_period_reason",
          "units_sold_asin", "units_sold_count", "units_sold_period",
          "sales_asin", "sales_amount", "sales_units", "sales_period",
          "fees_asin", "fees_total", "fees_per_unit", "fees_period",
          "gp_asin", "gp_amount", "gp_per_unit", "gp_period",
          "gm_asin", "gm_margin", "gm_period"):
    st.session_state.pop(k, None)

# Check data availability before invoking agent
data_availability = check_data_availability()
training_message = get_training_status_message(data_availability, user_prompt)

# invoke agent
error_occurred = False
try:
    result = st.session_state["agent"].invoke({"input": user_prompt})
    assistant_text = result.get("output", "").strip()
    
    # Check if the agent couldn't find a suitable tool and we have training issues
    if (not assistant_text or 
        "I didn't detect a supported request" in assistant_text or
        "I don't have access to" in assistant_text or
        "I cannot" in assistant_text) and training_message:
        assistant_text = training_message
        
except Exception as e:
    error_occurred = True
    # If agent fails and we have training issues, show training message
    if training_message:
        assistant_text = training_message
    else:
        assistant_text = f"I encountered an error while processing your request: {str(e)}"

# Log the conversation
log_conversation(
    user_question=user_prompt,
    ai_response=assistant_text,
    session_id=session_id,
    data_availability=data_availability,
    error_occurred=error_occurred
)

# Helper function to store assistant response in conversation history
def store_assistant_response(content, response_type="text", dataframe=None, text=None, source_file=None, error=False):
    message_data = {
        "role": "assistant",
        "content": content,
        "response_type": response_type
    }
    if dataframe is not None:
        message_data["dataframe"] = dataframe
    if text is not None:
        message_data["text"] = text
    if source_file is not None:
        message_data["source_file"] = source_file
    if error:
        message_data["error"] = True
    st.session_state.conversation_history.append(message_data)

# Helper function to capture and store simple text responses
def capture_and_store_response(response_text, source_file=None):
    store_assistant_response(response_text, source_file=source_file)

# RENDER — EXACT WORDING YOU REQUESTED
with st.chat_message("assistant"):
    if st.session_state.get("agent_error"):
        st.error(st.session_state["agent_error"])
        store_assistant_response(st.session_state["agent_error"], error=True)

    elif st.session_state.get("conv_table") is not None:
        # No echo of the user's question; date NOT bold
        period_line = st.session_state["conv_period"]
        response_text = f"The Conversion Rates from {period_line} were:"
        st.write(response_text)
        st.dataframe(st.session_state["conv_table"][["Brand", "ASIN", "Conversion Rate"]], use_container_width=True)
        source_file = st.session_state.get("business_file")
        if source_file:
            st.caption(f"Source file: {source_file}")
        store_assistant_response(
            response_text, 
            response_type="dataframe", 
            dataframe=st.session_state["conv_table"][["Brand", "ASIN", "Conversion Rate"]],
            text=response_text,
            source_file=source_file
        )

    elif st.session_state.get("avg_cr") is not None:
        response_text = f"The average Conversion Rate was {st.session_state['avg_cr']:.2f}% from {st.session_state['avg_period_display']}"
        st.write(response_text)
        source_file = st.session_state.get("business_file")
        if source_file:
            st.caption(f"Source file: {source_file}")
        store_assistant_response(response_text, source_file=source_file)

    elif st.session_state.get("below_plan_table") is not None:
        response_text = f"The following ASINs have an average sales price below the planned sales price for the settlement period {st.session_state['below_plan_period']}:"
        st.write(response_text)
        st.dataframe(st.session_state["below_plan_table"], use_container_width=True)
        store_assistant_response(
            response_text, 
            response_type="dataframe", 
            dataframe=st.session_state["below_plan_table"],
            text=response_text
        )

    elif st.session_state.get("orders_table") is not None:
        response_text = f"Here are all orders where the sales price was lower than planned for ASIN {st.session_state['orders_asin']} for the settlement period {st.session_state['orders_period']}:"
        st.write(response_text)
        st.dataframe(st.session_state["orders_table"], use_container_width=True)
        store_assistant_response(
            response_text, 
            response_type="dataframe", 
            dataframe=st.session_state["orders_table"],
            text=response_text
        )

    elif st.session_state.get("gross_sales_table") is not None:
        response_text = f"Gross sales were ${st.session_state['gross_sales_total']:,.2f} from {st.session_state['gross_sales_period']}:"
        st.write(response_text)
        st.dataframe(st.session_state["gross_sales_table"], use_container_width=True)
        source_file = st.session_state.get("business_file")
        if source_file:
            st.caption(f"Source file: {source_file}")
        store_assistant_response(
            response_text, 
            response_type="dataframe", 
            dataframe=st.session_state["gross_sales_table"],
            text=response_text,
            source_file=source_file
        )

    elif st.session_state.get("gross_sales_total_only") is not None:
        response_text = f"Gross sales were ${st.session_state['gross_sales_total_only']:,.2f} from {st.session_state['gross_sales_period_only']}."
        st.write(response_text)
        source_file = st.session_state.get("business_file")
        if source_file:
            st.caption(f"Source file: {source_file}")
        capture_and_store_response(response_text, source_file)

    elif st.session_state.get("net_sales_table") is not None:
        response_text = f"Net sales can only be calculated from your Amazon settlement period. Net sales for the last settlement period {st.session_state['net_sales_period']} were:"
        st.write(response_text)
        st.dataframe(st.session_state["net_sales_table"], use_container_width=True)
        store_assistant_response(
            response_text, 
            response_type="dataframe", 
            dataframe=st.session_state["net_sales_table"],
            text=response_text
        )

    elif st.session_state.get("gross_sales_specific_asin") is not None:
        response_text = f"Gross sales for ASIN {st.session_state['gross_sales_specific_asin_code']} were ${st.session_state['gross_sales_specific_asin']:,.2f} from {st.session_state['gross_sales_specific_asin_period']}."
        st.write(response_text)
        source_file = st.session_state.get("business_file")
        if source_file:
            st.caption(f"Source file: {source_file}")
        capture_and_store_response(response_text, source_file)

    elif st.session_state.get("buy_box_table") is not None:
        response_text = f"Buy Box percentages for ASINs from {st.session_state['buy_box_period']} were:"
        st.write(response_text)
        st.dataframe(st.session_state["buy_box_table"], use_container_width=True)
        source_file = st.session_state.get("business_file")
        if source_file:
            st.caption(f"Source file: {source_file}")
        store_assistant_response(
            response_text, 
            response_type="dataframe", 
            dataframe=st.session_state["buy_box_table"],
            text=response_text,
            source_file=source_file
        )

    elif st.session_state.get("avg_buy_box") is not None:
        response_text = f"The average Buy Box percentage was {st.session_state['avg_buy_box']:.2f}% from {st.session_state['avg_buy_box_period']}"
        st.write(response_text)
        source_file = st.session_state.get("business_file")
        if source_file:
            st.caption(f"Source file: {source_file}")
        capture_and_store_response(response_text, source_file)

    elif st.session_state.get("sessions_table") is not None:
        response_text = f"The total number of Sessions were {st.session_state['sessions_total']:,} from {st.session_state['sessions_period']}:"
        st.write(response_text)
        st.dataframe(st.session_state["sessions_table"], use_container_width=True)
        source_file = st.session_state.get("business_file")
        if source_file:
            st.caption(f"Source file: {source_file}")
        store_assistant_response(
            response_text, 
            response_type="dataframe", 
            dataframe=st.session_state["sessions_table"],
            text=response_text,
            source_file=source_file
        )

    elif st.session_state.get("aov_table") is not None:
        response_text = f"The Average Order Value was ${st.session_state['aov_overall']:,.2f} from {st.session_state['aov_period']}:"
        st.write(response_text)
        st.dataframe(st.session_state["aov_table"], use_container_width=True)
        source_file = st.session_state.get("business_file")
        if source_file:
            st.caption(f"Source file: {source_file}")
        store_assistant_response(
            response_text, 
            response_type="dataframe", 
            dataframe=st.session_state["aov_table"],
            text=response_text,
            source_file=source_file
        )

    elif st.session_state.get("suppressed_table") is not None:
        st.write(
            f"While I cannot show if an ASIN was suppressed on a past date, "
            f"a low Buy Box percentage can indicate if there is an issue with the listing. "
            f"Here are the ASINs with Buy Box percentages below 80% from {st.session_state['suppressed_period']}:"
        )
        st.dataframe(st.session_state["suppressed_table"], use_container_width=True)
        if st.session_state.get("business_file"):
            st.caption(f"Source file: {st.session_state['business_file']}")

    elif st.session_state.get("currently_suppressed_table") is not None:
        st.write(
            f"Here are the ASINs with a 0% Buy Box from {st.session_state['currently_suppressed_period']}:"
        )
        st.dataframe(st.session_state["currently_suppressed_table"], use_container_width=True)
        if st.session_state.get("business_file"):
            st.caption(f"Source file: {st.session_state['business_file']}")

    elif st.session_state.get("sales_lost_table") is not None:
        st.write(
            f"The amount of sales lost to other sellers from {st.session_state['sales_lost_period']} was ${st.session_state['sales_lost_total']:,.2f}."
        )
        st.dataframe(st.session_state["sales_lost_table"], use_container_width=True)
        if st.session_state.get("business_file"):
            st.caption(f"Source file: {st.session_state['business_file']}")

    elif st.session_state.get("profit_lost_table") is not None:
        st.write(
            f"The amount of gross profit lost to other sellers from {st.session_state['profit_lost_period']} was ${st.session_state['profit_lost_total']:,.2f}."
        )
        st.dataframe(st.session_state["profit_lost_table"], use_container_width=True)
        if st.session_state.get("business_file"):
            st.caption(f"Source file: {st.session_state['business_file']}")

    elif st.session_state.get("gm_underperformers_table") is not None:
        st.write(
            f"The following ASINs have a gross margin less than planned for the settlement period {settlement_period}:")
        st.dataframe(st.session_state["gm_underperformers_table"], use_container_width=True)
        if st.session_state.get("business_file"):
            st.caption(f"Source file: {st.session_state['business_file']}")

    elif st.session_state.get("gp_all_asins") is not None:
        st.write(
            f"Gross profit for all ASINs was ${st.session_state['gp_all_asins']:,.2f} from {st.session_state['gp_all_asins_period']}.")
        if st.session_state.get("business_file"):
            st.caption(f"Source file: {st.session_state['business_file']}")

    elif st.session_state.get("gp_all_brands") is not None:
        st.write(
            f"Gross profit for all brands was ${st.session_state['gp_all_brands']:,.2f} from {st.session_state['gp_all_brands_period']}.")
        if st.session_state.get("business_file"):
            st.caption(f"Source file: {st.session_state['business_file']}")

    elif st.session_state.get("gm_all_asins") is not None:
        st.write(
            f"Gross margin for all ASINs was {st.session_state['gm_all_asins']:.2f}% from {st.session_state['gm_all_asins_period']}.")
        if st.session_state.get("business_file"):
            st.caption(f"Source file: {st.session_state['business_file']}")

    elif st.session_state.get("sales_desc_table") is not None:
        st.text(st.session_state["sales_desc_string"])
        st.dataframe(st.session_state["sales_desc_table"], use_container_width=True)
        if st.session_state.get("business_file"):
            st.caption(f"Source file: {st.session_state['business_file']}")

    elif st.session_state.get("gp_desc_table") is not None:
        st.text(st.session_state["gp_desc_string"])
        st.dataframe(st.session_state["gp_desc_table"], use_container_width=True)
        if st.session_state.get("business_file"):
            st.caption(f"Source file: {st.session_state['business_file']}")

    elif st.session_state.get("gm_desc_table") is not None:
        st.text(st.session_state["gm_desc_string"])
        st.dataframe(st.session_state["gm_desc_table"], use_container_width=True)
        if st.session_state.get("business_file"):
            st.caption(f"Source file: {st.session_state['business_file']}")

    elif st.session_state.get("brands_sales_desc_table") is not None:
        st.text(st.session_state["brands_sales_desc_string"])
        st.dataframe(st.session_state["brands_sales_desc_table"], use_container_width=True)
        if st.session_state.get("business_file"):
            st.caption(f"Source file: {st.session_state['business_file']}")

    elif st.session_state.get("brands_gp_desc_table") is not None:
        st.text(st.session_state["brands_gp_desc_string"])
        st.dataframe(st.session_state["brands_gp_desc_table"], use_container_width=True)
        if st.session_state.get("business_file"):
            st.caption(f"Source file: {st.session_state['business_file']}")

    elif st.session_state.get("brands_gm_desc_table") is not None:
        st.text(st.session_state["brands_gm_desc_string"])
        st.dataframe(st.session_state["brands_gm_desc_table"], use_container_width=True)
        if st.session_state.get("business_file"):
            st.caption(f"Source file: {st.session_state['business_file']}")

    elif st.session_state.get("top_brands_sales_table") is not None:
        st.text(st.session_state["top_brands_sales_string"])
        st.dataframe(st.session_state["top_brands_sales_table"], use_container_width=True)
        if st.session_state.get("business_file"):
            st.caption(f"Source file: {st.session_state['business_file']}")

    elif st.session_state.get("top_asins_sales_table") is not None:
        st.text(st.session_state["top_asins_sales_string"])
        st.dataframe(st.session_state["top_asins_sales_table"], use_container_width=True)
        if st.session_state.get("business_file"):
            st.caption(f"Source file: {st.session_state['business_file']}")

    elif st.session_state.get("fees_higher_plan_table") is not None:
        st.write(
            f"ASINs with average total fees higher than plan:")
        st.dataframe(st.session_state["fees_higher_plan_table"], use_container_width=True)
        if st.session_state.get("business_file"):
            st.caption(f"Source file: {st.session_state['business_file']}")

    elif st.session_state.get("top_brands_table") is not None:
        st.write(
            f"Top {st.session_state['top_brands_count']} brands by sales:")
        st.dataframe(st.session_state["top_brands_table"], use_container_width=True)
        if st.session_state.get("business_file"):
            st.caption(f"Source file: {st.session_state['business_file']}")

    elif st.session_state.get("top_asins_table") is not None:
        st.write(
            f"Top {st.session_state['top_asins_count']} ASINs by sales:")
        st.dataframe(st.session_state["top_asins_table"], use_container_width=True)
        if st.session_state.get("business_file"):
            st.caption(f"Source file: {st.session_state['business_file']}")

    elif st.session_state.get("fees_all_asins_table") is not None:
        st.write(
            f"Fees for all ASINs:")
        st.dataframe(st.session_state["fees_all_asins_table"], use_container_width=True)
        if st.session_state.get("business_file"):
            st.caption(f"Source file: {st.session_state['business_file']}")

    elif st.session_state.get("fees_all_brands_table") is not None:
        st.write(
            f"Fees for all brands:")
        st.dataframe(st.session_state["fees_all_brands_table"], use_container_width=True)
        if st.session_state.get("business_file"):
            st.caption(f"Source file: {st.session_state['business_file']}")

    elif st.session_state.get("gp_all_asins_string") is not None:
        st.text(st.session_state["gp_all_asins_string"])
        if st.session_state.get("business_file"):
            st.caption(f"Source file: {st.session_state['business_file']}")

    elif st.session_state.get("gp_all_brands_string") is not None:
        st.text(st.session_state["gp_all_brands_string"])
        if st.session_state.get("business_file"):
            st.caption(f"Source file: {st.session_state['business_file']}")

    elif st.session_state.get("gm_all_asins_string") is not None:
        st.text(st.session_state["gm_all_asins_string"])
        if st.session_state.get("business_file"):
            st.caption(f"Source file: {st.session_state['business_file']}")

    elif st.session_state.get("gm_all_brands_string") is not None:
        st.text(st.session_state["gm_all_brands_string"])
        if st.session_state.get("business_file"):
            st.caption(f"Source file: {st.session_state['business_file']}")

    elif st.session_state.get("sales_desc_table") is not None:
        st.text(st.session_state["sales_desc_string"])
        st.dataframe(st.session_state["sales_desc_table"], use_container_width=True)
        if st.session_state.get("business_file"):
            st.caption(f"Source file: {st.session_state['business_file']}")

    elif st.session_state.get("gp_desc_table") is not None:
        st.text(st.session_state["gp_desc_string"])
        st.dataframe(st.session_state["gp_desc_table"], use_container_width=True)
        if st.session_state.get("business_file"):
            st.caption(f"Source file: {st.session_state['business_file']}")

    elif st.session_state.get("units_sold_asin") is not None:
        st.write(
            f"You sold {st.session_state['units_sold_count']:,} units of ASIN {st.session_state['units_sold_asin']} "
            f"in the settlement period {st.session_state['units_sold_period']}.")
        if st.session_state.get("business_file"):
            st.caption(f"Source file: {st.session_state['business_file']}")

    elif st.session_state.get("sales_asin") is not None:
        st.write(
            f"Sales for ASIN {st.session_state['sales_asin']} were ${st.session_state['sales_amount']:,.2f} "
            f"({st.session_state['sales_units']:,} number of units) in the settlement period {st.session_state['sales_period']}.")
        if st.session_state.get("business_file"):
            st.caption(f"Source file: {st.session_state['business_file']}")

    elif st.session_state.get("fees_asin") is not None:
        st.text(
            f"The total fees for ASIN {st.session_state['fees_asin']} were ${st.session_state['fees_total']:,.2f} (${st.session_state['fees_per_unit']:.2f} per unit) "
            f"in the settlement period {st.session_state['fees_period']}.")
        if st.session_state.get("business_file"):
            st.caption(f"Source file: {st.session_state['business_file']}")

    elif st.session_state.get("gp_asin") is not None:
        st.text(
            f"The gross profit for ASIN {st.session_state['gp_asin']} was ${st.session_state['gp_amount']:,.2f} "
            f"(${st.session_state['gp_per_unit']:.2f} per unit) in the settlement period {st.session_state['gp_period']}.")
        if st.session_state.get("business_file"):
            st.caption(f"Source file: {st.session_state['business_file']}")

    elif st.session_state.get("gm_asin") is not None:
        st.write(
            f"The gross margin for ASIN {st.session_state['gm_asin']} was {st.session_state['gm_margin']} "
            f"in the settlement period {st.session_state['gm_period']}.")
        if st.session_state.get("business_file"):
            st.caption(f"Source file: {st.session_state['business_file']}")

    elif st.session_state.get("top_brands_gp_table") is not None:
        st.text(st.session_state["top_brands_gp_string"])
        st.dataframe(st.session_state["top_brands_gp_table"], use_container_width=True)
        if st.session_state.get("business_file"):
            st.caption(f"Source file: {st.session_state['business_file']}")

    elif st.session_state.get("top_brands_gm_table") is not None:
        st.text(st.session_state["top_brands_gm_string"])
        st.dataframe(st.session_state["top_brands_gm_table"], use_container_width=True)
        if st.session_state.get("business_file"):
            st.caption(f"Source file: {st.session_state['business_file']}")

    elif st.session_state.get("top_asins_gp_table") is not None:
        st.text(st.session_state["top_asins_gp_string"])
        st.dataframe(st.session_state["top_asins_gp_table"], use_container_width=True)
        if st.session_state.get("business_file"):
            st.caption(f"Source file: {st.session_state['business_file']}")

    elif st.session_state.get("top_asins_gm_table") is not None:
        st.text(st.session_state["top_asins_gm_string"])
        st.dataframe(st.session_state["top_asins_gm_table"], use_container_width=True)
        if st.session_state.get("business_file"):
            st.caption(f"Source file: {st.session_state['business_file']}")

    elif st.session_state.get("total_brands_table") is not None:
        st.text(st.session_state["total_brands_string"])
        st.dataframe(st.session_state["total_brands_table"], use_container_width=True)
        if st.session_state.get("business_file"):
            st.caption(f"Source file: {st.session_state['business_file']}")

    elif st.session_state.get("orders_sales_lower_table") is not None:
        st.text(st.session_state["orders_sales_lower_string"])
        st.dataframe(st.session_state["orders_sales_lower_table"], use_container_width=True)
        if st.session_state.get("business_file"):
            st.caption(f"Source file: {st.session_state['business_file']}")

    elif st.session_state.get("asins_avg_price_lower_table") is not None:
        st.text(st.session_state["asins_avg_price_lower_string"])
        st.dataframe(st.session_state["asins_avg_price_lower_table"], use_container_width=True)
        if st.session_state.get("business_file"):
            st.caption(f"Source file: {st.session_state['business_file']}")

    elif st.session_state.get("asins_fees_higher_table") is not None:
        st.text(st.session_state["asins_fees_higher_string"])
        st.dataframe(st.session_state["asins_fees_higher_table"], use_container_width=True)
        if st.session_state.get("business_file"):
            st.caption(f"Source file: {st.session_state['business_file']}")

    elif st.session_state.get("orders_fees_higher_table") is not None:
        st.text(st.session_state["orders_fees_higher_string"])
        st.dataframe(st.session_state["orders_fees_higher_table"], use_container_width=True)
        if st.session_state.get("business_file"):
            st.caption(f"Source file: {st.session_state['business_file']}")

    elif st.session_state.get("orders_referral_fees_higher_table") is not None:
        st.text(st.session_state["orders_referral_fees_higher_string"])
        st.dataframe(st.session_state["orders_referral_fees_higher_table"], use_container_width=True)
        if st.session_state.get("business_file"):
            st.caption(f"Source file: {st.session_state['business_file']}")

    elif st.session_state.get("orders_fulfillment_fees_higher_table") is not None:
        st.text(st.session_state["orders_fulfillment_fees_higher_string"])
        st.dataframe(st.session_state["orders_fulfillment_fees_higher_table"], use_container_width=True)
        if st.session_state.get("business_file"):
            st.caption(f"Source file: {st.session_state['business_file']}")

    elif st.session_state.get("settlement_period_definition") is not None:
        st.text(st.session_state["settlement_period_definition"])

    elif st.session_state.get("settlement_period_timing") is not None:
        st.text(st.session_state["settlement_period_timing"])

    elif st.session_state.get("settlement_period_reason") is not None:
        st.text(st.session_state["settlement_period_reason"])

    else:
        # If we have training issues, show the training message instead of default response
        if training_message and not assistant_text:
            st.markdown(training_message)
            store_assistant_response(training_message)
        else:
            final_response = assistant_text or "I didn't detect a supported request. Try one of the two examples above."
            st.write(final_response)
            store_assistant_response(final_response)