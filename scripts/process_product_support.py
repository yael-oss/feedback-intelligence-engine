#!/usr/bin/env python3
"""
Process Product Support Feedback - Fetch messages from #product-support, 
extract feedback with Claude, create Notion entries, and send Tier 1 alerts.
"""

import os
import sys
import json
from datetime import datetime, timedelta
from slack_sdk import WebClient
import anthropic
import requests

# Initialize clients
slack_client = WebClient(token=os.environ.get("SLACK_BOT_TOKEN"))
slack_user_id = os.environ.get("SLACK_USER_ID")
notion_api_key = os.environ.get("NOTION_API_KEY")
notion_database_id = os.environ.get("NOTION_DATABASE_ID")
anthropic_key = os.environ.get("ANTHROPIC_API_KEY")

NOTION_BASE_URL = "https://api.notion.com/v1"

def extract_feedback_with_claude(message_text):
    """
    Extract theme, persona, tier, bucket, snippet, sentiment from feedback using Claude.
    """
    client = anthropic.Anthropic(api_key=anthropic_key)
    
    prompt = """You are a feedback analyst for a legal services company. Extract structured data from this feedback.

Persona descriptions:
- Alice: Anxious about legal process, needs reassurance, clear communication, consistent updates
- Peter: Wants control and efficiency, needs clear documentation and step-by-step process
- Carol: Cost-conscious, worried about unnecessary expenses, needs transparent pricing
- Ron: Overwhelmed by complexity, wants minimal effort, needs simple instructions

Strategic buckets:
- Autopilot: Express Petition purchase/activation friction
- Co-pilot: LSS intake/form completion friction
- Voice AI: Call routing/escalation/understanding issues
- Other: General feedback

Tier definitions:
- Tier 1: BLOCKER to purchase, form submission, or escalation. Must be addressed immediately.
- Tier 2: Recurring pain point (pattern) that affects user experience but not blocking conversion
- Tier 3: Validation signal or positive feedback

Extract and return ONLY valid JSON (no markdown, no extra text):

Feedback: """ + message_text + """

Return exactly this format:
{
  "theme": "short category (2-5 words)",
  "urgency": "Tier 1" or "Tier 2" or "Tier 3",
  "persona": "Alice" or "Peter" or "Carol" or "Ron" or "Unknown",
  "strategic_bucket": "Autopilot" or "Co-pilot" or "Voice AI" or "Other",
  "snippet": "1-2 sentences, anonymized (remove names, emails, case details)",
  "sentiment": "Positive" or "Negative" or "Neutral"
}"""
    
    try:
        response = client.messages.create(
            model="claude-opus-4-5-20251101",
            max_tokens=500,
            messages=[{"role": "user", "content": prompt}]
        )
        
        text = response.content[0].text
        
        # Remove markdown if present
        if text.startswith("```"):
            text = text.split("```")[1].strip()
            if text.startswith("json"):
                text = text[4:].strip()
        
        return json.loads(text)
    except json.JSONDecodeError as e:
        print(f"❌ Failed to parse Claude response: {text}")
        return None
    except Exception as e:
        print(f"❌ Claude API error: {e}")
        return None

def create_notion_entry(extracted_data, source, timestamp):
    """
    Create a feedback entry in Notion database.
    """
    headers = {
        "Authorization": f"Bearer {notion_api_key}",
        "Notion-Version": "2022-06-28",
        "Content-Type": "application/json"
    }
    
    source_map = {
        "product-support": "product-support",
        "trustpilot": "trustpilot-reviews"
    }
    
    payload = {
        "parent": {"database_id": notion_database_id},
        "properties": {
            "Title": {
                "title": [{"text": {"content": extracted_data["theme"]}}]
            },
            "Date": {
                "date": {"start": datetime.now().isoformat()}
            },
            "Source": {
                "select": {"name": source_map.get(source, source)}
            },
            "Tier": {
                "select": {"name": extracted_data["urgency"]}
            },
            "Persona": {
                "select": {"name": extracted_data["persona"]}
            },
            "Strategic Bucket": {
                "select": {"name": extracted_data["strategic_bucket"]}
            },
            "Snippet": {
                "rich_text": [{"text": {"content": extracted_data["snippet"]}}]
            },
            "Theme": {
                "rich_text": [{"text": {"content": extracted_data["theme"]}}]
            },
            "Sentiment": {
                "select": {"name": extracted_data["sentiment"]}
            }
        }
    }
    
    try:
        response = requests.post(
            f"{NOTION_BASE_URL}/pages",
            json=payload,
            headers=headers
        )
        
        if response.status_code == 200:
            print(f"✅ Created Notion entry: {extracted_data['theme']}")
            return True
        else:
            print(f"❌ Failed to create Notion entry: {response.status_code} {response.text}")
            return False
    except Exception as e:
        print(f"❌ Error creating Notion entry: {e}")
        return False

def send_tier1_alert(extracted_data):
    """
    Send Tier 1 alert as Slack DM.
    """
    alert_msg = f"""🚨 *TIER 1 ALERT*

*Theme:* {extracted_data['theme']}
*Persona:* {extracted_data['persona']}
*Strategic Bucket:* {extracted_data['strategic_bucket']}

*Snippet:*
> {extracted_data['snippet']}

Source: #product-support"""
    
    try:
        slack_client.chat_postMessage(
            channel=slack_user_id,
            text=alert_msg
        )
        print(f"✅ Sent Tier 1 DM alert")
        return True
    except Exception as e:
        print(f"❌ Failed to send DM alert: {e}")
        return False

def process_product_support():
    """
    Main function: Fetch messages from #product-support and process them.
    """
    try:
        # Get channel ID for #product-support
        print("🔍 Fetching #product-support channel...")
        channels = slack_client.conversations_list(limit=100)
        product_support_channel = None
        
        for channel in channels["channels"]:
            if channel["name"] == "product-support":
                product_support_channel = channel["id"]
                break
        
        if not product_support_channel:
            print("❌ #product-support channel not found")
            return False
        
        print(f"✅ Found channel: {product_support_channel}")
        
        # Fetch messages from last hour
        one_hour_ago = int((datetime.now() - timedelta(hours=1)).timestamp())
        
        print(f"📨 Fetching messages from last hour...")
        messages = slack_client.conversations_history(
            channel=product_support_channel,
            oldest=one_hour_ago
        )
        
        if not messages["messages"]:
            print("ℹ️  No new messages in #product-support")
            return True
        
        print(f"📨 Found {len(messages['messages'])} messages to process")
        
        processed = 0
        tier1_count = 0
        
        for message in messages["messages"]:
            text = message.get("text", "").strip()
            
            # Skip empty, bot messages, and system messages
            if not text or message.get("subtype") in ["bot_message", "message_deleted"]:
                continue
            
            # Skip messages that are just emoji reactions or very short
            if len(text) < 10:
                continue
            
            print(f"\n📝 Processing: {text[:60]}...")
            
            # Extract feedback with Claude
            extracted = extract_feedback_with_claude(text)
            if not extracted:
                print("⚠️  Skipping message (extraction failed)")
                continue
            
            # Create Notion entry
            if create_notion_entry(extracted, "product-support", message.get("ts")):
                processed += 1
            
            # If Tier 1, send alert
            if extracted["urgency"] == "Tier 1":
                if send_tier1_alert(extracted):
                    tier1_count += 1
        
        print(f"\n✅ Processing complete!")
        print(f"   - Processed: {processed} messages")
        print(f"   - Tier 1 alerts: {tier1_count}")
        
        return True
    
    except Exception as e:
        print(f"❌ Fatal error: {e}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    success = process_product_support()
    sys.exit(0 if success else 1)
