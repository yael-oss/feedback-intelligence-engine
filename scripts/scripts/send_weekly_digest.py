#!/usr/bin/env python3
"""
Generate and send weekly feedback digest - Queries Notion database for past 7 days,
generates summary with Claude, and sends to Slack DM.
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

def query_notion_past_week():
    """
    Query Notion database for all entries from the past 7 days.
    """
    headers = {
        "Authorization": f"Bearer {notion_api_key}",
        "Notion-Version": "2022-06-28",
        "Content-Type": "application/json"
    }
    
    one_week_ago = (datetime.now() - timedelta(days=7)).isoformat()
    
    payload = {
        "filter": {
            "property": "Date",
            "date": {"on_or_after": one_week_ago}
        }
    }
    
    try:
        response = requests.post(
            f"{NOTION_BASE_URL}/databases/{notion_database_id}/query",
            json=payload,
            headers=headers
        )
        
        if response.status_code == 200:
            return response.json()["results"]
        else:
            print(f"❌ Failed to query Notion: {response.status_code}")
            return []
    except Exception as e:
        print(f"❌ Error querying Notion: {e}")
        return []

def format_entries_for_claude(notion_entries):
    """
    Format Notion entries into a structured format for Claude.
    """
    formatted = []
    
    for entry in notion_entries:
        props = entry.get("properties", {})
        
        # Extract properties safely
        theme = props.get("Theme", {}).get("rich_text", [])
        theme_text = theme[0]["text"]["content"] if theme else "N/A"
        
        persona = props.get("Persona", {}).get("select")
        persona_text = persona["name"] if persona else "Unknown"
        
        tier = props.get("Tier", {}).get("select")
        tier_text = tier["name"] if tier else "N/A"
        
        sentiment = props.get("Sentiment", {}).get("select")
        sentiment_text = sentiment["name"] if sentiment else "N/A"
        
        snippet = props.get("Snippet", {}).get("rich_text", [])
        snippet_text = snippet[0]["text"]["content"] if snippet else "N/A"
        
        source = props.get("Source", {}).get("select")
        source_text = source["name"] if source else "N/A"
        
        bucket = props.get("Strategic Bucket", {}).get("select")
        bucket_text = bucket["name"] if bucket else "Other"
        
        formatted.append({
            "theme": theme_text,
            "persona": persona_text,
            "tier": tier_text,
            "sentiment": sentiment_text,
            "snippet": snippet_text,
            "source": source_text,
            "bucket": bucket_text
        })
    
    return formatted

def generate_digest_with_claude(formatted_entries):
    """
    Generate weekly digest markdown with Claude.
    """
    if not formatted_entries:
        return "No feedback collected this week."
    
    client = anthropic.Anthropic(api_key=anthropic_key)
    
    entries_json = json.dumps(formatted_entries, indent=2)
    
    prompt = f"""Generate a weekly feedback digest in markdown format.

Feedback entries from the past 7 days:
{entries_json}

Format the digest as follows (use markdown):

# Weekly Feedback Summary

## 🚨 Tier 1 Alerts
[List all Tier 1 entries with theme and snippet. If none, write "None this week ✅"]

## 📊 Patterns by Persona
For each persona (Alice, Peter, Carol, Ron), list:
- [Persona]: [themes mentioned] (X mentions)

## ✅ Sentiment Breakdown
- Positive: X% (X entries)
- Negative: X% (X entries)  
- Neutral: X% (X entries)

By Persona:
- Alice: X% positive
- Peter: X% positive
- Carol: X% positive
- Ron: X% positive

## 💡 Top Themes This Week
[List the most mentioned themes and their frequency]

## 🎯 Strategic Buckets
- Autopilot: X entries
- Co-pilot: X entries
- Voice AI: X entries
- Other: X entries

Return only markdown, no extra text or formatting."""
    
    try:
        response = client.messages.create(
            model="claude-opus-4-5-20251101",
            max_tokens=1500,
            messages=[{"role": "user", "content": prompt}]
        )
        
        return response.content[0].text
    except Exception as e:
        print(f"❌ Claude API error: {e}")
        return None

def send_digest_to_slack(digest_text):
    """
    Send the digest to Slack DM.
    """
    try:
        slack_client.chat_postMessage(
            channel=slack_user_id,
            text=digest_text
        )
        print("✅ Weekly digest sent to Slack DM")
        return True
    except Exception as e:
        print(f"❌ Failed to send digest: {e}")
        return False

def main():
    """
    Main function: Generate and send weekly digest.
    """
    print("📊 Generating weekly feedback digest...")
    
    # Query Notion
    print("🔍 Querying Notion for past 7 days...")
    entries = query_notion_past_week()
    
    if not entries:
        print("ℹ️  No feedback entries found this week")
        digest = "# Weekly Feedback Summary\n\nNo feedback collected this week. ✅"
    else:
        print(f"✅ Found {len(entries)} entries")
        
        # Format entries
        formatted = format_entries_for_claude(entries)
        
        # Generate digest with Claude
        print("🤖 Generating digest with Claude...")
        digest = generate_digest_with_claude(formatted)
        
        if not digest:
            print("❌ Failed to generate digest")
            return False
    
    print("\n📝 Digest preview:")
    print("-" * 60)
    print(digest[:500] + "..." if len(digest) > 500 else digest)
    print("-" * 60)
    
    # Send to Slack
    print("\n📨 Sending to Slack DM...")
    if send_digest_to_slack(digest):
        print("\n✅ Weekly digest complete!")
        return True
    else:
        return False

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
