import json
import logging
import os
import urllib.error
import urllib.request
from datetime import datetime, timezone

logger = logging.getLogger(__name__)

REQUEST_TIMEOUT_SECS = 10
MAX_ERROR_CHARS      = 500


def resolveWebhookUrl():
    return os.environ.get("SLACK_APP_WEBHOOK_URL")


def nowUtc():
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")


def postToSlack(webhookUrl, payload):
    if not webhookUrl:
        logger.warning("Slack webhook URL not set — skipping notification")
        return
    data    = json.dumps(payload).encode("utf-8")
    request = urllib.request.Request(
        webhookUrl,
        data    = data,
        headers = {"Content-Type": "application/json"},
        method  = "POST",
    )
    with urllib.request.urlopen(request, timeout=REQUEST_TIMEOUT_SECS) as response:
        if response.status != 200:
            raise RuntimeError(f"Slack returned HTTP {response.status}")


def buildStartedPayload(executionId):
    return {
        "text": "Music Streaming Pipeline — Started",
        "attachments": [{
            "color": "#A0A0A0",
            "blocks": [
                {
                    "type": "section",
                    "text": {
                        "type": "mrkdwn",
                        "text": ":rocket: *Music Streaming Pipeline — Started*"
                    }
                },
                {
                    "type": "section",
                    "fields": [
                        {"type": "mrkdwn", "text": f"*Execution:*\n`{executionId[-12:]}`"},
                        {"type": "mrkdwn", "text": f"*Time:*\n{nowUtc()}"},
                    ]
                },
                {
                    "type": "context",
                    "elements": [
                        {"type": "mrkdwn", "text": "Crawler → Validate → Transform → KPIs → DynamoDB → Archive"}
                    ]
                }
            ]
        }]
    }


def buildSucceededPayload(executionId):
    return {
        "text": "Music Streaming Pipeline — Succeeded",
        "attachments": [{
            "color": "#36A64F",
            "blocks": [
                {
                    "type": "section",
                    "text": {
                        "type": "mrkdwn",
                        "text": ":large_green_circle: *Music Streaming Pipeline — Succeeded*\nAll KPIs computed and loaded to DynamoDB."
                    }
                },
                {
                    "type": "section",
                    "fields": [
                        {"type": "mrkdwn", "text": f"*Execution:*\n`{executionId[-12:]}`"},
                        {"type": "mrkdwn", "text": f"*Completed:*\n{nowUtc()}"},
                    ]
                }
            ]
        }]
    }


def buildFailedPayload(executionId, failedStep, error):
    return {
        "text": "Music Streaming Pipeline — FAILED",
        "attachments": [{
            "color": "#E01E5A",
            "blocks": [
                {
                    "type": "section",
                    "text": {
                        "type": "mrkdwn",
                        "text": ":red_circle: *Music Streaming Pipeline — FAILED*"
                    }
                },
                {
                    "type": "section",
                    "fields": [
                        {"type": "mrkdwn", "text": f"*Failed Step:*\n`{failedStep}`"},
                        {"type": "mrkdwn", "text": f"*Execution:*\n`{executionId[-12:]}`"},
                        {"type": "mrkdwn", "text": f"*Time:*\n{nowUtc()}"},
                    ]
                },
                {
                    "type": "section",
                    "text": {
                        "type": "mrkdwn",
                        "text": f"*Error:*\n```{str(error)[:MAX_ERROR_CHARS]}```"
                    }
                }
            ]
        }]
    }


def handler(event, context):
    webhookUrl  = resolveWebhookUrl()
    eventType   = event.get("event_type")
    executionId = event.get("execution_id", "unknown")

    if eventType == "started":
        payload = buildStartedPayload(executionId)

    elif eventType == "succeeded":
        payload = buildSucceededPayload(executionId)

    elif eventType == "failed":
        errorObj   = event.get("error", {})
        failedStep = errorObj.get("Error", "Unknown") if isinstance(errorObj, dict) else str(errorObj)
        error      = errorObj.get("Cause", "Unknown error") if isinstance(errorObj, dict) else str(errorObj)
        payload    = buildFailedPayload(executionId, failedStep, error)

    else:
        logger.error(f"Unknown event_type: {eventType}")
        return {"statusCode": 400}

    try:
        postToSlack(webhookUrl, payload)
        return {"statusCode": 200}
    except urllib.error.URLError as error:
        logger.error(f"Slack alert could not be delivered: {error}")
        raise
