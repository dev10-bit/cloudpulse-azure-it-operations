import os
import json
import logging
from datetime import datetime, timezone

import azure.functions as func
from azure.data.tables import TableServiceClient


# Function App (Python v2 programming model)
app = func.FunctionApp(http_auth_level=func.AuthLevel.FUNCTION)


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _require_fields(payload: dict, required: list[str]) -> list[str]:
    return [f for f in required if f not in payload]


def _safe_int(value, field_name: str):
    try:
        return int(value)
    except Exception:
        raise ValueError(f"Field '{field_name}' must be an integer")


@app.route(route="IngestMetrics", methods=["POST"])
def IngestMetrics(req: func.HttpRequest) -> func.HttpResponse:
    logging.info("CloudPulse: IngestMetrics request received")

    # Parse JSON body
    try:
        body = req.get_json()
    except ValueError:
        return func.HttpResponse(
            json.dumps({"success": False, "error": "Invalid JSON body"}),
            status_code=400,
            mimetype="application/json",
        )

    # Validate required fields
    required = [
        "deviceId",
        "timestampUtc",
        "cpuUsage",
        "memoryUsage",
        "diskFreePercent",
        "pingMicrosoftMs",
        "status",
    ]
    missing = _require_fields(body, required)
    if missing:
        return func.HttpResponse(
            json.dumps({"success": False, "error": f"Missing fields: {', '.join(missing)}"}),
            status_code=400,
            mimetype="application/json",
        )

    # Pull Storage connection string from app settings
    conn_str = os.environ.get("CLOUDPULSE_STORAGE")
    if not conn_str:
        logging.error("Missing CLOUDPULSE_STORAGE environment variable")
        return func.HttpResponse(
            json.dumps({"success": False, "error": "Server misconfigured: missing CLOUDPULSE_STORAGE"}),
            status_code=500,
            mimetype="application/json",
        )

    # Normalize values
    device_id = str(body["deviceId"]).strip()
    timestamp_utc = str(body["timestampUtc"]).strip()

    # If timestamp is empty, fallback to now (shouldn't happen if agent is correct)
    if not timestamp_utc:
        timestamp_utc = _utc_now_iso()

    # Convert numeric fields safely
    try:
        cpu = _safe_int(body["cpuUsage"], "cpuUsage")
        mem = _safe_int(body["memoryUsage"], "memoryUsage")
        disk_free = _safe_int(body["diskFreePercent"], "diskFreePercent")
        ping_ms = _safe_int(body["pingMicrosoftMs"], "pingMicrosoftMs")
    except ValueError as e:
        return func.HttpResponse(
            json.dumps({"success": False, "error": str(e)}),
            status_code=400,
            mimetype="application/json",
        )

    status = str(body["status"]).strip()

    # Create RowKey unique per device + time
    # (Using sanitized timestamp for key safety)
    safe_ts = timestamp_utc.replace(":", "").replace("-", "").replace(".", "")
    row_key = f"{safe_ts}_{device_id}"

    entity = {
        "PartitionKey": device_id,
        "RowKey": row_key,
        "timestampUtc": timestamp_utc,
        "cpuUsage": cpu,
        "memoryUsage": mem,
        "diskFreePercent": disk_free,
        "pingMicrosoftMs": ping_ms,
        "status": status,
    }

    # Write to Azure Table Storage
    try:
        table_service = TableServiceClient.from_connection_string(conn_str)
        table_client = table_service.get_table_client("DeviceMetrics")

        # Create table if it doesn't exist (nice for first-run)
        try:
            table_client.create_table()
        except Exception:
            # likely already exists
            pass

        table_client.upsert_entity(entity=entity, mode="merge")
    except Exception as e:
        logging.exception("Failed writing to Table Storage")
        return func.HttpResponse(
            json.dumps({"success": False, "error": f"Storage write failed: {str(e)}"}),
            status_code=500,
            mimetype="application/json",
        )

    return func.HttpResponse(
        json.dumps(
            {
                "success": True,
                "message": "Metric ingested",
                "partitionKey": device_id,
                "rowKey": row_key,
            }
        ),
        status_code=200,
        mimetype="application/json",
    )

        )