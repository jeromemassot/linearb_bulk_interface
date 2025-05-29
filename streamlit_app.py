import streamlit as st
import pandas as pd
import httpx
import json

from typing import Any, Optional, List, Dict

## -- Metrics definition

## Load resource and metadata
with open("metrics.json") as fp:
    metrics_list = json.load(fp)
    for m in metrics_list:
        m["selected"] = True
    metrics_df = pd.DataFrame(metrics_list)


## -- LinearB endpoints and API key

LINEARB_API_BASE = "https://public-api.linearb.io"
ENDPOINT = "/api/v2/measurements/export"
api_token = st.secrets["api_token"]


## -- REST API engine

def make_linearb_request(
    method: str,
    endpoint: str,
    params: Optional[Dict[str, Any]] = None,
    json_data: Optional[Dict[str, Any] | List[Any]] = None,
    timeout: float = 30.0,
) -> Any:
    """
    Makes an asynchronous request to the LinearB API.

    Args:
        method: HTTP method (GET, POST, PATCH, DELETE).
        endpoint: API endpoint path (e.g., /api/v1/deployments).
        api_key: The LinearB API Token.
        params: Dictionary of query parameters.
        json_data: Dictionary or List for the JSON request body.
        timeout: Request timeout in seconds.

    Returns:
        Parsed JSON response (dict or list) on success,
        A success message string for 204 No Content,
        An error dictionary { "error": "message", "status_code": code } on failure.
    """
    if not api_token:
        return {"error": "LinearB API Key is required.", "status_code": 401}

    headers = {
        "Content-Type": "application/json",
        "Accept": "application/json",
        "x-api-key": api_token
    }
    
    url = f"{LINEARB_API_BASE}{endpoint}"

    # Filter out None values from params and json_data
    if params:
        params = {k: v for k, v in params.items() if v is not None}
    if isinstance(json_data, dict):
         json_data = {k: v for k, v in json_data.items() if v is not None}
    # Lists in json_data are passed as is

    try:
        response = httpx.request(
            method, url, headers=headers, params=params, json=json_data, timeout=timeout
        )
            
        if 200 <= response.status_code < 300:
            if response.status_code == 204: # No Content
                return {"message": "Operation successful (No Content).", "status_code": 204}
            try:
                # Handle empty response body which might cause JSON decode error
                if response.content:
                    return response.json()
                else:
                    return {"message": f"Operation successful (Status: {response.status_code}).", "status_code": response.status_code}
            except json.JSONDecodeError:
                return {"error": "Failed to decode JSON response.", "status_code": response.status_code, "response_text": response.text}
        else:
            try:
                # Try to parse error details if available
                error_details = response.json()
            except json.JSONDecodeError:
                error_details = response.text # Fallback to raw text
            return {
                "error": f"API request failed.",
                "status_code": response.status_code,
                "details": error_details
            }
    except httpx.RequestError as e:
        return {"error": f"HTTP request error: {e.__class__.__name__}", "details": str(e)}
    except Exception as e:
        return {"error": f"An unexpected error occurred: {e.__class__.__name__}", "details": str(e)}


## -- Helper functions

def reformat_filters(filters: str):
    if not filters or len(filters) == 0:
        return None
    filters = filters.split(" ")
    filters = [f.strip() for f in filters]
    return filters

## -- Streamlit Application UI


st.title("LinearB Bulked Data Retrieval")

st.markdown("This application retrieves Metrics data from LinearB.")


st.header("Metrics")

st.markdown("Select the metrics that you want to export from LinearB")

with st.expander("Metrics", expanded=True):
    edited_metrics_df  = st.data_editor(metrics_df, hide_index=True, use_container_width=True)

with st.form("form"):

    st.header("Filters")

    st.markdown("Enter the filters that you want to use, separated by white space")

    st.warning("""Restrictions: Filtering by labels is restricted to 3 PR labels.""")

    with st.expander("Filters", expanded=True):
        contributor_ids = st.text_input("contributor ids")
        team_ids = st.text_input("team ids", help="At most 50 teams")
        repository_ids = st.text_input("repository ids", help="At most 10 repositories")
        service_ids = st.text_input("service id")
        labels = st.text_input("labels")

    st.header("Group by/Aggregations/Limit")

    st.warning("""Restrictions: If the grouping is based on labels, the results will 
    be grouped according to the specified labels. For other group by values, the 
    results will be filtered based on the provided labels""")

    with st.expander("Aggregations", expanded=True):
        group_by = st.selectbox("Group by", options=["contributor", "label", "organization", "repository", "team"])
        roll_up = st.selectbox("Roll-up", help="1d (one day), 1w (one week), 1m (one month)", options=["1d", "1w", "1m", "custom"])
        limit = st.number_input("Max amount of objects in the response", value=1, help="When request for multiple contributors, teams or repositories, limit should be more or equal of amount of passed ids (or data will be returned partially)")

    st.header("Time Ranges")

    st.markdown("""Specify the time ranges for pulling measurements to run the 
    report requires detailing each time range, whether it's a single range or 
    multiple ranges. Ensure inclusion of both "before" and "after" parameters.
    """)

    with st.expander("Time Ranges", expanded=True):
        before = st.date_input("Before")
        after = st.date_input("After")

    extract = st.form_submit_button("Extract Data")

if extract:

    # create the list of metrics
    requested_metrics = []
    for index, row in edited_metrics_df.iterrows():
        if row["selected"]:
            if type(row["value"]) != float:
                for v in row["value"]:
                    requested_metrics.append({
                        "name": row["name"],
                        "agg": v
                    })
            else:
                requested_metrics.append({
                    "name": row["name"]
                })

    # filters
    filters = {
        n: f for n, f in zip(
            ["contributor_ids", "team_ids", "repository_ids", "service_ids", "labels"],
            [
                reformat_filters(contributor_ids), 
                reformat_filters(team_ids), 
                reformat_filters(repository_ids), 
                reformat_filters(service_ids), 
                reformat_filters(labels)
            ]
        ) if f
    }

    # aggregations
    aggregations = {
        n: a for n, a in zip(
            ["group_by", "roll_up", "limit", "service_ids"],
            [group_by, roll_up, limit, service_ids]
        ) if a
    }

    payload = {
        "requested_metrics": requested_metrics,
        "time_ranges": [
            {
                "after": after.strftime("%Y-%m-%d"),
                "before": before.strftime("%Y-%m-%d")
            }
        ]
    }

    payload.update(filters)
    payload.update(aggregations)

    with open("payload.json", "w") as fp:
        json.dump(payload, fp, indent=2)

    with st.expander("Payload", expanded=False):
        st.json(payload)

    response = make_linearb_request(
        method="POST",
        endpoint=ENDPOINT,
        json_data=payload
    )

    if "error" in response:
        st.error(response["error"])

    st.link_button("Download data", url=response['report_url'])
