"""
pip install maxminddb-geolite2 pandas
"""

import os
from datetime import datetime, timedelta

import pandas as pd
from geolite2 import geolite2


def _parse_date(date_str):
    return datetime.strptime(date_str, "%Y-%m-%d").date()


def _get_current_quarter(date):
    quarter = ((date.month - 1) // 3) + 1
    return quarter


def calculate_date_ranges(frequency=None, start_date=None, end_date=None):
    today = datetime.now().date()

    if start_date and end_date:
        start_date_obj = _parse_date(start_date)
        end_date_obj = _parse_date(end_date)
        #  in 'yyyy/MM/dd'for athena partition
        return start_date_obj.strftime("%Y/%m/%d"), end_date_obj.strftime("%Y/%m/%d")

    if frequency == "weekly":
        start_of_this_week = today - timedelta(days=today.weekday())
        start_date = (start_of_this_week - timedelta(days=7)).strftime(
            "%Y/%m/%d"
        )  # Monday of the previous week
        end_date = (start_of_this_week - timedelta(days=1)).strftime(
            "%Y/%m/%d"
        )  # Sunday of the previous week
    elif frequency == "monthly":
        first_day_of_this_month = today.replace(day=1)
        last_day_of_previous_month = first_day_of_this_month - timedelta(days=1)
        start_date = last_day_of_previous_month.replace(day=1).strftime("%Y/%m/%d")
        end_date = last_day_of_previous_month.strftime("%Y/%m/%d")
    elif frequency == "quarterly":
        current_quarter = _get_current_quarter(today)
        first_month_of_current_quarter = (current_quarter - 1) * 3 + 1
        first_day_of_current_quarter = today.replace(
            month=first_month_of_current_quarter, day=1
        )
        last_day_of_previous_quarter = first_day_of_current_quarter - timedelta(days=1)
        start_month_of_previous_quarter = (
            (last_day_of_previous_quarter.month - 1) // 3
        ) * 3 + 1
        start_date = last_day_of_previous_quarter.replace(
            month=start_month_of_previous_quarter, day=1
        ).strftime("%Y/%m/%d")
        end_date = last_day_of_previous_quarter.strftime("%Y/%m/%d")

    return start_date, end_date


reader = geolite2.reader()


def get_country_from_ip(ip):
    try:
        match = reader.get(ip)
    except Exception as ex:
        return "Unknown"
    if match is not None:
        if "country" in match.keys():
            return match["country"]["iso_code"]
    return "Unknown"


def check_env_vars(enable_email=False):

    required_vars = [
        "S3_LOGS_LOCATION",
        "ATHENA_DATABASE",
        "ATHENA_TABLE",
        "RESULT_PATH",
    ]
    if enable_email:
        required_vars.extend(
            [
                "TARGET_EMAIL_ADDRESS",
                "SMTP_HOST",
                "SMTP_USERNAME",
                "SMTP_PASSWORD",
                "FROM_EMAIL",
                "REPLY_TO_EMAIL",
            ]
        )
    missing_vars = [var for var in required_vars if var not in os.environ]
    if missing_vars:
        raise ValueError("Missing environment variables: " + ", ".join(missing_vars))


def _get_email_html_head():
    head = """<head>
<style>
  table {{
    width: 100%;
    border-collapse: collapse;
  }}
  th, td {{
    border: 1px solid #ddd;
    padding: 8px;
  }}
  th {{
    text-align: left;
    background-color: #f2f2f2;
  }}
</style>
</head>"""
    return head


def _generate_understanding_metrics_section():

    explanation_section = """<h2>Understanding the Metrics:</h2>
<p><em>Our report includes several key metrics that provide insights into the usage patterns of our services. Here's a brief explanation of these metrics:</em></p>
<p><strong>Folders:</strong></p>
<ul>
<li>TM: Tasking Manager exports</li>
<li>default: Default exports generated usually from export tool / FMTM and fAIr general call</li>
<li>ISO3/HDX: Country exports currently pushed to HDX</li>
</ul>
<p><strong>Key Metrics:</strong></p>
<ul>
<li><strong>Total Downloads:</strong> Represents the total number of files retrieved ('GET' operations) from the service. This metric helps in understanding the demand for data stored in our service.</li>
<li><strong>Total Uploads/Updates:</strong> Counts the total number of files uploaded or modified ('PUT'/ 'POST' operations) . This metric gives an idea about how much new data is being generated from our service.</li>
<li><strong>Total Downloaded/Uploaded Data:</strong> Measures the total amount of data transferred during download and upload operations. It's a key indicator of the bandwidth utilized by these activities.</li>
<li><strong>Unique Users:</strong> Indicates the total number of distinct users that have interacted with our service, based on unique IP addresses. This helps in understanding the reach of our service. It is possible to have multiple downloads from same ip, Specially if downloads are being redirected using some server it might record only one ip of server</li>
<li><strong>Total User Interactions:</strong> The total number of interactions users have had with the service, excluding data upload/modification actions. This provides insight into how actively users are engaging with the service.</li>
<li><strong>Most Popular Files by Interactions:</strong> Highlights the files with the highest number of accesses, indicating user interest or demand for specific data.</li>
<li><strong>Popular location hotspot:</strong> Tries to extract user country location from requested ip, This may not be accurate and can be used to generalize request location. Unknown means ip couldn't be located</li>
</ul>
<p><strong>Note:</strong> Interactions refer to any action taken by users related to a file, including but not limited to downloading. For eg : User only listing the resource or fetching the file size etc</p>
"""
    return explanation_section


def generate_full_report_email(df, presigned_url_csv, verbose=True):
    if not isinstance(df, pd.DataFrame):
        df = df.to_pandas()
    df["requestdatetime"] = pd.to_datetime(
        df["requestdatetime"], format="%d/%b/%Y:%H:%M:%S %z"
    )
    df["objectsize"] = pd.to_numeric(df["objectsize"], errors="coerce").fillna(0)
    df["method"] = df["operation"].apply(lambda x: x.split(".")[1] if "." in x else x)
    df["top_level_key"] = df["key"].apply(lambda x: x.split("/")[0])
    df["top_level_key"] = df["top_level_key"].replace("-", "default")

    unique_users = df["remoteip"].nunique()
    if verbose:
        print("Fetching country information from ip")
    df["country"] = df["remoteip"].apply(get_country_from_ip)
    if verbose:
        print("Country info fetched")
    total_user_interactions = df[~df["method"].isin(["PUT", "POST", "DELETE"])].shape[0]

    total_downloads = df[df["method"] == "GET"]["objectsize"].count()
    total_uploads = df[df["method"] == "PUT"]["objectsize"].count()
    total_download_size_bytes = df[df["method"] == "GET"]["objectsize"].sum()
    total_upload_size_bytes = df[df["method"] == "PUT"]["objectsize"].sum()
    timeframe_start = df["requestdatetime"].min().strftime("%B %d, %Y")
    timeframe_end = df["requestdatetime"].max().strftime("%B %d, %Y")

    def format_size(size_bytes):
        if size_bytes == 0:
            return "0 MB"
        elif size_bytes < 1024:
            return "less than 1 MB"
        elif size_bytes < 1024**2:
            return f"{size_bytes / 1024:.0f} MB"
        else:
            return f"{size_bytes / (1024**3):.2f} GB"

    email_body = f"""
<html>
{_get_email_html_head()}
<body>
<p>Dear Stakeholder,</p>
<p>Please find below the comprehensive S3 Logs Summary Report covering the period from <strong>{timeframe_start}</strong> to <strong>{timeframe_end}</strong>.</p>
<h2>Overall Summary for the Service:</h2>
<ul>
  <li>Total Downloads: {total_downloads}</li>
  <li>Total Uploads/Updates: {total_uploads}</li>
  <li>Total Downloaded Data: {format_size(total_download_size_bytes)}</li>
  <li>Total Uploaded/Updated Data: {format_size(total_upload_size_bytes)}</li>
  <li>Unique Users: {unique_users}</li>
  <li>Total User Interactions: {total_user_interactions}</li>
</ul>
"""

    for folder in df["top_level_key"].unique():
        if folder.startswith("log"):
            continue

        folder_df = df[df["top_level_key"] == folder]
        downloads = folder_df[folder_df["method"] == "GET"]["objectsize"].count()
        uploads = folder_df[folder_df["method"] == "PUT"]["objectsize"].count()
        download_size_bytes = folder_df[folder_df["method"] == "GET"][
            "objectsize"
        ].sum()
        upload_size_bytes = folder_df[folder_df["method"] == "PUT"]["objectsize"].sum()

        unique_users_folder = folder_df["remoteip"].nunique()
        total_interactions_folder = folder_df[
            ~folder_df["method"].isin(["PUT", "POST", "DELETE"])
        ].shape[0]

        popular_files = (
            folder_df[~folder_df["method"].isin(["PUT", "POST", "DELETE"])]["key"]
            .value_counts()
            .head(5)
        )
        country_counts = (
            folder_df[~folder_df["method"].isin(["PUT", "POST", "DELETE"])]["country"]
            .value_counts()
            .head(5)
        )
        email_body += f"""
<h3>Folder: {folder}</h3>
<ul>
  <li>Total Downloads: {downloads}</li>
  <li>Total Uploads/Updates: {uploads}</li>
  <li>Total Downloaded Data: {format_size(download_size_bytes)}</li>
  <li>Total Uploaded/Updated Data: {format_size(upload_size_bytes)}</li>
  <li>Unique Users: {unique_users_folder}</li>
  <li>Total User Interactions: {total_interactions_folder}</li>
</ul>
<p><strong>Most Popular Files by Interactions:</strong></p>
<ul>
"""
        for file, count in popular_files.items():
            email_body += f"   <li>{file}: {count} times</li>\n"
        email_body += "</ul>"
        email_body += (
            "<p><strong>Popular location hotspots by interactions</strong></p><ul>"
        )
        for country, count in country_counts.items():
            email_body += f"   <li>{country.upper()}: {count} times</li>\n"
        email_body += "</ul>"

        if folder in ["TM", "ISO3"]:
            # Filter folder_df to exclude PUT, POST, and DELETE methods
            filtered_df = folder_df.loc[
                ~folder_df["method"].isin(["PUT", "POST", "DELETE"]), :
            ].copy()

            filtered_df["project"] = filtered_df["key"].apply(
                lambda x: x.split("/")[1] if len(x.split("/")) > 1 else "NA"
            )
            filtered_df["feature"] = filtered_df["key"].apply(
                lambda x: x.split("/")[2] if len(x.split("/")) > 2 else "NA"
            )
            filtered_df["fileformat"] = filtered_df["key"].apply(
                lambda x: (
                    "Other"
                    if "/" in (x.split("_")[-1].split(".")[0])
                    else (x.split("_")[-1].split(".")[0] if "_" in x else "NA")
                )
            )

            project_counts = filtered_df["project"].value_counts().head(5)
            feature_counts = filtered_df["feature"].value_counts().head(5)
            file_format_counts = filtered_df["fileformat"].value_counts().head(5)
            country_counts = filtered_df["country"].value_counts().head(5)

            additional_stats = f"""
        <p><strong>Additional Stats for {folder}:</strong></p>
        <p><strong>Most Popular Projects:</strong></p>
        <ul>
        """
            for project, count in project_counts.items():
                additional_stats += f"   <li>{project}: {count} times</li>\n"
            additional_stats += "</ul>"

            additional_stats += "<p><strong>Most Popular Features:</strong></p><ul>"
            for feature, count in feature_counts.items():
                additional_stats += f"   <li>{feature}: {count} times</li>\n"
            additional_stats += "</ul>"

            additional_stats += "<p><strong>Most Popular File Formats:</strong></p><ul>"
            for format, count in file_format_counts.items():
                additional_stats += f"   <li>{format.upper()}: {count} times</li>\n"
            additional_stats += "</ul>"

            email_body += additional_stats

    email_body += _generate_understanding_metrics_section()

    email_body += f"""
<hr style="border: 1px solid #ccc; margin-top: 20px;">
<p style="font-size: 0.8em; color: #666;">
    This email is auto-generated by <a href="https://github.com/hotosm/s3-logs-analyzer/" style="color: #666;">s3 logs analyzer</a> and might contain confidential data. You can download the full report meta CSV for your custom analysis from the attached link bearing in mind the link expires in 1 week. If you have any other queries, please contact the administrator or reply to this email.
    <br>
    <a href="{presigned_url_csv}" style="color: #666;">Download Full Data</a>
</p>

</body>
</html>
"""
    return email_body.strip()
