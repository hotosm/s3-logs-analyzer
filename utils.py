"""
pip install maxminddb-geolite2 pandas
"""

import os
from datetime import datetime, timedelta
from urllib.parse import urlparse

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
        return "N/A"
    if match is not None:
        if "country" in match.keys():
            return match["country"]["iso_code"]
    return "N/A"


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


def _generate_understanding_metrics_section():

    explanation_section = """<h2>Understanding the Metrics:</h2>
<p><em>Our report includes several key metrics that provide insights into the usage patterns of our services. Here's a brief explanation of these metrics:</em></p>
<p><strong>Sections:</strong></p>
<ul>
<li>Overall: Overall summary of service including all the subsections</li>
<p> <strong> Sub Sections (Sub sections are folders in s3)</strong> </p>
<li>TM: Tasking Manager exports</li>
<li>default: Default exports generated usually from export tool / FMTM and fAIr general call</li>
<li>ISO3/HDX: Country exports currently pushed to HDX</li>
</ul>
<p><strong>Key Metrics:</strong></p>
<ul>
<li><strong>Total Overall Interactions Count:</strong> Represents the total number of user actions done in our server including data views , downloads , metadata queries.</li>
<li><strong>Total Dataset Downloads Count:</strong> Represents the total number of files retrieved ('GET' operations) from the service. This is the number of actual downloads performed by user</li>
<li><strong>Total Unique Datasets Downloaded:</strong> Represents the total number of unique datasets that were downloaded. Dataset can be downloaded multiple times which will be included in above metrics meanwhile this is number of unique datasets only</li>
<li><strong>Total  Datasets Uploaded Count:</strong> Answers how many datasets were uploaded / updated by the service (in s3) in this period of time. It is possible that data may not be generated but user still be downloading old data. This helps to understands either service is actually updating/uploading the datasets or not</li>
<li><strong>Total  Datasets Downloaded Size:</strong> As name suggests its the total sum size of downloaded files from user</li>
<li><strong>Total  Datasets Uploaded Size:</strong>Sum of dataset size uploaded/updated by the service to s3</li>
<li><strong>Unique Users:</strong> Indicates the total number of distinct users that have interacted with our service, based on unique IP addresses. It is possible to have multiple downloads from same ip, Specially if downloads are being redirected using some server it might record only one ip of server</li>
<li><strong>Unique Users by Download:</strong> Above metrics takes into account of all users meanwhile this metrics only represents number of unique users who were enaged in download activity</li>

<li><strong>Most Popular Files by Download:</strong> Highlights the files with the highest number of downloads</li>
<li><strong>Popular Location Hotspot by Download:</strong> Tries to extract user country location (alpha 2) from requested ip. Unknow means ip couldn't be located</li>
<li><strong>Top Referrers:</strong> s3 server access logs by defaults try to catch the referrers. If referer is passed on header while calling the API then it would be recorded but not necessarily. This might not be the case all the time, so this metrics won't be accurate and can only be used for generalization</li>
</ul>
<p><strong>Note: Interactions </strong> refer to any action taken by users related to a file, including but not limited to downloading. For eg : User only listing the resource or fetching the file size etc</p>
"""
    return explanation_section


def format_size(size_bytes):
    if size_bytes == 0:
        return "0 MB"
    elif size_bytes < 1024:
        return "less than 1 MB"
    elif size_bytes < 1024**2:
        return f"{size_bytes / 1024:.0f} MB"
    else:
        return f"{size_bytes / (1024**3):.2f} GB"


def extract_key_components(df):
    df["project"] = df["key"].apply(
        lambda x: x.split("/")[1] if len(x.split("/")) > 1 else "NA"
    )
    df["feature"] = df["key"].apply(
        lambda x: x.split("/")[2] if len(x.split("/")) > 2 else "NA"
    )
    df["fileformat"] = df["key"].apply(
        lambda x: (
            "Other"
            if "/" in (x.split("_")[-1].split(".")[0])
            else (x.split("_")[-1].split(".")[0] if "_" in x else "NA")
        )
    )
    return df


def analyze_metrics(df, folder_name=None, enable_interaction_metrics=False):
    if folder_name:
        folder_df = df[df["top_level_key"] == folder_name].copy()
        if folder_name not in ["default", "athena"]:
            folder_df = extract_key_components(folder_df)
    else:
        folder_df = df.copy()

    interaction_df = folder_df[~folder_df["method"].isin(["POST", "PUT", "DELETE"])]
    download_df = folder_df[folder_df["method"] == "GET"]

    metrics = {
        "total_overall_interactions_count": interaction_df.shape[0],
        "total_datasets_downloads_count": download_df.shape[0],
        "total_unique_datasets_downloaded": download_df["key"].nunique(),
        "total_dataset_uploaded_count": folder_df[
            folder_df["method"].isin(["PUT", "POST"])
        ]["key"].nunique(),
        "total_dataset_downloaded_size": format_size(download_df["objectsize"].sum()),
        "total_dataset_uploaded_size": format_size(
            folder_df[folder_df["method"].isin(["PUT", "POST"])]["objectsize"].sum()
        ),
        "unique_users_overall": folder_df["remoteip"].nunique(),
        "unique_users_by_download": download_df["remoteip"].nunique(),
        "popular_files_by_download": download_df["key"]
        .value_counts()
        .head(5)
        .to_dict(),
        "popular_locations_by_download": download_df["country"]
        .value_counts()
        .head(5)
        .to_dict(),
        "top_referrers_by_download": download_df["referrer"]
        .value_counts()
        .head(5)
        .to_dict(),
    }

    def add_if_different(metric_key, download_metric, interaction_metric):
        if enable_interaction_metrics:
            if download_metric != interaction_metric:
                metrics[metric_key] = interaction_metric

    add_if_different(
        "popular_files_by_interaction",
        metrics["popular_files_by_download"],
        interaction_df["key"].value_counts().head(5).to_dict(),
    )
    add_if_different(
        "popular_locations_by_interaction",
        metrics["popular_locations_by_download"],
        interaction_df["country"].value_counts().head(5).to_dict(),
    )
    add_if_different(
        "top_referrers_by_interaction",
        metrics["top_referrers_by_download"],
        interaction_df["referrer"].value_counts().head(5).to_dict(),
    )

    if folder_name and folder_name not in ["default", "athena"]:
        project_downloads = download_df["project"].value_counts().head(5).to_dict()
        feature_downloads = download_df["feature"].value_counts().head(5).to_dict()
        fileformat_downloads = (
            download_df["fileformat"].value_counts().head(5).to_dict()
        )

        project_interactions = (
            interaction_df["project"].value_counts().head(5).to_dict()
        )
        feature_interactions = (
            interaction_df["feature"].value_counts().head(5).to_dict()
        )
        fileformat_interactions = (
            interaction_df["fileformat"].value_counts().head(5).to_dict()
        )

        add_if_different(
            "popular_projects_by_interaction", project_downloads, project_interactions
        )
        add_if_different(
            "popular_features_by_interaction", feature_downloads, feature_interactions
        )
        add_if_different(
            "popular_fileformats_by_interaction",
            fileformat_downloads,
            fileformat_interactions,
        )

        # compulsory one
        metrics.update(
            {
                "popular_projects_by_download": project_downloads,
                "popular_features_by_download": feature_downloads,
                "popular_fileformats_by_download": fileformat_downloads,
            }
        )

    return metrics


def metrics_to_html_table(metrics, title="Metrics"):
    table_html = f"<h3 style='font-family: Arial, sans-serif;'>{title.upper()}</h3>"
    table_html += generate_generic_summary(metrics, title)
    table_html += "<table style='border-collapse: collapse; width: 100%; margin-top: 20px; margin-bottom: 40px;'>"
    table_html += "<tr><th style='border: 1px solid #ddd; padding: 12px 15px; text-align: left; background-color: #D73F3F; color: #ffffff; font-size: 16px;'>Metric</th><th style='border: 1px solid #ddd; padding: 12px 15px; text-align: left; background-color: #D73F3F; color: #ffffff; font-size: 16px;'>Value</th></tr>"
    for key, value in metrics.items():
        if isinstance(value, dict):  # nested dictionaries -> separate sections
            table_html += f"<tr><td colspan='2' style='background-color: #f2f2f2; padding: 12px 15px; font-weight: bold;'>{key.replace('_', ' ').title()}</td></tr>"
            for sub_key, sub_value in value.items():
                table_html += f"<tr><td style='padding-left: 25px; font-style: italic; color: #555; border: 1px solid #ddd; padding: 8px 15px;'>{sub_key}</td><td style='border: 1px solid #ddd; padding: 8px 15px;'>{sub_value}</td></tr>"
        else:
            table_html += f"<tr><td style='font-weight: bold; border: 1px solid #ddd; padding: 12px 15px;'>{key.replace('_', ' ').title()}</td><td style='border: 1px solid #ddd; padding: 12px 15px;'>{value}</td></tr>"
    table_html += "</table>"
    return table_html


def generate_generic_summary(metrics, title="this section"):
    total_interactions = metrics.get("total_overall_interactions_count", 0)
    total_downloads = metrics.get("total_datasets_downloads_count", 0)
    unique_datasets = metrics.get("total_unique_datasets_downloaded", 0)
    unique_users_by_download = metrics.get("unique_users_by_download", 0)
    unique_users = metrics.get("unique_users_overall", 0)
    total_uploads = metrics.get("total_dataset_uploaded_count", 0)
    download_size = metrics.get("total_dataset_downloaded_size", "0 GB")
    upload_size = metrics.get("total_dataset_uploaded_size", "0 GB")

    summary_statement = f"""
    <p>Throughout this period, {title} received <strong>{total_interactions}</strong> interactions from <strong>{unique_users}</strong> users, including data views, downloads, and metadata queries.</p><p> Out of {unique_users} total of <strong>{unique_users_by_download}</strong> users downloaded <strong>{unique_datasets}</strong> datasets <strong>{total_downloads}</strong> times, amounting to <strong>{download_size}</strong> of data. Moreover, Raw Data API uploaded/updated <strong>{total_uploads}</strong> datasets, adding up to <strong>{upload_size}</strong> of content. More information is tabularized and listed below.</p>
    """

    return summary_statement


def generate_full_report_email(df, presigned_url_csv, verbose=True):
    if not isinstance(df, pd.DataFrame):
        df = df.to_pandas()

    # prepare df
    df["requestdatetime"] = pd.to_datetime(
        df["requestdatetime"], format="%d/%b/%Y:%H:%M:%S %z"
    )
    df["objectsize"] = pd.to_numeric(df["objectsize"], errors="coerce").fillna(0)
    df["method"] = df["operation"].apply(lambda x: x.split(".")[1] if "." in x else x)
    df["top_level_key"] = df["key"].apply(lambda x: x.split("/")[0])
    df["referrer"] = df["referrer"].apply(
        lambda url: (
            urlparse(str(url).strip('"')).netloc
            if urlparse(str(url).strip('"')).netloc
            else "Direct or N/A"
        )
    )
    df["country"] = df["remoteip"].apply(get_country_from_ip)

    timeframe_start = df["requestdatetime"].min().strftime("%B %d, %Y")
    timeframe_end = df["requestdatetime"].max().strftime("%B %d, %Y")

    email_body = f"""
    <html>
    <head>
    </head>
    <body>
    <p>Dear Stakeholder,</p>
    <p>Please find the comprehensive S3 Logs Summary Report for the period spanning <strong>{timeframe_start}</strong> to <strong>{timeframe_end}</strong>. This report begins with a overall summary of the service's performance, followed by detailed report in subsequent sections. For your convenience, explanations of the metrics utilized are provided at the end of the document. Additionally, a hyperlink for directly downloading the metadata CSV is available in the report's footer.</p>
    """
    overall_metrics = analyze_metrics(df)
    if verbose:
        print(overall_metrics)

    email_body += metrics_to_html_table(overall_metrics, "Raw Data API")

    for folder in df["top_level_key"].unique():
        if folder.startswith("log") or folder in ["athena"]:
            continue
        folder_metrics = analyze_metrics(df, folder)
        if verbose:
            print(folder_metrics)
        email_body += metrics_to_html_table(folder_metrics, f"section: {folder}")

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
