"""
pip install maxminddb-geolite2 pandas
"""

import os
from datetime import datetime, timedelta
from urllib.parse import urlparse

import humanize
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
    <div style='text-align: justify;'>
<p><em>Our report includes several key metrics that provide insights into the usage patterns of our services. Here's a brief explanation of these metrics:</em></p>
<p><strong>Sections:</strong></p>
<ul>
<li>Overall: Overall summary of service including all the subsections</li>
<p> <strong> Sub Sections (Sub sections are folders in s3)</strong> </p>
<li>TM:  Tasking Manager exports including production and staging setup</li>
<li>default: Exports generated usually from export tool, FMTM and fAIr through a direct API call</li>
<li>ISO3/HDX: Country exports currently pushed to HDX</li>
</ul>
<p><strong>Key Metrics:</strong></p>
<ul>
<li><strong>Total Overall Interactions Count:</strong> Represents the total number of user actions performed including file downloads  and other metadata queries.</li>
<li><strong>Total Dataset Downloads Count:</strong> Represents the total number of files retrieved (‘GET’ operations) from the service. This is the number of actual downloads performed by a user.</li>
<li><strong>Total Unique files Downloaded:</strong> Represents the total number of unique files that were downloaded from a service. Same file can be downloaded multiple times, so this count comes after deduplication on the download count. </li>
<li><strong>Total  files Uploaded Count:</strong> Answers how many files were generated and updated(in AWS S3) by the raw data API during this period of time. It is possible that new data may not be generated for existing files but user still be downloading old data. This helps to understand if the raw data API is actually generating new files or not</li>
<li><strong>Total  files Downloaded Size:</strong> As the name suggests it’s the aggregate sum of file size downloaded by the users</li>
<li><strong>Total  files Uploaded Size:</strong>Aggregate Sum of file size uploaded or updated by the raw data API to AWS S3</li>
<li><strong>Unique Users:</strong> Indicates the total number of distinct users that have interacted with files through our service, based on unique IP addresses. It is possible to have multiple downloads from the same IP, specially if downloads are being redirected using some server it might record only one server IP</li>
<li><strong>Unique Users by Download:</strong> Above metrics takes into account of all users who interacted with files,  meanwhile this metrics indicates number of unique users who actually downloaded the file</li>

<li><strong>Most Popular Files by Download:</strong>  Listing top 5 files with the highest number of downloads</li>
<li><strong>Top User Locations by Download:</strong> Tries to extract user’s country location (alpha 2 country codes) from requested IP. Unknown means IP couldn’t be located</li>
<li><strong>Top Referrers:</strong> AWS S3 server access logs by defaults try to catch the origin of request. If referrer identity is available in the API call’s request, then it is recorded.  However this referrer information in the header is not available in all the API calls, so this metrics is not accurate and can be used  only for generalization.</li>
</ul>
</div>
"""
    return explanation_section


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
        "total_files_downloads_count": download_df.shape[0],
        "total_unique_files_downloaded": download_df["key"].nunique(),
        "total_dataset_uploaded_count": folder_df[
            folder_df["method"].isin(["PUT", "POST"])
        ]["key"].nunique(),
        "total_dataset_downloaded_size": humanize.naturalsize(
            download_df["objectsize"].sum()
        ),
        "total_dataset_uploaded_size": humanize.naturalsize(
            folder_df[folder_df["method"].isin(["PUT", "POST"])]["objectsize"].sum()
        ),
        "unique_users_overall": folder_df["remoteip"].nunique(),
        "unique_users_by_download": download_df["remoteip"].nunique(),
        "popular_files_by_download": download_df["key"]
        .value_counts()
        .head(5)
        .to_dict(),
        "top_user_locations_by_dowload": download_df["country"]
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
        metrics["top_user_locations_by_dowload"],
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
    table_html = f"<details><summary><h3 style='font-family: Arial, sans-serif;'>{title.upper()}</h3></summary>"
    table_html += "<div style='margin-top: 10px;'>"
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
    table_html += "</div></details>"
    return table_html


def generate_generic_summary(metrics, title="this section"):
    total_interactions = metrics.get("total_overall_interactions_count", 0)
    total_downloads = metrics.get("total_files_downloads_count", 0)
    unique_files = metrics.get("total_unique_files_downloaded", 0)
    unique_users_by_download = metrics.get("unique_users_by_download", 0)
    unique_users = metrics.get("unique_users_overall", 0)
    total_uploads = metrics.get("total_dataset_uploaded_count", 0)
    download_size = metrics.get("total_dataset_downloaded_size", "0 GB")
    upload_size = metrics.get("total_dataset_uploaded_size", "0 GB")

    summary_statement = f"""
    <div style='text-align: justify;'>
    <p>Throughout this period, {title} received <strong>{humanize.intcomma(total_interactions)}</strong> interactions from <strong>{humanize.intcomma(unique_users)}</strong> unique users, including data views, downloads, and metadata queries. Out of {humanize.intcomma(unique_users)} users, a total of <strong>{humanize.intcomma(unique_users_by_download)}</strong> unique users downloaded <strong>{humanize.intcomma(unique_files)}</strong> files <strong>{humanize.intcomma(total_downloads)}</strong> times, amounting to <strong>{download_size}</strong> of data. Moreover, Raw Data API updated <strong>{humanize.intcomma(total_uploads)}</strong> files, adding up to <strong>{upload_size}</strong> of content. More information is tabularized and listed below.</p>
    </div>
    """

    return summary_statement


def generate_full_report_email(df, presigned_url_csv, verbose=True, filename=None):
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
    <p>Dear Colleague,</p>
    <p>Please find the comprehensive Raw Data API usage report for the period spanning from <strong>{timeframe_start}</strong> to <strong>{timeframe_end}</strong>. This report begins with a overall summary of the Raw Data API’s usage, followed by a detailed breakdown by different services that utilises the Raw Data API.</p>
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
        This email ({filename}) is auto-generated by <a href="https://github.com/hotosm/s3-logs-analyzer/" style="color: #666;">s3 logs analyzer</a> and might contain confidential data. You can download the complete CSV logs of all files generated  through raw data API for your custom analysis from <a href='{presigned_url_csv}' style="color: #666;">here</a> and this link auto-expires in 1 week. If you have any other queries, Please reply to this email.
        <br>
    </p>
    </body>
    </html>
    """
    return email_body.strip()
