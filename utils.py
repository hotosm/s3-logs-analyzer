import os

import pandas as pd


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


def generate_full_report_email(df, presigned_url_csv):
    if not isinstance(df, pd.DataFrame):
        df = df.to_pandas()

    df["requestdatetime"] = pd.to_datetime(
        df["requestdatetime"], format="%d/%b/%Y:%H:%M:%S %z"
    )
    df["objectsize"] = pd.to_numeric(df["objectsize"], errors="coerce").fillna(0)
    df["method"] = df["operation"].apply(lambda x: x.split(".")[1] if "." in x else x)
    df["top_level_key"] = df["key"].apply(lambda x: x.split("/")[0])
    df["top_level_key"] = df["top_level_key"].replace("-", "default")

    total_downloads = df[df["method"] == "GET"]["objectsize"].count()
    total_uploads = df[df["method"] == "PUT"]["objectsize"].count()
    total_download_size_bytes = df[df["method"] == "GET"]["objectsize"].sum()
    total_upload_size_bytes = df[df["method"] == "PUT"]["objectsize"].sum()
    timeframe_start = df["requestdatetime"].min().strftime("%B %d, %Y")
    timeframe_end = df["requestdatetime"].max().strftime("%B %d, %Y")

    def format_size(size_bytes):
        if size_bytes < 1024:
            return "less than 1 MB"
        elif size_bytes < 1024**2:
            return f"{size_bytes / 1024:.0f} MB"
        else:
            return f"{size_bytes / (1024**3):.0f} GB"

    email_body = f"""
<html>
<head>
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
</head>
<body>
<p>Dear Stakeholder,</p>
<p>Please find below the comprehensive S3 Logs Summary Report covering the period from <strong>{timeframe_start}</strong> to <strong>{timeframe_end}</strong>.</p>
<h2>Overall Summary for the Service:</h2>
<ul>
  <li>Total Downloads: {total_downloads}</li>
  <li>Total Uploads/Updates: {total_uploads}</li>
  <li>Total Download Transferred: {format_size(total_download_size_bytes)}</li>
  <li>Total Upload/Update Transferred: {format_size(total_upload_size_bytes)}</li>
</ul>
<h2>Detailed Folder Statistics:</h2>
<p><em>Folder Explanation:</em><br>
- TM: Tasking Manager exports<br>
- default: Default exports generated usually from export tool / FMTM and fAIr general call<br>
- ISO3: Country exports currently pushed to HDX</p>
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

        popular_files = folder_df["key"].value_counts().head(5)
        email_body += f"""
<h3>Folder: {folder}</h3>
<ul>
  <li>Total Downloads: {downloads}</li>
  <li>Total Uploads/Updates: {uploads}</li>
  <li>Total Download Transferred: {format_size(download_size_bytes)}</li>
  <li>Total Upload/Update Transferred: {format_size(upload_size_bytes)}</li>
</ul>
<p><strong>Most Popular Files:</strong></p>
<ul>
"""
        for file, count in popular_files.items():
            email_body += f"   <li>{file}: {count} times</li>\n"
        email_body += "</ul>"

        if folder in ["TM", "ISO3"]:
            folder_df["project"] = folder_df["key"].apply(
                lambda x: x.split("/")[1] if len(x.split("/")) > 1 else "NA"
            )
            folder_df["feature"] = folder_df["key"].apply(
                lambda x: x.split("/")[2] if len(x.split("/")) > 2 else "NA"
            )
            folder_df["fileformat"] = folder_df["key"].apply(
                lambda x: (
                    "Other"
                    if "/" in (x.split("_")[-1].split(".")[0])
                    else (x.split("_")[-1].split(".")[0] if "_" in x else "NA")
                )
            )

            project_counts = folder_df["project"].value_counts().head(5)
            feature_counts = folder_df["feature"].value_counts().head(5)
            file_format_counts = folder_df["fileformat"].value_counts().head(5)

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

    email_body += f"""
    <p>
    Download the full report meta CSV for your custom analysis from the attached link.
    Note: This link expires in 1 week so if you have any other queries, please contact the administrator or reply to this email.
    <br>
    <a href="{presigned_url_csv}">Download Full Report</a>
</p>
</body>
</html>
"""
    return email_body.strip()
