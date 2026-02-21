"""
S3 Monitoring DAG.

This DAG monitors S3 storage for data quality, file integrity,
and storage optimization.
"""

from __future__ import annotations

import logging
import pendulum

from airflow.decorators import dag, task
from airflow.operators.python import PythonOperator
from airflow.providers.slack.operators.slack_webhook import SlackWebhookOperator

from src.utils.s3_utils import list_s3_objects, check_s3_connection

LOG = logging.getLogger(__name__)


@dag(
    dag_id="s3_monitoring",
    start_date=pendulum.datetime(2024, 1, 1, tz="UTC"),
    schedule="@daily",
    catchup=False,
    tags=["monitoring", "s3", "maintenance"],
    description="Monitor S3 storage for data quality and optimization",
)
def s3_monitoring_dag():
    """
    Monitor S3 storage and perform maintenance tasks.
    
    This DAG:
    - Checks S3 connection and accessibility
    - Monitors file sizes and counts
    - Identifies potential data quality issues
    - Sends notifications for anomalies
    """
    
    @task
    def check_s3_connectivity() -> bool:
        """
        Check if S3 is accessible.
        
        Returns:
            True if connection successful, False otherwise
        """
        return check_s3_connection()
    
    @task
    def analyze_s3_storage() -> dict:
        """
        Analyze S3 storage usage and file patterns.
        
        Returns:
            Dictionary with storage analysis results
        """
        import pandas as pd
        from src.config.settings import get_settings
        
        settings = get_settings()
        
        # List objects in different layers
        raw_objects = list_s3_objects(prefix=settings.s3_raw_layer)
        curated_objects = list_s3_objects(prefix=settings.s3_curated_layer)
        
        analysis = {
            "raw_layer": {
                "file_count": len(raw_objects),
                "files": raw_objects,
            },
            "curated_layer": {
                "file_count": len(curated_objects),
                "files": curated_objects,
            },
            "total_files": len(raw_objects) + len(curated_objects),
        }
        
        # Analyze file patterns
        def analyze_file_patterns(objects):
            if not objects:
                return {"extensions": {}, "date_patterns": {}}
            
            extensions = {}
            date_patterns = {}
            
            for obj in objects:
                # Extract file extension
                if "." in obj:
                    ext = obj.split(".")[-1].lower()
                    extensions[ext] = extensions.get(ext, 0) + 1
                
                # Extract date patterns (simplified)
                parts = obj.split("/")
                for part in parts:
                    if part.replace("-", "").replace("_", "").isdigit():
                        date_patterns[len(part)] = date_patterns.get(len(part), 0) + 1
            
            return {"extensions": extensions, "date_patterns": date_patterns}
        
        analysis["raw_layer"]["patterns"] = analyze_file_patterns(raw_objects)
        analysis["curated_layer"]["patterns"] = analyze_file_patterns(curated_objects)
        
        LOG.info(f"S3 storage analysis completed: {analysis['total_files']} total files")
        return analysis
    
    @task
    def check_data_freshness(storage_analysis: dict) -> dict:
        """
        Check data freshness based on file timestamps.
        
        Args:
            storage_analysis: Storage analysis results
            
        Returns:
            Dictionary with freshness check results
        """
        import re
        from datetime import datetime, timedelta
        from src.utils.s3_utils import get_s3_client
        
        freshness_checks = {
            "stale_files": [],
            "recent_files": [],
            "oldest_file": None,
            "newest_file": None,
        }
        
        try:
            s3_client = get_s3_client()
            settings = get_settings()
            
            all_files = (
                storage_analysis["raw_layer"]["files"] + 
                storage_analysis["curated_layer"]["files"]
            )
            
            cutoff_date = datetime.now() - timedelta(days=7)
            
            for file_key in all_files[:50]:  # Limit to first 50 files for demo
                try:
                    response = s3_client.head_object(
                        Bucket=settings.s3_bucket_name,
                        Key=file_key
                    )
                    
                    last_modified = response["LastModified"]
                    file_size = response["ContentLength"]
                    
                    file_info = {
                        "key": file_key,
                        "last_modified": last_modified.isoformat(),
                        "size_bytes": file_size,
                        "size_mb": round(file_size / (1024 * 1024), 2),
                    }
                    
                    if last_modified < cutoff_date:
                        freshness_checks["stale_files"].append(file_info)
                    else:
                        freshness_checks["recent_files"].append(file_info)
                    
                    # Track oldest and newest
                    if (
                        freshness_checks["oldest_file"] is None or 
                        last_modified < datetime.fromisoformat(freshness_checks["oldest_file"]["last_modified"])
                    ):
                        freshness_checks["oldest_file"] = file_info
                    
                    if (
                        freshness_checks["newest_file"] is None or 
                        last_modified > datetime.fromisoformat(freshness_checks["newest_file"]["last_modified"])
                    ):
                        freshness_checks["newest_file"] = file_info
                        
                except Exception as e:
                    LOG.warning(f"Could not get metadata for {file_key}: {e}")
            
            LOG.info(f"Freshness check completed: {len(freshness_checks['stale_files'])} stale files found")
            
        except Exception as e:
            LOG.error(f"Error during freshness check: {e}")
        
        return freshness_checks
    
    @task
    def generate_monitoring_report(
        connectivity_result: bool,
        storage_analysis: dict,
        freshness_checks: dict,
    ) -> dict:
        """
        Generate comprehensive monitoring report.
        
        Args:
            connectivity_result: S3 connectivity check result
            storage_analysis: Storage analysis results
            freshness_checks: Freshness check results
            
        Returns:
            Dictionary with monitoring report
        """
        from datetime import datetime
        
        report = {
            "timestamp": datetime.now().isoformat(),
            "connectivity": {
                "status": "healthy" if connectivity_result else "unhealthy",
                "s3_accessible": connectivity_result,
            },
            "storage": storage_analysis,
            "freshness": freshness_checks,
            "alerts": [],
            "recommendations": [],
        }
        
        # Generate alerts
        if not connectivity_result:
            report["alerts"].append("CRITICAL: S3 connection failed")
        
        if storage_analysis["total_files"] == 0:
            report["alerts"].append("WARNING: No files found in S3 storage")
        
        if len(freshness_checks["stale_files"]) > 10:
            report["alerts"].append(
                f"WARNING: {len(freshness_checks['stale_files'])} stale files detected"
            )
        
        # Generate recommendations
        if storage_analysis["total_files"] > 1000:
            report["recommendations"].append(
                "Consider implementing data archiving for old files"
            )
        
        if len(freshness_checks["stale_files"]) > 0:
            report["recommendations"].append(
                "Review and clean up stale files to optimize storage"
            )
        
        LOG.info(f"Monitoring report generated with {len(report['alerts'])} alerts")
        return report
    
    def send_slack_notification(report: dict) -> None:
        """
        Send monitoring report to Slack.
        
        Args:
            report: Monitoring report to send
        """
        from src.config.settings import get_settings
        
        settings = get_settings()
        
        if not settings.slack_webhook_url:
            LOG.warning("Slack webhook URL not configured, skipping notification")
            return
        
        # Create Slack message
        message = f"""
        *S3 Monitoring Report - {report['timestamp']}*
        
        *Connectivity Status:* {report['connectivity']['status'].upper()}
        *Total Files:* {report['storage']['total_files']}
        *Alerts:* {len(report['alerts'])}
        *Recommendations:* {len(report['recommendations'])}
        
        """
        
        if report['alerts']:
            message += "*Alerts:*\n"
            for alert in report['alerts']:
                message += f"• {alert}\n"
        
        if report['recommendations']:
            message += "\n*Recommendations:*\n"
            for rec in report['recommendations']:
                message += f"• {rec}\n"
        
        # Note: This would require Slack provider to be installed
        # For now, we'll just log the message
        LOG.info(f"Slack notification: {message}")
    
    # Define task flow
    connectivity_task = check_s3_connectivity()
    storage_task = analyze_s3_storage()
    freshness_task = check_data_freshness(storage_task)
    report_task = generate_monitoring_report(
        connectivity_result=connectivity_task,
        storage_analysis=storage_task,
        freshness_checks=freshness_task,
    )
    
    # Send notification (conditional on alerts)
    slack_task = PythonOperator(
        task_id="send_slack_notification",
        python_callable=send_slack_notification,
        op_kwargs={"report": report_task},
        trigger_rule="all_done",
    )
    
    connectivity_task >> storage_task >> freshness_task >> report_task >> slack_task


# Instantiate the DAG
s3_monitoring_dag_instance = s3_monitoring_dag()
