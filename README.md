# ship-data
GSE data-platform

Data wharehouse transformation orchestration service on GCP cloud run.

### Local Dev Setup

    pip install requirements.txt
    python main.py

### Deploy

    gcloud run jobs deploy job-quickstart \
	    --source . \
	    --set-env-vars LOG_LEVEL=DEBUG \
	    --max-retries 5 \
	    --region REGION \
	    --project=PROJECT_ID

### Execute Job Manually

    gcloud run jobs execute job-quickstart --region REGION

### Schedule Job

    gcloud scheduler jobs create http SCHEDULER_JOB_NAME \
	    --location SCHEDULER_REGION  \  
	    --schedule="SCHEDULE"  \  
	    --uri="https://CLOUD_RUN_REGION-run.googleapis.com/apis/run.googleapis.com/v1/namespaces/PROJECT-ID/jobs/JOB-NAME:run"  \  
	    --http-method POST \  
	    --oauth-service-account-email PROJECT-NUMBER-compute@developer.gserviceaccount.com
