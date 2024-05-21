# ship-data
GSE data-platform

Data warehouse transform orchestration service.

### Local Dev Setup

Install dependencies
```
pip install requirements.txt
```

Run workflow locally
```
python main.py
```
### Deploy
```
gcloud run jobs deploy daily-tables \
    --source . \
    --set-env-vars LOG_LEVEL=DEBUG \
    --max-retries 0 \
    --region us-east1 \
    --project gse-dw-prod
```

### Execute Job Manually
```
gcloud run jobs execute daily-tables --region us-east1
```

### Schedule Job
```
gcloud scheduler jobs create http daily-tables-schedule \
    --location us-east1  \
    --schedule="0 12 * * *"  \
    --uri="https://us-east1-run.googleapis.com/apis/run.googleapis.com/v1/namespaces/gse-dw-prod/jobs/daily-tables:run"  \
    --http-method POST \
    --oauth-service-account-email 493461219675-compute@developer.gserviceaccount.com
```
