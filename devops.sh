# activate service account
gcloud auth activate-service-account \
    ship-data@gse-dw-prod.iam.gserviceaccount.com \
    --key-file=/Users/bobcolner/shipsticks/dev/gcp_service_account/ship-data/gse-dw-prod-64abcd721f7b.json \
    --project=gse-dw-prod

gcloud auth activate-service-account \
    mode-analytics-bq@gse-dw-prod.iam.gserviceaccount.com \
    --key-file=/Users/bobcolner/shipsticks/dev/gcp_service_account/mode/gse-dw-prod-4ce934ed9a0f.json \
    --project=gse-dw-prod

# login to both user and service account (from env var)
gcloud auth login --update-adc

# login to user accout for sdk
gcloud auth application-default login

# deploy cloud run
gcloud run jobs deploy daily-tables \                                                                                                                                             130 ↵
    --source . \
    --set-env-vars LOG_LEVEL=DEBUG \
    --max-retries 0 \
    --region us-east1 \
    --project gse-dw-prod

# run cloud run job
gcloud run jobs execute daily-tables --region us-east1
