gcloud auth activate-service-account \
    ship-data@gse-dw-prod.iam.gserviceaccount.com \
    --key-file=/Users/bobcolner/shipsticks/dev/gcp_service_account/ship-data/gse-dw-prod-64abcd721f7b.json \
    --project=gse-dw-prod

gcloud auth activate-service-account \
    mode-analytics-bq@gse-dw-prod.iam.gserviceaccount.com \
    --key-file=/Users/bobcolner/shipsticks/dev/gcp_service_account/mode/gse-dw-prod-4ce934ed9a0f.json \
    --project=gse-dw-prod

gcloud auth login --update-adc

gcloud auth application-default login

gcloud run jobs execute daily-tables --region us-east1

