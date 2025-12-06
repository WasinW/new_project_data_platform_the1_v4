terraform {
 backend "gcs" {
    bucket="devops-terraformstate-nonprod"
    prefix="the1-insight/services/personas/ms-personas"
 }
}
