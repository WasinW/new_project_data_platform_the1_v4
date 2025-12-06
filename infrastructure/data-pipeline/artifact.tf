# ============================================
# ARTIFACT REGISTRY
# ============================================

module "insight_dataflow_common_repo" {
  source         = "git::https://gitlab.com/The1central/platform/the1-terraform-gcp.git//modules/artifact-registry?ref=main"
  project_id     = "the1-${var.domain}-${terraform.workspace}"
  location       = var.region
  repository_id  = "insight-datapipeline-dataflow-common"
  description    = "Image repository for Insight Data Pipeline Dataflow Common"
  format         = "DOCKER"
  immutable_tags = false
}