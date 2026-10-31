variable "region" {
  type        = string
  default     = "eu-west-1"
  description = "AWS region for every resource."
}

variable "name" {
  type        = string
  default     = "wikistream"
  description = "Prefix for resource names."
}

variable "monthly_budget_usd" {
  type        = number
  default     = 10
  description = "Alert when forecast spend passes this amount."
}

variable "alert_email" {
  type        = string
  description = "Email that receives budget alerts."
}

variable "athena_scan_limit_gb" {
  type        = number
  default     = 10
  description = "Athena cancels any query that scans more than this per query (cost guard-rail)."
}

variable "bronze_expire_days" {
  type        = number
  default     = 90
  description = "Raw events are deleted after this many days. Aggregates are kept."
}
