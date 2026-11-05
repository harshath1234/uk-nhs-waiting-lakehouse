variable "subscription_id" {
  description = "Azure subscription ID (Azure for Students or free account). `az account show --query id -o tsv`"
  type        = string
}

variable "location" {
  description = "Azure region. UK South keeps data in the UK and matches the Snowflake region used later."
  type        = string
  default     = "uksouth"
}

variable "prefix" {
  description = "Short lowercase prefix for resource names."
  type        = string
  default     = "nhswait"
  validation {
    condition     = can(regex("^[a-z0-9]{3,10}$", var.prefix))
    error_message = "prefix must be 3-10 lowercase letters or digits (storage account naming rules)."
  }
}

variable "budget_amount" {
  description = "Monthly budget for the resource group, in the subscription's billing currency."
  type        = number
  default     = 5
}

variable "budget_start_date" {
  description = "First day of the current month, RFC3339 (Azure requires the first of a month)."
  type        = string
  default     = "2026-11-01T00:00:00Z"
}

variable "alert_emails" {
  description = "Who gets budget alerts."
  type        = list(string)
}

variable "trigger_activated" {
  description = "Start the monthly ADF trigger. Keep false until a manual pipeline run has succeeded."
  type        = bool
  default     = false
}

variable "tags" {
  type = map(string)
  default = {
    project = "uk-nhs-waiting-lakehouse"
    owner   = "portfolio"
  }
}
