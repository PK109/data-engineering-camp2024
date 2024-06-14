
variable "credentials" {
  description = "Path to the Service Account json file"
  default     = "../../keys/my-keys.json"
  #ex: if you have a directory where this file is called keys with your service account json file
  #saved there as my-creds.json you could use default = "./keys/my-creds.json"
}


variable "project" {
  description = "Project"
  default     = "lateral-attic-426206-c4"
}

variable "region" {
  description = "Region"
  #Update the below to your desired region
  default     = "europe-west4-a"
}

variable "location" {
  description = "Project Location"
  #Update the below to your desired location
  default     = "EU"
}

variable "bq_dataset_name" {
  description = "Big Query Dataset Name"
  default = "demo_dataset"
}

variable "gcs_bucket_name" {
    description = "Bucket Storage Name"
    default = "lateral-attic-426206-c4-terra-bucket"   
}

variable "gcs_storage_class" {
    description = "Bucket Storage Class"
    default = "STANDARD"   
}