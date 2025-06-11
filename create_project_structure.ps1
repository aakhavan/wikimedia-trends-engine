# This script assumes it is run from the root of your project directory (wikimedia-trends-engine).
# It will create the subdirectories and files directly within the current location.

# Define the directories to create (relative to the current path)
$Directories = @(
    "terraform/environments/dev",
    "terraform/environments/prod",
    "terraform/modules/msk",
    "terraform/modules/other_aws_services", # Placeholder
    "src",
    "src/producer",
    "src/consumer", # Or stream_processor
    "src/utils",
    "src/common",
    "dbt_project",
    "dbt_project/models",
    "spark_apps",
    "notebooks",
    "config"
)

# Define the files to create (relative to the current path)
$Files = @(
    ".env",
    ".gitignore",
    "README.md",
    "docker-compose.yml",
    "terraform/environments/dev/main.tf",
    "terraform/environments/dev/variables.tf",
    "terraform/modules/msk/main.tf",
    "src/__init__.py",
    "src/producer/__init__.py",
    "src/producer/wikimedia_producer.py",
    "src/producer/config.py",
    "src/consumer/__init__.py",
    "src/consumer/data_consumer.py",
    "src/consumer/config.py",
    "src/utils/__init__.py",
    "src/utils/kafka_client.py",
    "src/common/__init__.py",
    "src/common/event_schemas.py",
    "config/app_settings.py",
    "requirements.txt"
)

# Create directories
Write-Host "Creating directories in current location..."
foreach ($dir in $Directories) {
    if (-not (Test-Path -Path $dir -PathType Container)) {
        New-Item -Path $dir -ItemType Directory | Out-Null
        Write-Host "  Created directory: $dir"
    } else {
        Write-Host "  Directory already exists: $dir"
    }
}

# Create files
Write-Host "`nCreating files in current location..."
foreach ($file in $Files) {
    if (-not (Test-Path -Path $file -PathType Leaf)) {
        New-Item -Path $file -ItemType File | Out-Null
        Write-Host "  Created file: $file"
    } else {
        Write-Host "  File already exists: $file"
    }
}

Write-Host "`nProject structure created successfully in the current directory!"