# Use a specific version for reproducibility
FROM bitnami/spark:3.5.1

# Set the working directory
WORKDIR /app

# --- 1. Install Dependencies ---
# Copy only the requirements file first to leverage Docker's layer caching.
COPY requirements.txt .

# Install 'wheel' first for efficiency, then install all other dependencies.
# This entire step is cached as long as requirements.txt doesn't change.
RUN pip install --no-cache-dir wheel && \
    pip install --no-cache-dir -r requirements.txt

# --- 2. Copy Application Code ---
# Now copy the rest of the application source code into the container.
COPY src/ ./src
COPY config/ ./config
COPY scripts/ ./scripts

# --- 3. Switch to Non-Root User for Security ---
# The default non-root user in the bitnami image is '1001'.
# All subsequent commands and the running container will use this user.
USER 1001

# The CMD (what the container runs by default) is inherited from the base image.