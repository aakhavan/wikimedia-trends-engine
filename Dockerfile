# Use an official, well-maintained Spark base image that includes Python.
FROM bitnami/spark:3.4.0

# --- SOLUTION: Switch to the root user to install dependencies ---
USER root

# Set the working directory inside the container
WORKDIR /app

# Copy the requirements file first to leverage Docker's layer caching.
COPY requirements.txt .

# Install the 'wheel' package first, then install all other dependencies.
# This prevents fallback to the legacy 'setup.py install'.
RUN pip install --no-cache-dir wheel && \
    pip install --no-cache-dir -r requirements.txt

# --- BEST PRACTICE: Switch back to the non-root user ---
# The default non-root user in the bitnami image is '1001'.
USER 1001

# Copy the application source code and configuration into the container
# These files will now be owned by the non-root user.
COPY src/ ./src
COPY config/ ./config