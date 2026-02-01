# 1. Use a lightweight Python base image
FROM python:3.12-slim

# 2. Set the "home base" for our code
WORKDIR /app

# 3. Install system tools needed for the database driver (psycopg2)
RUN apt-get update && apt-get install -y libpq-dev gcc && rm -rf /var/lib/apt/lists/*

# 4. Copy ONLY the files needed to install dependencies first
# (This allows Docker to cache the install step if your code changes but deps don't)
COPY pyproject.toml .
COPY src/ ./src/

# 5. Install the project and its dependencies
RUN pip install --no-cache-dir .

# 6. Copy your jobs/ scripts
COPY jobs/ ./jobs/

# 7. Ensure logs appear in Google Cloud console immediately
ENV PYTHONUNBUFFERED=True

# 8. Run the dummy heartbeat script
CMD ["python", "jobs/dummy_heartbeat.py"]