# Base image: comes with Chrome + Chromedriver + Selenium server
FROM selenium/standalone-chrome:latest

USER root

# Install Python + supervisor + system dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    python3 python3-pip python3-venv supervisor build-essential gcc \
    libxml2-dev libxslt1-dev zlib1g-dev libffi-dev \
  && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Copy python dependencies
COPY requirements.txt /app/requirements.txt

# Install python deps
RUN pip3 install --no-cache-dir -r requirements.txt

# Copy your entire application
COPY . /app

# Copy supervisor config
COPY supervisord.conf /etc/supervisor/conf.d/supervisord.conf

ENV PYTHONUNBUFFERED=1
ENV PORT=5000

EXPOSE 5000 4444

# Supervisor runs selenium server + gunicorn
CMD ["/usr/bin/supervisord", "-c", "/etc/supervisor/conf.d/supervisord.conf"]

