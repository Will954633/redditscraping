# Use an official Python runtime as a parent image
FROM python:3.12.7-alpine

# Set the working directory in the container
WORKDIR /app

# Copy the requirements file and install dependencies
COPY requirements.txt ./


RUN pip install --no-cache-dir -r requirements.txt

# Copy the rest of the code
COPY . .

# Command to run your scraper
CMD ["python", "redditScrapper.py"]
