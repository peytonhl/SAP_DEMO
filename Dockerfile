# Use an official Python base image
FROM python:3.11-slim

# Set the working directory inside the container
WORKDIR /app

# Copy requirements.txt and install Python packages
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy the rest of the app code into the container
COPY . .

# Expose port 5000 for Flask (or whatever port your app runs on)
EXPOSE 5000

# Start the app
CMD ["python", "app.py"]
