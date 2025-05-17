# Use the official PySpark Jupyter image as the base
FROM jupyter/pyspark-notebook:latest

# Set working directory
WORKDIR /app

# Copy dependency file
COPY requirements.txt .

# Install Python dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy project files to the working directory
COPY . .

# Set environment variable
ENV PYTHONPATH=/app

# Expose Jupyter port
EXPOSE 8888

# Keep the container running
CMD ["jupyter", "lab", "--ip=0.0.0.0", "--port=8888", "--no-browser", "--allow-root", "--NotebookApp.token=''", "--NotebookApp.password=''"]
