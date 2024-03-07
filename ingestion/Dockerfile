FROM python:3.10

WORKDIR /ingestion 

#################################
# Install the Python dependencies
#################################
ADD requirements.txt ./
RUN pip install -r requirements.txt

########################################################
# Copy all the application files inside of the container
########################################################
COPY . .

########################################################################################
# Allow statements and log messages to immediately appear in the Knative CloudWatch logs
########################################################################################
ENV PYTHONUNBUFFERED True

CMD python3 ingestion.py