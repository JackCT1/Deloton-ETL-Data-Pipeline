# Base image
FROM public.ecr.aws/lambda/python:3.8

# Copy script
COPY report.py ${LAMBDA_TASK_ROOT}

# Copy & Install requirments
COPY requirements.txt .
RUN  pip3 install -r requirements.txt --target "${LAMBDA_TASK_ROOT}"

# Run Handler
CMD [ "report.handler" ]