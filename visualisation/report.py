import base64
from datetime import date, datetime, timedelta
import logging
import os

import boto3
from botocore.exceptions import ClientError
from dotenv import load_dotenv
import numpy as np
import pandas as pd
import plotly.express as px
import sqlalchemy