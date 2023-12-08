from pathlib import Path 
import yaml
import logging
from logging.handlers import TimedRotatingFileHandler
import argparse

## Flask related
from flask import Flask

## App related
from src.auth import Auth 
from src.storage import StorageAPI
from src.tasks import TaskAPI
from flask import render_template

## App declaration
app=Flask(__name__)
@app.route("/")
def hello():
    return render_template('index.html')


