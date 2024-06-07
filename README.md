# choco-orm
Construction of a python package for communicating with various databases, including mysql, postgres, mongod, csv, txt, json and xml.

To execute the project:
`cd choco-orm`

Create the virtual environment:
`python3 -m venv venv`

Activating the virtual environment:

Linux:
`source venv/bin/activate`

Windows:
`.\venv\Scripts\activate`

Install the required packages:
`pip install --no-cache-dir -r requirements.txt`

In Linux, you may need to install this package to install one of the dependencies::
`sudo apt-get update`
`sudo apt-get install build-essential libpq-dev python3-dev`

To disable the virtual environment:
`deactivate`
