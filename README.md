[![Build Status](https://dev.azure.com/databricksdevops/CodingAssesment/_apis/build/status/rsamant07.Coding-Assesment?branchName=main)](https://dev.azure.com/databricksdevops/CodingAssesment/_build/latest?definitionId=4&branchName=main)

# Programming Exercise 

## Background:
This Repository is for code assesment based on the instructions given in **excerise.md**


## Clone repository and install dependencies:

Next, clone this repository into your local machine using:

$ git clone git@github.com:rsamant07/Coding-Assesment.git

Next, install the requirements.txt in the cloned repository. cd into the source of the repository and do the following:

$ pip3 install -r requirements.txt

This should install all the required libraries for the program

## Configuration Defintion

Define the Configuration in the **conf/config.ini**

## Running the application
To run the application make sure you are in the project directory

$ python processDataset.py --dataset1 <dataset1Path> --dataset2 <dataset2Path> --countrylist <CountriesList>
  
**e.g.**   
python processDataset.py --dataset1 dataset_one.csv --dataset2 dataset_two.csv --countrylist 'United Kingdom','Netherlands'
  

