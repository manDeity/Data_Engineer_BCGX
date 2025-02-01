### Project Overview :

    This project analyzes vehicle crash data using Apache Spark. The analysis includes identifying accident patterns, 
    vehicle makes involved in crashes, and factors contributing to road accidents.
    The main script (main.py) runs multiple queries using PySpark to extract insights 
    from vehicle crash data. The results are stored in output files.

### Project Sructure

📂 Data                                  # Input CSV Files

📂 Output                                # Output is stored in this folder 

📂 src

     │── utils.py                        # Utility functions for reading YAML & writing data

     │── VehicleCrashAnalysis.py         # Class with data analysis methods


│── main.py                              # Main script to run analysis

│── config.yaml                          # Configuration file (input/output paths)

│── requirements.txt                     # Required dependencies

│── README.md                            # Documentation


Customization

Modify config.yaml to change input file paths and input formats.

You can modify VehicleCrashAnalysis.py to add new queries for further analysis.



### Installation and Setup
1. Prerequisites
    Ensure you have the following installed:

    `Python (Version 3.7 or later)`
    `Apache Spark (Version 3.x)`
    `pip (Python package manager)`


2. Install Dependencies:
    Run the following command inside the project folder:

    `pip install -r requirements.txt`

3. Extract zip files if any: 
    If you have received a ZIP file of this repository:

    Unzip the file to a folder on your system.
    Navigate to the extracted folder.


### To Run the script

Run the command 
    `python3 main.py`


### It will display all the output mentioned in the query.


🚀 Happy Analyzing! 🚀
