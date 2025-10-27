Water Quality Project: Quick Start

This project loads water data, cleans it up, and displays it in a web dashboard for analysis.

You need Python installed to run this project.


**_Step 1: Install Libraries_**

Open your computer's terminal (or command prompt) in this project folder and install all the necessary tools:

**pip install -r requirements.txt**


**_Step 2: Run the Project_**

The project needs two parts to run: the Backend API (which holds the data) and the Frontend Dashboard (what you see in the browser).

You must open **two separate terminal** windows for these two commands.


Terminal 1: Start the Data API (Backend)

**You can simply just click the Run button on the water_quality_api.py for it to run. Keep the window open and running.**


Terminal 2: Start the Web Dashboard (Frontend)

This opens the analysis dashboard in your web browser.

**python -m streamlit run "Class Project/client/water_quality_client.py"**

OR

**streamlit run client/water_quality_client.py**


---------------------------------------------------------------------------


The data is handled by a local server running at http://localhost:5000/


/api/observations : Gets the main table data, filtered by time or specific values.


/api/stats : Calculates quick statistics (average, min, max) for all water fields.


/api/outliers : Finds data points that are unusual (outliers).
