# Project Title 
YouTube Data Harvesting and Warehousing using SQL and Streamlit

## Getting Started
run the command "streamlit run youtube.py" to start the application


### Prerequisites
List any software, libraries, or hardware needed to run this project.
 1. googleapiclient.discovery
 2. streamlit
 3. sqlalchemy
 4. pandas
 5. isodate

 isodate is used to convert the video duration format to HH:MM:SS so that average duration can be calculated.
 Handled exception through HttpError, When video have disabled comments it is handled.
 And video tag type is converted to a string of comma separated values to support dataframe conversion.


### Installing
pip install the above mentioned libraries.

## Usage
1. Input a valid Channel ID, to save and view it's details.
2. Once data is retrieved and saved, User notified on successfull save response.
3. User can view tables availble through radio options.
4. User can select a question and the answer will be shown.
5. If the input is Invalid, user will be notified "Not a valid channel ID"
6. If no input, user will get a warning, "Please enter a channel ID."

## Contributing
Guidelines on how to contribute to the project.

## License
This project is licensed under the [License Name] - see the [LICENSE.md](LICENSE.md) file for details.
