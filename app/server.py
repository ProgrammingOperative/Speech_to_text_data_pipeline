# Import flask and datetime module for showing date and time
from flask import Flask, request
import datetime

x = datetime.datetime.now()

# Initializing flask app
app = Flask(__name__)


# Route for seeing a data
@app.route('/data')
def get_time():

	# Returning an api for showing in reactjs
	return {
		'Name':"geek",
		"Age":"22",
		"Date":x,
		"programming":"python"
		}
@app.route('/data2', methods=['POST'])
def result():
    #print(request.data)  # raw data
    #print(request.json)  # json (if content-type of application/json is sent with the request)
    #print(request.get_json(force=True))  # json (if content-type of application/json is not sent)
	return {
		'Name':str(request.data),
		"Age":"25",
		"Date":x,
		"programming":"python"
		}
	
# Running app
if __name__ == '__main__':
	app.run(debug=True)
