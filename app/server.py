# Import flask and datetime module for showing date and time
from flask import Flask, request
import datetime
from kafka import KafkaConsumer
from json import loads  
from json import dumps
from flask_cors import CORS
from kafka import KafkaProducer

x = datetime.datetime.now()

# Initializing flask app
app = Flask(__name__)
CORS(app)

def load_text(topic_name, server):
	consumer = KafkaConsumer(topic_name,
							bootstrap_servers=server,
							auto_offset_reset='latest',
							enable_auto_commit=False,
							value_deserializer=lambda x: loads(x.decode('utf-8')))

	
	for message in consumer:
		return message.value

def send_meta(topic_name, server, meta):
	producer = KafkaProducer(bootstrap_servers=server,
                                 value_serializer=lambda x: dumps(x).encode('utf-8'))
	producer.send(topic_name, meta)

# Route for seeing a data
@app.route('/data')
def get_text():
	resultdd = load_text("audio_exp5", ['localhost:9092'])
	# Returning an api for showing in reactjs
	return {"Data": resultdd}
@app.route('/data2', methods=['POST'])
def result():
    #print(request.data)  # raw data
    #print(request.json)  # json (if content-type of application/json is sent with the request)
    #print(request.get_json(force=True))  # json (if content-type of application/json is not sent)
	audio = request.files['file'].stream.read()
	text = request.files['text'].stream.read().decode('utf-8')
	id = request.files['id'].stream.read().decode('utf-8')
	audio_url = f"/mnt/10ac-batch-5/week9/reiten/unprocessed/{id}.wav"
	meta = {"id":id, "text":text, "url":audio_url }
	
	send_meta("audio_exp8", ['localhost:9092'], meta)
	
	print(text, id)
	with open(audio_url, 'wb') as f:
		f.write(audio)
	print('saved')
	return {'Name':str(request.data)}
	
# Running app
if __name__ == '__main__':
	app.run(debug=True)
