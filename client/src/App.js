import React from 'react';
import { Recorder } from 'react-voice-recorder'
import 'react-voice-recorder/dist/index.css'
import './App.css';

function App() {

  const [text, setText] = React.useState(
    {message: {
      id: null,
      text: null
    }}
  )
  const [response, setResponse] = React.useState('tmp')
  const [recordedAudio, setRecordedAudio] = React.useState({
    audioDetails: {
      url: null,
      blob: null,
      chunks: null,
      duration: {
        h: null,
        m: null,
        s: null,
      }
    }
  });



  function handleAudioStop(audio) {
    setRecordedAudio({
      audioDetails: audio
    })
    console.log(recordedAudio)
  }

  function handleAudioUpload(audio) {
    
    const data = {
      text,
      audio
    }
    const str2blob = txt => new Blob([txt]);
    var file = new FormData();
    file.append('file', recordedAudio.audioDetails.blob, 'file');
    file.append('text', str2blob(text.message.text), 'text');
    file.append('id', str2blob(text.message.id), 'id');
    fetch('/data2', {
      method: 'POST',
      mode: 'cors',
      body: file
    }).then(response => response.json()
    ).then(json => {
      console.log(json)
    });
  }

  function handleReset() {
    setRecordedAudio({
      audioDetails: {
        url: null,
        blob: null,
        chunks: null,
        duration: {
          h: null,
          m: null,
          s: null,
        }
      }
    })
  }

  function handleLoadAnotherText() {
    // send request to fetch a sentence
     // Using fetch to fetch the api from 
        // flask server it will be redirected to proxy
        fetch("/data").then((res) =>
            res.json().then((data) => {
                // Setting a data from api
                setText({message:{id:data.Data.id, text:data.Data.text}});
            })
        );
  }



  React.useEffect(() => {
    // Code is executed when this page loads for the first time
    // get the text from kafka
    // setText
    setText({message:{id:-1, text:"null"}})

  }, []);

  return (
    <div className="container">
      <nav className='nav-bar'>
        <h3>Welcome to user page!</h3>
      </nav>

      <div className='user-info'>
        <p>
          {text.message.text}
        </p>

        <button className='btn' onClick={handleLoadAnotherText}>
          Load another text
        </button>
        <div className='audo-recorder'>
          <Recorder
            record={true}
            title={"Start recording the text above"}
            audioURL={recordedAudio.audioDetails.url}
            showUIAudio
            handleAudioStop={audio => handleAudioStop(audio)}
            // handleOnChange={(value) => handleOnChange(value, 'firstname')}
            handleAudioUpload={audio => handleAudioUpload(audio)}
            handleReset={handleReset} />
        </div>
      </div>
    </div>
  );
}

export default App;
