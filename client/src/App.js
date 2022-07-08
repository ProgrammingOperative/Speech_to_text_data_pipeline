import React from 'react';
import { Recorder } from 'react-voice-recorder'
import 'react-voice-recorder/dist/index.css'
import './App.css';

function App() {

  const [text, setText] = React.useState('')
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
  }

  function handleAudioUpload(audio) {
    console.log(data.duration)
    const data = {
      text,
      audio
    }

    console.log(data)
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
                setText(data.Data);
            })
        );
  }



  React.useEffect(() => {
    // Code is executed when this page loads for the first time
    // get the text from kafka
    // setText
    setText('This is the text from kafka')

  }, []);

  return (
    <div className="container">
      <nav className='nav-bar'>
        <h3>Welcome to user page!</h3>
      </nav>

      <div className='user-info'>
        <p>
          {text}
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
