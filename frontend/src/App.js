import './App.css';
import { BrowserRouter, Route, Routes } from "react-router-dom";
import React, { useState } from "react";
import AudioRecorder from "./components/AudioRecorder";

function App() {
  
  return (
    <BrowserRouter>
          <Routes>
              <Route path="/audio-recorder" element={<AudioRecorder/>} />
          </Routes>
      </BrowserRouter>

  );
}

export default App;
