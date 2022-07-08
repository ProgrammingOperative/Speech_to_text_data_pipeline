// Importing modules
import React, { useState, useEffect } from "react";
import "./App.css";
  
function App() {
    // usestate for setting a javascript
    // object for storing and using data
    const [data, setdata] = useState({
        name: ""
    });
  
    // Using useEffect for single rendering
    function handleClick() {
        // Using fetch to fetch the api from 
        // flask server it will be redirected to proxy
        fetch("/data").then((res) =>
            res.json().then((data) => {
                // Setting a data from api
                setdata({
                    name: data.Name
                });
            })
        );
    }
    // Using useEffect for single rendering
    function handleClick2() {
        // Simple POST request with a JSON body using fetch
        const requestOptions = {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ title: 'React POST Request Example' })
        };
        fetch('/data1', requestOptions)
            .then(response => response.json())
            .then((data) => {
                // Setting a data from api
                setdata({
                    name: JSON.parse(data.Name)['bini']
                });
            });
    }
    return (
        <div className="App">
            
            <header className="App-header">
                <h1>React and flask</h1>
                <div onClick={handleClick} style={{
                    textAlign: 'center',
                    width: '100px',
                    border: '1px solid gray',
                    borderRadius: '5px'
                    }}>
                    Send data to backend
                </div>
                {/* Calling a data from setdata for showing */}
                <p>{data.name}</p>
  
            </header>
        </div>
    );
}
  
export default App;