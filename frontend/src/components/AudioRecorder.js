import AudioReactRecorder, { RecordState } from "audio-react-recorder";
import React, { useState } from "react";
import ReactAudio from "react-audio-player";
import "./Audio_recorder.css";
import "./component.css";

const AudioRecorder = () => {
    const [recordState, setRecordState] = useState(null);
    const [audioBlob, setAudioBlob] = useState(null);
    const [loading, setLoading] = useState(false);

    const start = () => {
        setAudioBlob(null);
        setRecordState(RecordState.START);
    };

    const stop = () => {
        setRecordState(RecordState.STOP);
    };

    //audioData contains blob and blobUrl
    const onStop = (audioData) => {
        console.log("audioData", audioData);
        setAudioBlob(audioData);
    };

    const submit = () => {
        setLoading(true);
        const data = new FormData();
        data.append("file", audioBlob["blob"]);
    };

    return (
        <>
            <div className="app">
                </div>

                <div className="main">
                    <div className="container">
                        <div className="display">
                            <div className="text-center">
                                <h2>Record Audio</h2>
                                <button
                                    onClick={start}
                                    type="button"
                                    className="button"
                                >
                                    Start
                                </button>
                                <button
                                    onClick={stop}
                                    type="button"
                                    className="button"
                                >
                                    Stop
                                </button>
                            </div>
                            <div className="mt-4">
                                <AudioReactRecorder
                                    state={recordState}
                                    onStop={onStop}
                                />
                            </div>

                            {!!audioBlob && (
                                <div className="my-4">
                                    <ReactAudio
                                        src={audioBlob["url"]}
                                        controls
                                    />
                                </div>
                            )}

                            {!!audioBlob && (
                                <div className="mt-4">
                                    <div className="col-md-6">
                                        <button
                                            type="submit"
                                            className="btn btn-dark"
                                            onClick={() => submit()}
                                        >
                                            {loading ? (
                                                <div
                                                    className="spinner-border"
                                                    role="status"
                                                ></div>
                                            ) : (
                                                <span>Submit</span>
                                            )}
                                        </button>
                                    </div>
                                </div>
                            )}

                        </div>
                    </div>
                </div>
        </>
    );
};
export default AudioRecorder;