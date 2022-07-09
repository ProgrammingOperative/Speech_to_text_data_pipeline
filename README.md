# Speech_to_text_data_pipeline

![image](https://user-images.githubusercontent.com/44437166/178087878-3434a31a-a0c9-4078-a530-a59f5ac5307b.png)


**Table of content**

- [Overview](#overview)
- [Install](#install)
- [Data](#data)
- [Folders](#notebooks)

## Overview

> The purpose of this week’s challenge is to build a data engineering pipeline that allows recording millions of Amharic and Swahili speakers reading digital texts in-app and web platforms. There are a number of large text corpora we will use
> We will design and build a robust, large scale, fault tolerant, highly available Kafka cluster that can be used to post a sentence and receive an audio file. By the end of this project, we will produce a tool that can be deployed to process posting and receiving text and audio files from and into a data lake, apply transformation in a distributed manner, and load it into a warehouse in a suitable format to train a speech-t0-text model. 
## Install

```
git clone https://github.com/Reiten-10Academy/Speech_to_text_data_pipeline
cd Speech_to_text_data_pipeline
pip install -r requirements.txt
```

## Data

Data can be found [here](https://github.com/IsraelAbebe/An-Amharic-News-Text-classification-Dataset)

#### description

     Amharic news text classification dataset with baseline performance dataset: 

## folders

> - backend: a flask server and a bunch of python scripts that process data in pipeline 
> - frontend: a react application.
> - extra: contains, notebooks, docs, and other development and testing files.

## Authors

- 👤 **Biniyam Belayneh**
- 👤 **Meron Abate**
- 👤 **Tewodros Kaderaleh**
- 👤 **Gezahegne Wondachew**
- 👤 **Hewan Mulu**
- 👤 **Titus Wachira**
- 👤 **Amal Abdallah**



## Show your support

Give a ⭐ if you like this project!
