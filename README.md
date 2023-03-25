# Online Learning Deployment for Streaming Applications

This repository is the official implementation of the paper **Online Learning Deployment for Streaming Applications in the Banking Sector** (Barry, Montiel, Bifet, Wadkar, Chiky, Shakman, Manchev, Halford, El Baroudi,  ICDE 2023). The ressources can be used to set up and deploy instances of online machine learning models, to generate predictions and update the model weights on streaming data.  

> **Motivations** Our goal is to propose platform to provide a seamless bridge between data science-centric activities and data engineering activities, in a way that satisfies both the imposed production constraints in term of scalability and streaming application requirements in term of online learning. Examples of potential use cases can be anomaly and fraud detection for time-evolving data streams or real-time classification of user activities or IT or logs events. This is can be a real accelerator to gain in pro-activity for real world problems solving.

## Tools used : RIVER, Kafka & Domino Platform on AWS

> [River](https://github.com/online-ml/river) [[1]](#1) is an open-source online machine learning library written in Python which main focus is **instance-incremental
learning**, meaning that every component (estimators, transformers, performance metrics, etc.) is designed to be updated one sample at a time. We used River to continuously train and update online learning model from last data streams. 
> [KAFKA](https://kafka.apache.org/) is 
a state of the art open-source distributed
event streaming platform and we used a managed hosted Kafka ([confluent](https://www.confluent.io/). We used it as a data streams generator.

> The [Domino Platform](https://www.dominodatalab.com/) platform is implemented on top of Kubernetes, where
it spins up containers on demand for running user workloads. The containers are based on Docker images, which are fully customizable. We used Domino to host the models and run scalability tests on hig velocity data generated as streams. 

<img width="484" alt="technologies_used_river_domino" src="https://user-images.githubusercontent.com/27995832/113413633-6655d280-93bb-11eb-9f0d-d9674024d465.PNG">