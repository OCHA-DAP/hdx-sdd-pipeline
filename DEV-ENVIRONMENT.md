# Development Environment

This file describes the development environment for the SSD pipeline.

## SSD Pipeline
- is a python 3.12 application
- python pipeline that runs on the host system, not in docker
- runs in a python virtual environment

## Redis service
- runs  in a docker compose stack
- expose a port 6379 so that the SSD pipeline can connect to it

## Python helper script
- `redis_streams_event_generator.py` 
- reads events from the json file, `events.json` and pushes them to **redis streams**


## Testing
- based on pytest
- testing requirements are all in `dev-requirements.txt` file