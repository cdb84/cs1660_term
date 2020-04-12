xhost +"local:docker@"
docker run -it --privileged -e DISPLAY=$DISPLAY -v /tmp/.X11-unix:/tmp/.X11-unix -e GOOGLE_APPLICATION_CREDENTIALS=~/credentials.json gcp-client