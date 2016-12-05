This folder contains the files that need to be deployed on each worker that will process our media.

The example that we took is about detecting a face on an image and cropping it around the detected face. This is done in meditator.py which is called by worker.py.

It is of course completely possible to customize this script to do other jobs. Because there is an acknoledgement refresh, long tasks can also be handled using the same technic 