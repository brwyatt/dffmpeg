# dffmpeg
`dffmpeg` is a centrally-coordinated FFmpeg worker job manager.

This project is heavily inspired by [joshuaboniface/rffmpeg](https://github.com/joshuaboniface/rffmpeg), but instead of clients directly pushing requests to workers over SSH, work requests are pushed to a central coordinator that assigns work to active workers using HTTP polling and/or message queues.

While this requires a greater infrastructure setup, it seeks to provide the following additional helpful properties as a result:

* Multiple clients can effectively balance load on the workers
* Coordinator can re-assign or retry jobs
* Path mapping support, allowing for workers and clients to have different local mount locations
  * work assignments can take available mounts/maps, avoiding assigning work to workers that lack proper access
* High availability support - host failure does not result in lost tracking of work
