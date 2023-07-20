## Updates

### July 20, 2023

Beginnin rework.
I'm taking a new approach to how user code is handled, and simplifying the framework code.

From narrowest to wides scope:
- [Tier 0] User code operates on individual rows, and produces any number of output rows
- [Tier 1] First level of GoMR code feeds data from the OS/filesystem to user code, and stores the output in files
  - Mapper outputs files by key
  - Reducer outputs a file for each key, which may be combined
- [Tier 2] Second level moves files between machines. When map tasks have output all of their data, those outputs need to be sent to the appropriate reducer
- [Tier 3] Third level coordinates job initialization. User code needs to be sent to process on a server somehow

The next step is to design a storage layer for use within Tier 1.
Tier 1 should receive a list of stores as input, and create and close stores for output.

### July 13, 2023

This project has a lot of attention! At least, more than my other repositories.
Thank you to all who have starred this repo and are interested/found the work.
I hope you continue to follow the project.

There is good news! I recently quit my job and have some time to spend on side projects.
I will be continuing development on this repo, and cleaning up what's here already.

In the past, to distribute the compute I relied on a hacky solution that directly depended on Kubernetes.
In the coming weeks, I'll be removing this explicit dependency and leaving it to the developer/deployer/implementer to define their own infra.
My goal is to have a configuration file that is as simple as providing a list of IP:port(s) where GoMR servers in the same cluster are listening.
This will allow flexible distributed deployments that could be dependent on Kubernetes, but not relying on it.
