# Vagrant

Setup scripts inside of this folder are useful for testing either a cluster on a single node or multiple clusters on separate nodes.
Multi node is useful for testing master/feature differences.

All single node setup scripts are inside of `./single`.
This setup is generally best for performance testing and benchmarking a single version of M3DB.

All multi node setup scripts are inside of `./multi`.
This setup is useful for comparing performance of feature branches against `latest`, as well as helping verify correctness of the feature branch.

All shared scripts are inside of `./shared`.
These are the same for both single/multi node setups.
