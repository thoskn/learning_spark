Default mode is bridge. To access containers on bridge network will need to map port so that they are available from localhost

Can specify --network=host. Container is on the host network, so no port mapping allowed. Can access from localhost.

Can create a user defined network `docker network create my-network`. Then attach containers to network at runtime --network=my-network.
Containers on the same user defined network can refer to each other by container/service name, rather than needing to know
their ip address. 