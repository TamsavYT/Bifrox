# Broker peer authentication

Broker RPCs require authenticated TLS by default. Cluster IDs and IP addresses are not credentials. Each broker needs a distinct certificate and key, signed by the configured CA. Certificates must support both server and client authentication and include the advertised hostname/IP in their subject alternative names.

Example for node 1 (configure the reciprocal node 1 pin on node 2):

```properties
security.protocol=SSL
ssl.ca.path=/etc/bifrox/ca.pem
ssl.cert.path=/etc/bifrox/node1.pem
ssl.key.path=/etc/bifrox/node1.key
replication.peer.cert.2=/etc/bifrox/node2.pem
```

The peer pin is the exact leaf certificate, mapped to its node ID. Configure every allowed peer. Duplicate identities and pins for the local node are rejected. Restart brokers after certificate rotation. Ordinary TLS clients may omit a client certificate unless `ssl.client.auth=required`; they cannot send broker RPCs. SASL_SSL continues to require SASL for ordinary client requests, including forwarded client requests; certificate authentication does not grant a client principal or bypass client ACLs.

Replication fetch requires the certificate's node ID to match the request and belong to the partition's replica set. Heartbeats and votes must claim the certificate's node ID. Data pushes require the assigned partition leader; metadata pushes require the known controller leader. Outbound connections validate the CA, hostname, and peer certificate pin. Connect/handshake and complete RPC exchanges have bounded deadlines.

For isolated plaintext development clusters only:

```properties
replication.allow.insecure=true
```

This explicitly disables peer authentication and must not be used on untrusted networks. It cannot be combined with TLS, SASL, ACLs, or token authentication. Existing plaintext clusters must migrate to certificates or explicitly opt into this development setting. A standalone broker's ordinary plaintext client interface remains available; its replication interface rejects unauthenticated requests by default.
