# Sneller SQL Daemon

The `snellerd` daemon exposes the Sneller SQL query engine
as an HTTP API. The `snellerd` daemon is responsible for
performing query planning (dividing queries up across
multiple machines) and for enforcing a security boundary
between tenant query processes.

The `snellerd` binary delegates the responsibility
of performing request authorization/authentication
and peer discovery to external programs in order
to make the query engine itself maximally portable.

## Command Line Options

### `-e <bind-address>`

The `-e` argument indicates the address
on which to bind the public HTTP endpoint.
Note that *this endpoint should only be made
available publicly via HTTPS in order to avoid
leaking bearer tokens*. Consider configuring
a reverse-proxy to perform TLS termination
and request forwarding to `snellerd` listening on localhost.

The default value for `-e` is `127.0.0.1:8000`.

### `-r <bind-address>`

The `-r` argument indicates the address
on which to listen for peers to request
subqueries be executed.

The default value for `-r` is `127.0.0.1:9000`.

*THIS ADDRESS SHOULD NOT BE PUBLICLY ACCESSIBLE.
IT IS ASSUMED THAT TRAFFIC OVER THIS SOCKET HAS
ALREADY BEEN AUTHENTICATED.*

### `-x <peers-cmdline>`

The `-x` argument is used to indicate
which program to run in order to determine
the list of peer addresses to use for
"split" (multi-machine) queries.

The program given by `-x` will be run
on a regular interval and its JSON output
will be interpreted as the current list of
peers to use for query planning.

(Note that this program is run periodically
in the background after `snellerd` has initialized,
so peer updates are not synchronous with respect to
new requests. Peers that are removed from the peer list
should remain available for a grace period of at least
a few minutes after disappearing from the peer list.)

The list of peers can be configured from a local file
by setting the `-x` program to `-x cat path/to/static-peers.json`.

### `-a <auth>`

The `-a` flag indicates the authorization and
authentication strategy used for requests.

If `-a` is passed an `http://` or `https://` URL,
then the bearer token present in the `Authorization`
header is forwarded to that address, and its response
body is expected to indicate the S3 location and credentials
necessary to run queries associated with that token's identity
(or return a non-200 response if the token is bogus).

If `-a` is passed a `file://` URI, then the file path
occurring after the `file://` prefix should contain a JSON
structure with the static credentials that the `snellerd`
process should use. (Note that this configuration only
works for single-tenant deployments.)

## Other Options

### `CACHEDIR`

The `CACHEDIR` environment variable determines the root
of the file tree in which tenants will cache data.

### `bwrap(1)`

If the `bwrap(1)` program is available, then `snellerd`
will use it to sandbox tenant processes.
*Sandboxing is strongly recommended in multi-tenant deployments.*

## Running locally

Here's a short example of how to two `snellerd`
processes as children of a shell session with
peering enabled between the two daemons:

```
$ cat >creds.json <<EOF
... your credential object here
EOF
$ cat >peers.json <<EOF
{"peers":[{"addr":"127.0.0.1:8002", "addr":"127.0.0.1:8004"}]}
EOF
$ cachedir0=$(mktemp -d)
$ cachedir1=$(mktemp -d)
$ CACHEDIR=$cachedir0 snellerd -e 127.0.0.1:8001 -r 127.0.0.1:8002 -x 'cat peers.json' -a file://creds.json &
$ CACHEDIR=$cachedir1 snellerd -e 127.0.0.1:8003 -r 127.0.0.1:8003 -x 'cat peers.json' -a file://creds.json &
```

The `CACHEDIR` should be set to a directory that is unique for each node, so they all have a private cache folder. Using `mktemp -d` guarantuees a new temporary directory, but make sure to remove these directories when you finished debugging. If you don't have sufficient RAM, then you might want to map to disk-backed directory at the expense of reduced performance.
