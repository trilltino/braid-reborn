# Transport credentials and event metadata

Request credentials reach HTTP authentication but are excluded when an HTTP
entity becomes an Update. The shared parser excludes credentials, hop-by-hop
fields and nominated Connection fields from replicated metadata. Other unknown
application metadata remains intact. Update framing also filters these fields
from old retained events, while HTTP request encoding preserves credentials.

The reliable channel rejects transport fields in new queued events. If an old
outbox contains them, recovery stops explicitly and preserves the original bytes
and uncertain event identity for local migration. Channel request credentials
belong outside the durable event payload.

`engineering_wire` verifies request authentication fields and both replication
directions; `reliable_channel` verifies rejected queueing and unchanged legacy
journal bytes. Both suites pass locally. An existing database or old event
fingerprint may still contain earlier transport fields. A complete legacy
metadata/fingerprint migration and corpus audit remain release work; wire
filtering is not evidence that arbitrary old on-disk secrets were removed.
