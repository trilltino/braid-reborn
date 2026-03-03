# Server Chat Module (`chat`)

The `chat` module implements the high-level application logic for decentralized messaging and collaboration over the Braid protocol.

## Key Sub-modules

### CRDT Integration (`crdt.rs`)
Implements the chat-specific conflict resolution rules. It ensures that messages from multiple peers are merged deterministically, maintaining a consistent conversation history across all nodes.

### AI Context Agents (`ai/`)
Integrates AI models (via `genai`) into the chat flow. Agents can listen to channels, process conversation history, and provide contextual responses natively within the Braid sync cycle.

### Braidmail (`mail/`)
A decentralized email-like messaging protocol implemented over Braid-HTTP. It handles message hydration, attachment syncing, and inbox management for peer-to-peer communication.

### Contacts and Social (`friends/`)
Handles peer discovery and contact management:
- Managing shared friend lists.
- Processing and validating friend requests.
- Synchronizing contact metadata across devices.

### Handlers (`handlers/`)
The implementation of the chat application's HTTP and WebSocket endpoints. These handlers bridge the high-level chat logic to the underlying Braid protocol engine.
