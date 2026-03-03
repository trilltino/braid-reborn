# Braid-HTTP Protocol Module (`protocol`)

The `protocol` module is the low-level byte-crunching engine of the Braid implementation. It handles the serialization and deserialization of the Braid-HTTP protocol rules.

## Responsibility

This module is responsible for translating high-level Rust types (`Update`, `Version`, `Patch`) into the specific wire format required by the Braid-HTTP specification.

### Header Engineering (`headers.rs`, `formatter.rs`)
- Implements parsing and formatting for Braid-specific headers using Structured Field Values (SFV).
- Handles complex `Version` and `Parents` header logic, including support for vector clocks and structured version identifiers.
- Defines the `Merge-Type` and `Patch-Type` negotiation logic.

### Stream Multiplexing (`multiplex.rs`)
Handles the orchestration of multiple Braid updates within a single long-lived HTTP stream. This includes managing the boundaries between updates and ensuring that metadata is correctly associated with body patches.

### Constants (`constants.rs`)
Centralized registry of the Braid protocol specification constants, including:
- Standard header names (e.g., `Subscribe`, `Version`, `Parents`).
- Protocol codes (e.g., HTTP 209).
- MIME types for common Braid patch formats.

## Key Logic

- **Serialization**: Converting version DAGs into the `Parents` header format.
- **Discovery**: Detecting Braid capability in remote servers via the `Version` header in the first response.
- **Parsing**: A state-machine based parser for the Braid subscription stream.
