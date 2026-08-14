# Security policy

Do not open a public issue for a vulnerability that could expose transcript data, authentication credentials, or a
deployed Brain service. Use GitHub's private vulnerability reporting for this repository.

## Supported version

Security fixes are applied to the latest release and the `main` branch.

## Deployment assumptions

- Remote traffic uses HTTPS or a private encrypted network.
- Token files and proxy credentials are stored outside the repository.
- Trusted-header mode is reachable only through the trusted proxy.
- The data volume contains sensitive transcript content and is encrypted and access-controlled by the operator.
- Operators review the repository scope and parser behavior before adding new transcript sources.

Brain request observability deliberately hashes search text, summarizes large hash lists, drops unknown argument values,
caps stored JSON, and never stores upload or MCP response bodies. Transcript search results still return sensitive content
to authorized readers; authentication is the primary confidentiality boundary.
