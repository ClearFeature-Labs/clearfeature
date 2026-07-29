# Security Policy

## Reporting a vulnerability

Please report suspected vulnerabilities privately via
**GitHub Security Advisories** for this repository
(https://github.com/ClearFeature-Labs/clearfeature/security/advisories/new).
Do not open public issues for security reports. We aim to acknowledge reports
within a reasonable time and will coordinate a fix and disclosure with you.

## Supported versions

The Community beta is a pre-1.0 release; only the latest release line receives
security fixes.

## Security model (read before reporting)

- **Trusted UDF boundary**: Community feature UDFs are code written by trusted
  authors and execute **in-process** with worker permissions. There is **no
  sandbox** and no isolation between a UDF and its worker. "A malicious UDF can
  affect its worker" is therefore the documented trust model, not a vulnerability.
- **Fail-closed authentication**: the API requires bearer keys in the default mode
  and refuses to start without them; the only bypass is an explicit development
  setting.
- **Artifact binding**: production-like deployments verify the installed feature
  code byte-for-byte against the promoted artifact digest and fail closed on
  mismatch. Reports that bypass this verification are in scope and high priority.
- Reports about secrets appearing in logs, metrics, readiness output, or CLI
  errors are in scope.
