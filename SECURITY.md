# Security Policy

The Hermes maintainers take the security and integrity of the project seriously. This document outlines our policy for reporting and handling security vulnerabilities.

---

## Supported Versions

Security updates and critical patches are actively applied to the following versions:

| Version | Supported          |
| ------- | ------------------ |
| 0.1.x   | :white_check_mark: |
| < 0.1.0 | :x:                |

---

## Reporting a Vulnerability

**Please do not report security vulnerabilities through public GitHub issues.**

If you believe you have discovered a security vulnerability in Hermes (such as protocol bypass, memory corruption, authentication flaw in SASL/SCRAM, unauthorized data access, or denial of service), please report it responsibly:

1. **GitHub Security Advisory (Preferred)**:
   Navigate to the **Security** tab of the repository and click **"Report a vulnerability"** to submit a private report.
2. **Direct Contact**:
   If private vulnerability reporting is unavailable, contact the project maintainers directly via private channels.

### Information to Include

To help us triage and resolve the issue quickly, please include:
- A clear description of the vulnerability and its potential impact.
- Step-by-step instructions or proof-of-concept (PoC) code to reproduce the issue.
- Details regarding your environment (OS, Rust version, Hermes configuration, broker architecture).
- Any proposed remediation or patch, if available.

---

## Response Process & SLA

When a security vulnerability is reported:
1. **Acknowledgement**: A maintainer will acknowledge receipt of your report within 48–72 hours.
2. **Assessment & Confirmation**: We will investigate and verify the impact in a secure, private environment.
3. **Remediation**: A fix will be developed and tested across supported platforms.
4. **Coordinated Disclosure**: A security advisory and patched release will be published simultaneously. We will credit the reporter unless you request to remain anonymous.
