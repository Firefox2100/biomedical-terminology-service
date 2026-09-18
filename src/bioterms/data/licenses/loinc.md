# Licence notice for LOINC data

This software can download and load LOINC data, but it does not distribute the LOINC release
itself and does not grant a LOINC licence. LOINC is maintained and copyrighted by Regenstrief
Institute, Inc. and the LOINC Committee. LOINC® is a registered trademark of Regenstrief
Institute, Inc.

## Operator responsibility

The person or organisation operating this software must create its own LOINC account, accept the
current LOINC licence, and determine whether its deployment, redistribution, or hosted service
complies with that licence and with any third-party licences applicable to the selected release
artifacts. Supplying credentials configures this software to act on that operator's behalf; it
does not transfer or evidence any licence from this project.

The downloader retrieves only the primary LOINC and replacement tables, its Part file, and the Component Hierarchy
by System, plus the publisher's Part-to-external-code mapping file used for the supported
LOINC→SNOMED annotation. It also extracts the exact `LoincLicense_*.txt` shipped with that release to
`loinc/license.txt`, beside the downloaded data. That release-specific file is authoritative and
must be retained with the data. This repository notice is explanatory and is not a substitute for
the official terms.

LOINC's current licence permits commercial and non-commercial use subject to conditions,
including restrictions on creating a competing observation standard, changing official fields,
using LOINC Parts outside their permitted role, and requirements when distributing copies. Terms
may change independently of the terminology release.

## Official sources

- [Current LOINC licence](https://loinc.org/license)
- [LOINC downloads](https://loinc.org/downloads)
- [LOINC API authentication](https://loinc.org/kb/api/auth)
- [LOINC download API](https://loinc.org/kb/api/download)
