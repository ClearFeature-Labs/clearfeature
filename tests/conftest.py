"""Suite-wide security default : the EXPLICIT development bypass.

The platform is secure by default (`FSP_SECURITY_MODE=api_key`, which refuses to start
without keys). The Docker-free suite runs the HTTP apps with the documented
development-only bypass instead of forcing every test to configure real keys.

Set at import time because `fintech_feature_platform.api.app` builds an app when the
module is imported. Security behavior itself is tested in
`tests/api/test_security.py`, which passes explicit `SecurityConfig` objects and env
mappings — those tests are unaffected by this default.
"""

import os

os.environ["FSP_SECURITY_MODE"] = "disabled"
os.environ["FSP_ENVIRONMENT"] = "development"
