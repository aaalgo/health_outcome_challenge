import os
import sys
CMS_HOME=os.environ.get("CMS_HOME", None)
if not CMS_HOME is None:
    sys.path.append(CMS_HOME)
    pass
