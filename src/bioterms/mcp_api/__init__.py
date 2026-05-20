from fastmcp.server.transforms import ResourcesAsTools

# Import the files to ensure decorators are all applied and registered
from .resource import *
from .tool import *

from .app import mcp


mcp.add_transform(ResourcesAsTools(mcp))
