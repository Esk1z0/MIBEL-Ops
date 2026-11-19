"""Allow ``python -m mibel_ingestor`` as an alternative to the ``mibel-ingestor`` command."""

from .cli import main

raise SystemExit(main())
