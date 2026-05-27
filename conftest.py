# conftest.py (raíz del proyecto)
# ─────────────────────────────────────────────────────────────────
# Configuración global de pytest para GITU.
# Agrega tanto pc1/ como pc2/ al sys.path para que los módulos
# de ambos nodos sean importables desde sus respectivos tests.
# ─────────────────────────────────────────────────────────────────

import sys
from pathlib import Path

ROOT = Path(__file__).parent
for subdir in ("pc1", "pc2"):
    p = str(ROOT / subdir)
    if p not in sys.path:
        sys.path.insert(0, p)
