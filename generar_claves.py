"""
generar_claves.py
=================
Script de administración — ejecutar UNA SOLA VEZ antes del despliegue.

Genera los pares de claves Curve25519 para el servidor (PC2) y el
cliente (PC3) del canal de control cifrado con CurveZMQ (puerto 5563).

Uso:
    python generar_claves.py

Archivos generados:
    pc2/keys/server.key             ← clave pública de PC2 (distribuir a PC3)
    pc2/keys/server.key_secret      ← clave secreta de PC2 (NUNCA compartir)
    pc2/keys/clients/pc3_client.key ← clave pública de PC3 autorizada en PC2
    pc3/keys/client.key             ← clave pública de PC3
    pc3/keys/client.key_secret      ← clave secreta de PC3 (NUNCA compartir)
    pc3/keys/server.key             ← copia de pc2/keys/server.key

AVISO DE SEGURIDAD:
    Los archivos .key_secret deben permanecer SOLO en su nodo correspondiente
    y están excluidos de git mediante .gitignore.
    Tras copiar las claves a los nodos en producción, borra los .key_secret
    de esta máquina administradora.
"""

import shutil
import sys
from pathlib import Path

try:
    import zmq.auth
except ImportError:
    print("[ERROR] pyzmq no encontrado. Instala con: pip install pyzmq")
    sys.exit(1)

ROOT            = Path(__file__).parent
PC2_KEYS_DIR    = ROOT / "pc2" / "keys"
PC2_CLIENTS_DIR = PC2_KEYS_DIR / "clients"
PC3_KEYS_DIR    = ROOT / "pc3" / "keys"


def _limpiar_claves() -> None:
    for f in PC2_KEYS_DIR.glob("server.key*"):
        f.unlink()
    for f in PC2_CLIENTS_DIR.glob("*.key"):
        f.unlink()
    for f in PC3_KEYS_DIR.glob("*.key*"):
        f.unlink()


def main() -> None:
    print()
    print("=" * 60)
    print("  CurveZMQ — Generación de claves del canal de control")
    print("=" * 60)
    print()

    # Crear directorios si no existen
    for d in (PC2_KEYS_DIR, PC2_CLIENTS_DIR, PC3_KEYS_DIR):
        d.mkdir(parents=True, exist_ok=True)

    # Confirmar regeneración si ya existen claves
    server_secret = PC2_KEYS_DIR / "server.key_secret"
    if server_secret.exists():
        print("[AVISO] Ya existen claves generadas previamente.")
        resp = input("  ¿Regenerar? Esto invalida las claves actuales. (s/N): ").strip().lower()
        if resp != "s":
            print("[KEYGEN] Cancelado. Sin cambios.")
            return
        _limpiar_claves()

    # 1. Generar clave del servidor (PC2)
    print("\n[KEYGEN] Generando claves del servidor (PC2)...")
    zmq.auth.create_certificates(str(PC2_KEYS_DIR), "server")
    print(f"  ✔ {PC2_KEYS_DIR / 'server.key'}")
    print(f"  ✔ {PC2_KEYS_DIR / 'server.key_secret'}  ← SECRETO, mantener solo en PC2")

    # 2. Generar clave del cliente (PC3)
    print("\n[KEYGEN] Generando claves del cliente (PC3)...")
    zmq.auth.create_certificates(str(PC3_KEYS_DIR), "client")
    print(f"  ✔ {PC3_KEYS_DIR / 'client.key'}")
    print(f"  ✔ {PC3_KEYS_DIR / 'client.key_secret'}  ← SECRETO, mantener solo en PC3")

    # 3. Copiar clave pública de PC2 → PC3 (para que PC3 autentique al servidor)
    print("\n[KEYGEN] Copiando clave pública de PC2 → pc3/keys/server.key ...")
    shutil.copy2(PC2_KEYS_DIR / "server.key", PC3_KEYS_DIR / "server.key")
    print(f"  ✔ {PC3_KEYS_DIR / 'server.key'}")

    # 4. Copiar clave pública de PC3 → pc2/keys/clients/ (whitelist ZAP)
    print("\n[KEYGEN] Copiando clave pública de PC3 → pc2/keys/clients/pc3_client.key ...")
    shutil.copy2(PC3_KEYS_DIR / "client.key", PC2_CLIENTS_DIR / "pc3_client.key")
    print(f"  ✔ {PC2_CLIENTS_DIR / 'pc3_client.key'}")

    print()
    print("=" * 60)
    print("  ✔ Claves generadas correctamente.")
    print()
    print("  PRÓXIMOS PASOS:")
    print("  1. Copia el directorio pc3/keys/ completo al nodo físico PC3.")
    print("  2. Copia el directorio pc2/keys/ completo al nodo físico PC2.")
    print("  3. En producción, elimina los archivos .key_secret de esta")
    print("     máquina administradora una vez copiados a sus nodos.")
    print("=" * 60)
    print()


if __name__ == "__main__":
    main()
