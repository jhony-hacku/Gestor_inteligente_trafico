"""
servicios/cripto.py
===================
Utilidades de criptografía CurveZMQ para el nodo PC2 (servidor).

Expone dos funciones:

  cargar_clave_servidor(keys_dir)
      Carga la clave pública y secreta del servidor PC2 desde
      ``keys_dir/server.key_secret``.
      Falla con SystemExit si el archivo no existe.

  iniciar_authenticator(ctx, clients_dir)
      Inicia el ZAP handler (ThreadAuthenticator) y lo configura para
      rechazar cualquier cliente cuya clave pública no esté en
      ``clients_dir/*.key``.
      Falla con SystemExit si el directorio no existe o está vacío.

MODO ESTRICTO:
    El sistema termina inmediatamente con mensaje descriptivo si falta
    cualquier archivo de clave. No existe modo degradado sin cifrado.
    Ejecutar ``python generar_claves.py`` para crear las claves.
"""

import sys
from pathlib import Path

import zmq
import zmq.auth
from zmq.auth.thread import ThreadAuthenticator


def cargar_clave_servidor(keys_dir: Path) -> tuple[bytes, bytes]:
    """
    Carga (public_key, secret_key) del servidor desde
    ``keys_dir/server.key_secret``.

    Retorna
    -------
    (public_key, secret_key) como bytes (formato Z85 de 40 bytes cada uno).

    Lanza
    -----
    SystemExit si el archivo no existe.
    """
    secret_file = keys_dir / "server.key_secret"
    if not secret_file.exists():
        print(
            f"\n[CRIPTO] ✖  Clave secreta del servidor no encontrada:\n"
            f"           {secret_file}\n"
            f"\n"
            f"           Pasos para resolver:\n"
            f"           1. Ejecuta en la máquina administradora:\n"
            f"                  python generar_claves.py\n"
            f"           2. Copia el directorio pc2/keys/ a este nodo PC2.\n"
        )
        sys.exit(1)

    public_key, secret_key = zmq.auth.load_certificate(str(secret_file))
    print(f"[CRIPTO] Clave del servidor cargada: {secret_file.name}")
    return public_key, secret_key


def iniciar_authenticator(ctx: zmq.Context, clients_dir: Path) -> ThreadAuthenticator:
    """
    Inicia el ZAP authenticator y lo configura para aceptar únicamente
    los clientes cuya clave pública esté en ``clients_dir``.

    Lanza
    -----
    SystemExit si el directorio no existe o no contiene ningún archivo .key.
    """
    if not clients_dir.exists():
        print(
            f"\n[CRIPTO] ✖  Directorio de claves de clientes no encontrado:\n"
            f"           {clients_dir}\n"
            f"\n"
            f"           Pasos para resolver:\n"
            f"           1. Ejecuta en la máquina administradora:\n"
            f"                  python generar_claves.py\n"
            f"           2. Copia el directorio pc2/keys/ a este nodo PC2.\n"
        )
        sys.exit(1)

    client_keys = list(clients_dir.glob("*.key"))
    if not client_keys:
        print(
            f"\n[CRIPTO] ✖  No hay claves de clientes autorizados en:\n"
            f"           {clients_dir}\n"
            f"\n"
            f"           Pasos para resolver:\n"
            f"           1. Ejecuta 'python generar_claves.py'.\n"
            f"           2. Copia pc2/keys/clients/pc3_client.key a este nodo.\n"
        )
        sys.exit(1)

    auth = ThreadAuthenticator(ctx)
    auth.start()
    auth.configure_curve(domain="*", location=str(clients_dir))

    nombres = ", ".join(k.name for k in client_keys)
    print(
        f"[CRIPTO] ZAP authenticator activo — "
        f"{len(client_keys)} cliente(s) autorizado(s): {nombres}"
    )
    return auth
