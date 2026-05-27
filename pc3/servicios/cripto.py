"""
servicios/cripto.py
===================
Utilidades de criptografía CurveZMQ para el nodo PC3 (cliente).

Expone:

  aplicar_curve_cliente(sock, keys_dir)
      Configura las opciones CURVE en un socket REQ de PC3
      ANTES de llamar a connect().
      Carga:
        - keys_dir/client.key_secret  → clave pública + secreta del cliente
        - keys_dir/server.key         → clave pública del servidor (PC2)
      Falla con SystemExit si alguno de los archivos no existe.

MODO ESTRICTO:
    El sistema termina inmediatamente con mensaje descriptivo si falta
    cualquier archivo de clave. No existe modo degradado sin cifrado.
    Ejecutar ``python generar_claves.py`` para crear las claves.
"""

import sys
from pathlib import Path

import zmq
import zmq.auth


def aplicar_curve_cliente(sock: zmq.Socket, keys_dir: Path) -> None:
    """
    Aplica las opciones CURVE al socket REQ ANTES de connect().

    Parámetros
    ----------
    sock : zmq.Socket
        Socket REQ recién creado, antes de cualquier connect().
    keys_dir : Path
        Directorio que contiene ``client.key_secret`` y ``server.key``.

    Lanza
    -----
    SystemExit si alguno de los archivos de clave no existe.
    """
    client_secret_file = keys_dir / "client.key_secret"
    server_public_file = keys_dir / "server.key"

    faltantes = [
        str(f) for f in (client_secret_file, server_public_file)
        if not f.exists()
    ]
    if faltantes:
        print(
            "\n[CRIPTO] ✖  Archivos de clave CurveZMQ no encontrados en PC3:\n"
            + "\n".join(f"           • {f}" for f in faltantes)
            + "\n\n"
            "           Pasos para resolver:\n"
            "           1. Ejecuta en la máquina administradora:\n"
            "                  python generar_claves.py\n"
            "           2. Copia el directorio pc3/keys/ a este nodo PC3.\n"
        )
        sys.exit(1)

    client_pub, client_sec = zmq.auth.load_certificate(str(client_secret_file))
    server_pub, _          = zmq.auth.load_certificate(str(server_public_file))

    sock.curve_publickey = client_pub
    sock.curve_secretkey = client_sec
    sock.curve_serverkey = server_pub

    print("[CRIPTO] Opciones CurveZMQ aplicadas — canal PC3→PC2:5563 cifrado")
