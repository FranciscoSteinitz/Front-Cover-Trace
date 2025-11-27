import snap7_module

if __name__ == "__main__":

    #BD (MySQL) parametros de conexion
    plc_parameters = ['192.168.0.10', 0, 1, 4, 0, 0, 42, 0]
    sql_parameters = ["localhost", "root", "acme2019", "plc_data"]

    """"Instanciar objeto de clase Plc con parametros:
    ip, rack, slot, heartbeat db, heartbeat byte, heartbeat bit
    heartbeat python byte y heartbeat python bit"""

    front_cover_plc = snap7_module.Plc(plc_parameters , sql_parameters)
    if front_cover_plc.start():
        try:
            while True:
                continue
        except KeyboardInterrupt:
            print("\n👋 Cerrando aplicación...")
        finally:
            front_cover_plc.stop()
    else:
        print("❌ No se pudo iniciar la aplicación")