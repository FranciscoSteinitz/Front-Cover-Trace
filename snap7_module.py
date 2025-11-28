import snap7
import snap7.util
import time
import threading
import sqlreg
from datetime import datetime, timedelta
from typing import Callable, Optional

class Plc:
    #Constructor de clase plc
    def __init__(self,
                set_plc:list,
                set_sql:list
                ):
        
        self.ip                     = set_plc[0]
        self.rack                   = set_plc[1]
        self.slot                   = set_plc[2]
        self.hb_db                  = set_plc[3]
        self.hb_byte                = set_plc[4]
        self.hb_bit                 = set_plc[5]
        self.hb_byte_python         = set_plc[6]
        self.hb_bit_python          = set_plc[7]
        self.host_name              = set_sql[0]
        self.user_name              = set_sql[1]
        self.user_password          = set_sql[2]
        self.data_base_name         = set_sql[3]
        self.client                 = snap7.client.Client()
        self.sql_conexion           = None
        self.sql_connection_status  = False  
        self.is_running             = False
        self.is_connected           = False
        self.last_plc_response      = None
        self.connection_callbacks   = []
        self.monitor_thread         = None
        self.heartbeat_thread       = None
        self.station_thread1        = None
        self.station_thread2        = None
        self.contador               = 0
        self.data_recv              = {'OP5_Part_Present' : None,
                                    'OP5_Laser_Mark_Ok' : None,
                                    'OP5_Laser_Mark_Nok' : None,
                                    'OP5_Code_Verify_Ok' : None,
                                    'OP5_Code_Verify_Nok' : None,
                                    'OP5_Code_Read' : None,
                                    'OP5_Code_No_Read' : None,
                                    'OP5_Reserved1' : None,
                                    'OP10_Code_Verify_Ok' : None,
                                    'OP10_Code_Verify_Nok' : None,
                                    'OP10_Code_Read' : None,
                                    'OP10_Code_No_Read' : None,
                                    'OP10_Reserved1' : None,
                                    'OP10_Reserved2' : None,
                                    'OP10_Reserved3' : None,
                                    'OP10_Reserved4' : None,
                                    'OP20_PartMachined_Ok' : None,
                                    'OP20_PartMachined_Nok' : None,
                                    'OP20_FirstPart' : None,
                                    'OP20_PartQC' : None,
                                    'OP20_Reserved1' : None,
                                    'OP20_Reserved2' : None,
                                    'OP20_Reserved3' : None,
                                    'OP20_Reserved4' : None,
                                    'OP20_No_MachineProduced' : None,
                                    'OP30_PartMachined_Ok' : None,
                                    'OP30_PartMachined_Nok' : None,
                                    'OP30_FirstPart' : None,
                                    'OP30_PartQC' : None,
                                    'OP30_Reserved1' : None,
                                    'OP30_Reserved2' : None,
                                    'OP30_Reserved3' : None,
                                    'OP30_Reserved4' : None,
                                    'OP30_No_MachineProduced' : None,
                                    'OP40_PartMachined_Ok' : None,
                                    'OP40_PartMachined_Nok' : None,
                                    'OP40_FirstPart' : None,
                                    'OP40_PartQC' : None,
                                    'OP40_Reserved1' : None,
                                    'OP40_Reserved2' : None,
                                    'OP40_Reserved3' : None,
                                    'OP40_Reserved4' : None,
                                    'OP40_No_MachineProduced' : None,
                                    'OP50_PartMachined_Ok' : None,
                                    'OP50_PartMachined_Nok' : None,
                                    'OP50_FirstPart' : None,
                                    'OP50_PartQC' : None,
                                    'OP50_Reserved1' : None,
                                    'OP50_Reserved2' : None,
                                    'OP50_Reserved3' : None,
                                    'OP50_Reserved4' : None,
                                    'OP50_No_MachineProduced' : None,
                                    'OP60_PartMachined_Ok' : None,
                                    'OP60_PartMachined_Nok' : None,
                                    'OP60_FirstPart' : None,
                                    'OP60_PartQC' : None,
                                    'OP60_Reserved1' : None,
                                    'OP60_Reserved2' : None,
                                    'OP60_Reserved3' : None,
                                    'OP60_Reserved4' : None,
                                    'OP60_No_MachineProduced' : None,
                                    'OP70_PartMachined_Ok' : None,
                                    'OP70_PartMachined_Nok' : None,
                                    'OP70_FirstPart' : None,
                                    'OP70_PartQC' : None,
                                    'OP70_Reserved1' : None,
                                    'OP70_Reserved2' : None,
                                    'OP70_Reserved3' : None,
                                    'OP70_Reserved4' : None,
                                    'OP70_No_MachineProduced' : None,
                                    'OP80_PartMachined_Ok' : None,
                                    'OP80_PartMachined_Nok' : None,
                                    'OP80_FirstPart' : None,
                                    'OP80_PartQC' : None,
                                    'OP80_Reserved1' : None,
                                    'OP80_Reserved2' : None,
                                    'OP80_Reserved3' : None,
                                    'OP80_Reserved4' : None,
                                    'OP80_No_MachineProduced' : None,
                                    'OP90_PartMachined_Ok' : None,
                                    'OP90_PartMachined_Nok' : None,
                                    'OP90_FirstPart' : None,
                                    'OP90_PartQC' : None,
                                    'OP90_Reserved1' : None,
                                    'OP90_Reserved2' : None,
                                    'OP90_Reserved3' : None,
                                    'OP90_Reserved4' : None,
                                    'OP90_No_MachineProduced' : None,
                                    'OP100_PartMachined_Ok' : None,
                                    'OP100_PartMachined_Nok' : None,
                                    'OP100_FirstPart' : None,
                                    'OP100_PartQC' : None,
                                    'OP100_Reserved1' : None,
                                    'OP100_Reserved2' : None,
                                    'OP100_Reserved3' : None,
                                    'OP100_Reserved4' : None,
                                    'OP100_No_MachineProduced' : None,
                                    'JobNumber' : None,
                                    'PartNumberNA' : None,
                                    'PartNumberEESA' : None,
                                    'SerialNumber' : None,
                                    'SupplierID' : None}
        self.data_send              = {'OP5_Part_Present' : None,
                                    'OP5_Laser_Mark_Ok' : None,
                                    'OP5_Laser_Mark_Nok' : None,
                                    'OP5_Code_Verify_Ok' : None,
                                    'OP5_Code_Verify_Nok' : None,
                                    'OP5_Code_Read' : None,
                                    'OP5_Code_No_Read' : None,
                                    'OP5_Reserved1' : None,
                                    'OP10_Code_Verify_Ok' : None,
                                    'OP10_Code_Verify_Nok' : None,
                                    'OP10_Code_Read' : None,
                                    'OP10_Code_No_Read' : None,
                                    'OP10_Reserved1' : None,
                                    'OP10_Reserved2' : None,
                                    'OP10_Reserved3' : None,
                                    'OP10_Reserved4' : None,
                                    'OP20_PartMachined_Ok' : None,
                                    'OP20_PartMachined_Nok' : None,
                                    'OP20_FirstPart' : None,
                                    'OP20_PartQC' : None,
                                    'OP20_Reserved1' : None,
                                    'OP20_Reserved2' : None,
                                    'OP20_Reserved3' : None,
                                    'OP20_Reserved4' : None,
                                    'OP20_No_MachineProduced' : None,
                                    'OP30_PartMachined_Ok' : None,
                                    'OP30_PartMachined_Nok' : None,
                                    'OP30_FirstPart' : None,
                                    'OP30_PartQC' : None,
                                    'OP30_Reserved1' : None,
                                    'OP30_Reserved2' : None,
                                    'OP30_Reserved3' : None,
                                    'OP30_Reserved4' : None,
                                    'OP30_No_MachineProduced' : None,
                                    'OP40_PartMachined_Ok' : None,
                                    'OP40_PartMachined_Nok' : None,
                                    'OP40_FirstPart' : None,
                                    'OP40_PartQC' : None,
                                    'OP40_Reserved1' : None,
                                    'OP40_Reserved2' : None,
                                    'OP40_Reserved3' : None,
                                    'OP40_Reserved4' : None,
                                    'OP40_No_MachineProduced' : None,
                                    'OP50_PartMachined_Ok' : None,
                                    'OP50_PartMachined_Nok' : None,
                                    'OP50_FirstPart' : None,
                                    'OP50_PartQC' : None,
                                    'OP50_Reserved1' : None,
                                    'OP50_Reserved2' : None,
                                    'OP50_Reserved3' : None,
                                    'OP50_Reserved4' : None,
                                    'OP50_No_MachineProduced' : None,
                                    'OP60_PartMachined_Ok' : None,
                                    'OP60_PartMachined_Nok' : None,
                                    'OP60_FirstPart' : None,
                                    'OP60_PartQC' : None,
                                    'OP60_Reserved1' : None,
                                    'OP60_Reserved2' : None,
                                    'OP60_Reserved3' : None,
                                    'OP60_Reserved4' : None,
                                    'OP60_No_MachineProduced' : None,
                                    'OP70_PartMachined_Ok' : None,
                                    'OP70_PartMachined_Nok' : None,
                                    'OP70_FirstPart' : None,
                                    'OP70_PartQC' : None,
                                    'OP70_Reserved1' : None,
                                    'OP70_Reserved2' : None,
                                    'OP70_Reserved3' : None,
                                    'OP70_Reserved4' : None,
                                    'OP70_No_MachineProduced' : None,
                                    'OP80_PartMachined_Ok' : None,
                                    'OP80_PartMachined_Nok' : None,
                                    'OP80_FirstPart' : None,
                                    'OP80_PartQC' : None,
                                    'OP80_Reserved1' : None,
                                    'OP80_Reserved2' : None,
                                    'OP80_Reserved3' : None,
                                    'OP80_Reserved4' : None,
                                    'OP80_No_MachineProduced' : None,
                                    'OP90_PartMachined_Ok' : None,
                                    'OP90_PartMachined_Nok' : None,
                                    'OP90_FirstPart' : None,
                                    'OP90_PartQC' : None,
                                    'OP90_Reserved1' : None,
                                    'OP90_Reserved2' : None,
                                    'OP90_Reserved3' : None,
                                    'OP90_Reserved4' : None,
                                    'OP90_No_MachineProduced' : None,
                                    'OP100_PartMachined_Ok' : None,
                                    'OP100_PartMachined_Nok' : None,
                                    'OP100_FirstPart' : None,
                                    'OP100_PartQC' : None,
                                    'OP100_Reserved1' : None,
                                    'OP100_Reserved2' : None,
                                    'OP100_Reserved3' : None,
                                    'OP100_Reserved4' : None,
                                    'OP100_No_MachineProduced' : None,
                                    'JobNumber' : None,
                                    'PartNumberNA' : None,
                                    'PartNumberEESA' : None,
                                    'SerialNumber' : None,
                                    'SupplierID' : None}

        self.mem_value              = [False,False,False,False,False,False]  # Valores de memoria para p_trig

    def establecer_conexion(self):
        #Establecer conexión con el PLC
        try:
            print(f"🔌 Intentando conectar a {self.ip} (Rack {self.rack}, Slot {self.slot})...")
            self.client.connect(self.ip, self.rack, self.slot)
            # Configurar heartbeat inicial - apagar primero
            self._write_python_heartbeat(True)
            time.sleep(0.1)
            print('Conectado')
            # Verificar que PLC responde
            if self._check_plc_response():
                self.is_connected = True
                self.last_plc_response = datetime.now()
                print("✅ Conexión establecida y heartbeat iniciado")
                return True
            else:
                print("⚠️  Conexión establecida pero PLC no responde al heartbeat")
                self.client.disconnect()
                return False
        except Exception as e:
            print(f"❌ Error de conexión: {e}")
            self.is_connected = False
            return False
        
    def _write_python_heartbeat(self, state:bool):
        #Escribir bit de heartbeat en DB para Python en PLC
        try:
            # Leer byte actual
            data = self.client.db_read(self.hb_db, self.hb_byte_python, 1)
            # Modificar nuestro bit
            snap7.util.set_bool(data, 0, self.hb_bit_python, state)
            # Escribir byte modificado
            self.client.db_write(self.hb_db, self.hb_byte_python, data)
        except Exception as e:
            raise Exception(f"Error escribiendo heartbeat Python: {e}")

    def _check_plc_response(self) -> bool:
        #Verificar si el PLC está respondiendo al heartbeat
        try:
            # Leer respuesta del PLC
            plc_hb = self._read_plc_heartbeat()
            if plc_hb:
                self.last_plc_response = datetime.now()
            return plc_hb
        except Exception:
            return False

    def _read_plc_heartbeat(self) -> bool:
        #Leer bit de heartbeat del PLC en DB2171.DBX2.0
        try:
            data = self.client.db_read(self.hb_db, self.hb_byte, 1)
            return snap7.util.get_bool(data, 0, self.hb_bit)
        except Exception as e:
            raise Exception(f"Error leyendo heartbeat PLC: {e}")
  
    def _heartbeat_loop(self):
        """Loop principal de heartbeat y monitoreo de conexión"""
        hb_state = False
        last_hb_toggle = datetime.now()
        hb_interval = 1.0  # Toggle cada 1 segundo
        while self.is_running:
            try:
                if not self.is_connected:
                    # Intentar reconexión
                    if self.establecer_conexion():
                        hb_state = True
                        last_hb_toggle = datetime.now()
                    else:
                        time.sleep(2)  # Esperar antes de reintentar
                        continue

                # Procesar si la conexion está activa
                if self.is_connected:

                    #Crear conexion a SQL
                    if not self.sql_connection_status:
                        self.sql_conexion = sqlreg.create_server_connection(self.host_name, self.user_name, self.user_password)
                        sqlreg.create_database(self.sql_conexion, f"CREATE DATABASE IF NOT EXISTS {self.data_base_name}", self.data_base_name)
                    if self.sql_conexion:
                        self.sql_connection_status = True
                    else:
                        self.sql_connection_status = False
                    
                    # Parametros de estaciones de trabajo
                    """"DB Incoming Offset
                        Byte Incoming Offset
                        Bit Incoming Offset
                        Memory Index
                        DB Outgoing Offset
                        Byte Outgoing Offset
                        Bit Outgoing Offset
                        Memory Index 
                        DB Read Offset
                        Start Byte Read Offset
                        Length Read
                        DB Incoming ack Offset
                        Byte Incoming ack Offset
                        Bit Incoming ack Offset
                        DB Outgoing ack Offset
                        Byte Outgoing ack Offset
                        Bit Outgoing ack Offset"""
                    station_05 = [4, 2, 0, 0, 0, 0, 0, 0, 3, 0, 60, 4, 44, 0, 0, 0, 0]
                    station_10 = [4, 4, 1, 1, 0, 0, 0, 0, 3, 60, 60, 4, 46, 1, 0, 0, 0]
                    #station_30_QC = []

                    self.station_thread1 = threading.Thread(target=self.station(station_05, True, False, False, False), daemon=True)
                    self.station_thread2 = threading.Thread(target=self.station(station_10, False, True, False, False), daemon=True)
                    self.station_thread1.start()
                    self.station_thread2.start()

                current_time = datetime.now()
               
                # 1. Toggle nuestro heartbeat periódicamente
                if (current_time - last_hb_toggle) >= timedelta(seconds=hb_interval):
                    hb_state = not hb_state
                    self._write_python_heartbeat(hb_state)
                    last_hb_toggle = current_time
                    # print(f"💓 Python Heartbeat: {hb_state}")  # Debug
               
                # 2. Verificar respuesta del PLC
                plc_responding = self._check_plc_response()
               
                # 3. Verificar timeout
                if self.last_plc_response and \
                   (current_time - self.last_plc_response) > timedelta(seconds=5):
                    print("⏰ Timeout - PLC no responde")
                    self.is_connected = False
                    self.client.disconnect()
                    continue
               
                # 4. Esperar hasta siguiente ciclo
                time.sleep(1)
               
            except Exception as e:
                print(f"❌ Error en heartbeat loop: {e}")
                self.is_connected = False
                try:
                    self.client.disconnect()
                except:
                    pass
                time.sleep(1)
   
    def start(self):
        #Iniciar el sistema de monitoreo
        if self.is_running:
            print("⚠️  El monitor ya está ejecutándose")
            return False
       
    
        self.is_running = True
        self.heartbeat_thread = threading.Thread(target=self._heartbeat_loop, daemon=True)
        self.heartbeat_thread.start()
        print("🚀 Sistema de heartbeat iniciado")
        print(f"📍 Configuración:")
        print(f"   - IP: {self.ip}")
        print(f"   - Rack: {self.rack}, Slot: {self.slot}")
        print(f"   - DB: {self.hb_db}")
        print(f"   - Python escribe: DB{self.hb_db}.DBX{self.hb_byte_python}.{self.hb_bit_python}")
        print(f"   - PLC responde: DB{self.hb_db}.DBX{self.hb_byte}.{self.hb_bit}")
        return True

    def stop(self):
        #Detener el sistema de monitoreo
        self.is_running = False
    
        # Apagar heartbeat antes de desconectar
        if self.is_connected:
            try:
                self._write_python_heartbeat(False)
                time.sleep(0.1)
            except:
                pass
    
        if self.heartbeat_thread:
            self.heartbeat_thread.join(timeout=5)
    
        try:
            self.client.disconnect()
        except:
            pass
        self.is_connected = False
        print("🛑 Sistema de heartbeat detenido") 

    def p_trig(self, signal, mem_index:int):
        #Función de trigger para pruebas
        if self.mem_value[mem_index] == False and signal == True:
            print("🔔 Trigger activado")
            self.mem_value[mem_index] = True
            return True
        if self.mem_value[mem_index] == True and signal == False:
            print("🔔 Trigger desactivado")
            self.mem_value[mem_index] = False
            return False

    def write_bit(self, db_number: int, byte_offset: int, bit_offset: int, value: bool) -> bool:
        #Escribir un bit en el PLC (solo si hay conexión)"""
        if not self.is_connected:
            print("⚠️  No conectado - No se puede escribir")
            return False
    
        try:
            data = self.client.db_read(db_number, byte_offset, 1)
            snap7.util.set_bool(data, 0, bit_offset, value)
            self.client.db_write(db_number, byte_offset, data)
            return True
        except Exception as e:
            print(f"❌ Error escribiendo bit: {e}")
            return False

    def read_bit(self, db_number: int, byte_offset: int, bit_offset: int) -> Optional[bool]:
        #Leer un bit del PLC (solo si hay conexión)
        if not self.is_connected:
            print("⚠️  No conectado - No se puede leer")
            return None
        try:
            data = self.client.db_read(db_number, byte_offset, 1)
            return snap7.util.get_bool(data, 0, bit_offset)
        except Exception as e:
            print(f"❌ Error leyendo bit: {e}")
            return None

    def read_db(self, db_number: int, byte_offset: int, byte_count: int):
            #Leer un numero de bytes especificos de un DB.
            try:
                data = self.client.db_read(db_number, byte_offset, byte_count)
                print(f"Leido del DB{db_number}, byte {byte_offset}: {data.hex()}")
                print(data)
                return data
            except Exception as e:
                print(f"Error al leer DB: {e}")
                return None

    def recv_data_operationbits(self, raw_data:bytearray, decode_data:dict):
        #Debug de logica que no se use despues de completar programa incluido parametro de funciones ya no llamados
        #Lista con mascaras en formato byte para cada uno de los 8 bit en un byte
        mask = [b'\x01',b'\x02',b'\x04',b'\x08',b'\x10',b'\x20',b'\x40',b'\x80']

        #Offset de bits inicales en PLC por operacion
        op_offset = {'OP5_': 0,
                     'OP10_':1,
                     'OP20_':2,
                     'OP30_':4,
                     'OP40_':6,
                     'OP50_':8,
                     'OP60_':10,
                     'OP70_':12,
                     'OP80_':14,
                     'OP90_':16,
                     'OP100_':18}
        
        #Convertir datos de la mascara a enteros para procesar operaciones AND
        int_mask_list = []
        for int_mask_0 in mask:
            int_mask_list.append(int.from_bytes(int_mask_0,byteorder='big'))

        #Filtro de operaciones
        #op_key = filter(lambda clave: clave.startswith(operation), decode_data)
        
        for op in op_offset:
            key = filter(lambda clave: clave.startswith(op), decode_data)
            for columna in key:
                if self.contador <= 7:
                    decode_data[columna] = 1 if raw_data[op_offset[op]] & int_mask_list[self.contador] else 0
                    #print(columna, decode_data[columna])
                    self.contador += 1
            self.contador = 0
        self.contador = 0
        #Recorrer claves filtradas y asignar valor al diccionario de salida dependiendo del offset de operacion y valor leido del PLC
        # if operation == 'OP5_' or operation == 'OP10_':
        #     for columna in op_key:
        #         decode_data[columna] = 1 if raw_data[op_offset[operation]] & int_mask_list[self.contador] else 0
        #         self.contador += 1
        # if operation != 'OP5_' and operation != 'OP10_':
        #     for columna in op_key:
        #         if self.contador <= 7:
        #             decode_data[columna] = 1 if raw_data[op_offset[operation]] & int_mask_list[self.contador] else 0
        #             self.contador += 1
        return decode_data 

    def recv_data_NoMachineProd (self, raw_data:bytearray, decode_data:dict):

        #Offset de bits inicales en PLC por operacion
        op_offset = {'OP5_': 0,
                     'OP10_':1,
                     'OP20_':3,
                     'OP30_':5,
                     'OP40_':7,
                     'OP50_':9,
                     'OP60_':11,
                     'OP70_':13,
                     'OP80_':15,
                     'OP90_':17,
                     'OP100_':19}
        number_machine_str = 'No_MachineProduced'

        for op in op_offset:
            key = filter(lambda clave: clave.startswith(op+number_machine_str), decode_data)
            for columna in key:
                decode_data[columna] = raw_data[op_offset[op]]
                #print(columna, decode_data[columna], raw_data[op_offset[op]])

    def recv_data_part(self, raw_data:bytearray, decode_data:dict):
        raw_data_str = raw_data.decode('utf-8') 
        part_Number_NA  = '' 
        part_Number_EE  = ''
        serial_Number   = ''
        supplier_ID     = ''
        data_offset = { 'job_Number'        :20,
                        'part_Number_NA'    :22,
                        'part_Number_EE'    :32,
                        'serial_Number'     :43,
                        'supplier_ID'       :53,
                        'fin'               :60}
        
        for pNNA in range(data_offset['part_Number_NA'], data_offset['part_Number_EE']):
            part_Number_NA += raw_data_str[pNNA]
        for pNEE in range(data_offset['part_Number_EE'], data_offset['serial_Number']):
            part_Number_EE += raw_data_str[pNEE]
        for sn in range(data_offset['serial_Number'], data_offset['supplier_ID']):
            serial_Number += raw_data_str[sn]
        for sID in range(data_offset['supplier_ID'], data_offset['fin']):
            supplier_ID += raw_data_str[sID]
        
        decode_data['JobNumber']        = snap7.util.get_int(raw_data,data_offset['job_Number'])
        decode_data['PartNumberNA']     = part_Number_NA
        decode_data['PartNumberEESA']   = part_Number_EE
        decode_data['SerialNumber']     = serial_Number
        decode_data['SupplierID']       = supplier_ID

    def send_data_to_PLC(self, encode_data:dict):
        #Enviar datos al PLC desde el diccionario interno Send
        outgoing_data = bytearray(60)
        #Asignar valores al bytearray outgoing_data desde el diccionario data_send

        #OP20 Machine Produced Count
        snap7.util.set_byte(outgoing_data, 3, encode_data['OP20_No_MachineProduced'])
        #OP30 Machine Produced Count
        snap7.util.set_byte(outgoing_data, 5, encode_data['OP30_No_MachineProduced'])
        #OP40 Machine Produced Count
        snap7.util.set_byte(outgoing_data, 7, encode_data['OP40_No_MachineProduced'])
        #OP50 Machine Produced Count
        snap7.util.set_byte(outgoing_data, 9, encode_data['OP50_No_MachineProduced'])
        #OP60 Machine Produced Count
        snap7.util.set_byte(outgoing_data, 11, encode_data['OP60_No_MachineProduced'])
        #OP70 Machine Produced Count
        snap7.util.set_byte(outgoing_data, 13, encode_data['OP70_No_MachineProduced'])
        #OP80 Machine Produced Count
        snap7.util.set_byte(outgoing_data, 15, encode_data['OP80_No_MachineProduced'])
        #OP90 Machine Produced Count
        snap7.util.set_byte(outgoing_data, 17, encode_data['OP90_No_MachineProduced'])
        #OP100 Machine Produced Count
        snap7.util.set_byte(outgoing_data, 19, encode_data['OP100_No_MachineProduced'])

        #OP5 Operation Bits
        snap7.util.set_byte(outgoing_data, 0,
            (encode_data['OP5_Part_Present'] << 0) |
            (encode_data['OP5_Laser_Mark_Ok'] << 1) |
            (encode_data['OP5_Laser_Mark_Nok'] << 2) |
            (encode_data['OP5_Code_Verify_Ok'] << 3) |
            (encode_data['OP5_Code_Verify_Nok'] << 4) |
            (encode_data['OP5_Code_Read'] << 5) |
            (encode_data['OP5_Code_No_Read'] << 6) |
            (encode_data['OP5_Reserved1'] << 7)
        )  

        #OP10 Operation Bits
        snap7.util.set_byte(outgoing_data, 1,
            (encode_data['OP10_Code_Verify_Ok'] << 0) |
            (encode_data['OP10_Code_Verify_Nok'] << 1) |
            (encode_data['OP10_Code_Read'] << 2) |
            (encode_data['OP10_Code_No_Read'] << 3) |
            (encode_data['OP10_Reserved1'] << 4) |
            (encode_data['OP10_Reserved2'] << 5) |
            (encode_data['OP10_Reserved3'] << 6) |
            (encode_data['OP10_Reserved4'] << 7)
        )

        #OP20 Operation Bits
        snap7.util.set_byte(outgoing_data, 2,
            (encode_data['OP20_PartMachined_Ok'] << 0) |
            (encode_data['OP20_PartMachined_Nok'] << 1) |
            (encode_data['OP20_FirstPart'] << 2) |
            (encode_data['OP20_PartQC'] << 3) |
            (encode_data['OP20_Reserved1'] << 4) |
            (encode_data['OP20_Reserved2'] << 5) |
            (encode_data['OP20_Reserved3'] << 6) |
            (encode_data['OP20_Reserved4'] << 7)
        )

        #OP30 Operation Bits
        snap7.util.set_byte(outgoing_data, 4,
            (encode_data['OP30_PartMachined_Ok'] << 0) |
            (encode_data['OP30_PartMachined_Nok'] << 1) |
            (encode_data['OP30_FirstPart'] << 2) |
            (encode_data['OP30_PartQC'] << 3) |
            (encode_data['OP30_Reserved1'] << 4) |
            (encode_data['OP30_Reserved2'] << 5) |
            (encode_data['OP30_Reserved3'] << 6) |
            (encode_data['OP30_Reserved4'] << 7)
        )

        #OP40 Operation Bits
        snap7.util.set_byte(outgoing_data, 6,
            (encode_data['OP40_PartMachined_Ok'] << 0) |
            (encode_data['OP40_PartMachined_Nok'] << 1) |
            (encode_data['OP40_FirstPart'] << 2) |
            (encode_data['OP40_PartQC'] << 3) |
            (encode_data['OP40_Reserved1'] << 4) |
            (encode_data['OP40_Reserved2'] << 5) |
            (encode_data['OP40_Reserved3'] << 6) |
            (encode_data['OP40_Reserved4'] << 7)
        )

        #OP50 Operation Bits
        snap7.util.set_byte(outgoing_data, 8,
            (encode_data['OP50_PartMachined_Ok'] << 0) |
            (encode_data['OP50_PartMachined_Nok'] << 1) |
            (encode_data['OP50_FirstPart'] << 2) |
            (encode_data['OP50_PartQC'] << 3) |
            (encode_data['OP50_Reserved1'] << 4) |
            (encode_data['OP50_Reserved2'] << 5) |
            (encode_data['OP50_Reserved3'] << 6) |
            (encode_data['OP50_Reserved4'] << 7)
        )

        #OP60 Operation Bits
        snap7.util.set_byte(outgoing_data, 10,
            (encode_data['OP60_PartMachined_Ok'] << 0) |
            (encode_data['OP60_PartMachined_Nok'] << 1) |
            (encode_data['OP60_FirstPart'] << 2) |
            (encode_data['OP60_PartQC'] << 3) |
            (encode_data['OP60_Reserved1'] << 4) |
            (encode_data['OP60_Reserved2'] << 5) |
            (encode_data['OP60_Reserved3'] << 6) |
            (encode_data['OP60_Reserved4'] << 7)
        )   

        #OP70 Operation Bits
        snap7.util.set_byte(outgoing_data, 12,
            (encode_data['OP70_PartMachined_Ok'] << 0) |
            (encode_data['OP70_PartMachined_Nok'] << 1) |
            (encode_data['OP70_FirstPart'] << 2) |
            (encode_data['OP70_PartQC'] << 3) |
            (encode_data['OP70_Reserved1'] << 4) |
            (encode_data['OP70_Reserved2'] << 5) |
            (encode_data['OP70_Reserved3'] << 6) |
            (encode_data['OP70_Reserved4'] << 7)
        )

        #OP80 Operation Bits
        snap7.util.set_byte(outgoing_data, 14,
            (encode_data['OP80_PartMachined_Ok'] << 0) |
            (encode_data['OP80_PartMachined_Nok'] << 1) |
            (encode_data['OP80_FirstPart'] << 2) |
            (encode_data['OP80_PartQC'] << 3) |
            (encode_data['OP80_Reserved1'] << 4) |
            (encode_data['OP80_Reserved2'] << 5) |
            (encode_data['OP80_Reserved3'] << 6) |
            (encode_data['OP80_Reserved4'] << 7)
        )

        #OP90 Operation Bits
        snap7.util.set_byte(outgoing_data, 16,
            (encode_data['OP90_PartMachined_Ok'] << 0) |
            (encode_data['OP90_PartMachined_Nok'] << 1) |
            (encode_data['OP90_FirstPart'] << 2) |
            (encode_data['OP90_PartQC'] << 3) |
            (encode_data['OP90_Reserved1'] << 4) |
            (encode_data['OP90_Reserved2'] << 5) |
            (encode_data['OP90_Reserved3'] << 6) |
            (encode_data['OP90_Reserved4'] << 7)
        )

        #OP100 Operation Bits
        snap7.util.set_byte(outgoing_data, 18,
            (encode_data['OP100_PartMachined_Ok'] << 0) |
            (encode_data['OP100_PartMachined_Nok'] << 1) |
            (encode_data['OP100_FirstPart'] << 2) |
            (encode_data['OP100_PartQC'] << 3) |
            (encode_data['OP100_Reserved1'] << 4) |
            (encode_data['OP100_Reserved2'] << 5) |
            (encode_data['OP100_Reserved3'] << 6) |
            (encode_data['OP100_Reserved4'] << 7)
        )

        #Job Number
        snap7.util.set_int(outgoing_data, 20, encode_data['JobNumber'])
        #Part Number NA
        part_number_na_bytes = encode_data['PartNumberNA'].encode('utf-8')
        outgoing_data[22:32] = part_number_na_bytes.ljust(10, b'\x00')
        #Part Number EESA
        part_number_ee_bytes = encode_data['PartNumberEESA'].encode('utf-8')
        outgoing_data[32:43] = part_number_ee_bytes.ljust(11, b'\x00')
        #Serial Number
        serial_number_bytes = encode_data['SerialNumber'].encode('utf-8')
        outgoing_data[43:53] = serial_number_bytes.ljust(10, b'\x00')
        #Supplier ID
        supplier_id_bytes = encode_data['SupplierID'].encode('utf-8')
        outgoing_data[53:60] = supplier_id_bytes.ljust(7, b'\x00')
        self.client.db_write(3, 0, outgoing_data)

        return outgoing_data

    def station(self, set_station:list, store:bool, update:bool, search:bool, delete:bool):
        db_number_incoming          = set_station[0]
        incoming_byte_offset        = set_station[1]
        incoming_bit_offset         = set_station[2]
        incoming_mem_index          = set_station[3]
        db_number_outgoing          = set_station[4]
        outgoing_byte_offset        = set_station[5]
        outgoing_bit_offset         = set_station[6]
        outgoing_mem_index          = set_station[7]
        db_read                     = set_station[8]
        read_start                  = set_station[9]
        read_size                   = set_station[10]
        ack_incoming_db_offset      = set_station[11]
        ack_incoming_byte_offset    = set_station[12]
        ack_incoming_bit_offset     = set_station[13]
        ack_outgoing_db_offset      = set_station[14]
        ack_outgoing_byte_offset    = set_station[15]
        ack_outgoing_bit_offset     = set_station[16]

        #Estacion solo guarda datos en SQL
        if store:
            dict_store = self.data_recv
            #Detectar flanco de dato para almacenar del PLC en DB?.DBX?.?
            if self.p_trig(self.read_bit(db_number_incoming, incoming_byte_offset, incoming_bit_offset), incoming_mem_index):
                #Leer dato de PLC para enviar a MySQL
                store_data = self.client.db_read(db_read, read_start, read_size)
                print(f"🗣️ Dato recibido desde el PLC")

                #Extraer datos recibidos del PLC y asignar a diccionario interno Recv
                self.recv_data_operationbits(store_data, dict_store)  #Remover parametro operacion, ya no es utilixzado por la funcion
                self.recv_data_NoMachineProd(store_data, dict_store)
                self.recv_data_part(store_data, dict_store)
                print(f"📥 Dato recibido del PLC para registrar en MySQL: {dict_store["SerialNumber"]}")

                #Enviar dato a MySQL
                if self.sql_connection_status:

                    #Buscar dato recibido del plc en MySQL si no existe registro previo de serial number
                    dato_buscado = sqlreg.search_data(self.sql_conexion, 'front_cover', 'SerialNumber', dict_store["SerialNumber"])

                    if dato_buscado == None or dato_buscado == 3:
                        print("🔍 Dato no registrado en MySQL, procediendo a insertar...")
                        sqlreg.recv_Data_To_MySQL(self.sql_conexion, 'front_cover', dict_store)
                        print("✅ Dato insertado en MySQL")
                        #Comprobar insercion
                        dato_verificado = sqlreg.search_data(self.sql_conexion, 'front_cover', 'SerialNumber', dict_store["SerialNumber"])
                        if dato_verificado == 1 or dato_verificado == 2:
                            print("✅ Inserción verificada en MySQL")
                        else:
                            print("❌ Error de verificación en MySQL después de inserción")
                        #Borrar dato del diccionario interno Send para proxima lectura
                        dict_store = {key: None for key in dict_store}
                        #Borrar dato del bytearray de salida
                        store_data = bytearray(60)
                        #Borrar dato verificado
                        dato_verificado = None
                        #Borrar dato buscado
                        dato_buscado = None
                    else:
                        print("⚠️  Dato ya existente en MySQL, no se inserta registro duplicado")
                
                #outgoing_data = b'223145DATA'  # Ejemplo de dato a enviar
                print(f"Informar al PLC que el dato fue almacenado")
                self.write_bit(ack_incoming_db_offset, ack_incoming_byte_offset, ack_incoming_bit_offset,True)  # Acknowledge
                time.sleep(0.1)
                self.write_bit(ack_incoming_db_offset, ack_incoming_byte_offset, ack_incoming_bit_offset,False) # Reset Acknowledge
        if update:
            dict_update = self.data_recv
            #Detectar flanco de dato para actualizar en SQL desde PLC
            if self.p_trig(self.read_bit(db_number_incoming, incoming_byte_offset, incoming_bit_offset), incoming_mem_index):
                update_data = self.client.db_read(db_read, read_start, read_size)
                
                #Extraer datos recibidos del PLC y asignar a diccionario interno Recv
                self.recv_data_operationbits(update_data, dict_update)
                self.recv_data_NoMachineProd(update_data, dict_update)
                self.recv_data_part(update_data, dict_update)
                print(f"📥 Dato recibido del PLC para actualizar en MySQL: {dict_update["SerialNumber"]}")

                #Actualizar dato en MySQL
                if self.sql_connection_status:
                    print("🔄 Procediendo a actualizar dato en MySQL...")
                    sqlreg.update_part_data(self.sql_conexion, 'front_cover', dict_update)
                    print("✅ Dato actualizado en MySQL")
                    #Borrar dato del diccionario interno Send para proxima lectura
                    dict_update = {key: None for key in dict_update}
                    #Borrar dato del bytearray de salida
                    update_data = bytearray(60)

                print(f"Informar al PLC que el dato fue actualizado")
                self.write_bit(ack_incoming_db_offset, ack_incoming_byte_offset, ack_incoming_bit_offset,True)  # Acknowledge
                time.sleep(0.1)
                self.write_bit(ack_incoming_db_offset, ack_incoming_byte_offset, ack_incoming_bit_offset,False) # Reset Acknowledge
        if search:
            pass
             #Detectar flanco de dato entrante en DB4.DBX0.2
            # if self.p_trig(self.read_bit(db_number_incoming, incoming_byte_offset, incoming_bit_offset), incoming_mem_index):
            #     incoming_data = self.client.db_read(db_read, read_start, read_size)
            #     print(f"🗣️ Informar al PLC que el dato fue recibido")
                
            #     #Extraer datos recibidos del PLC y asignar a diccionario interno Recv
            #     self.recv_data_part(incoming_data, self.data_recv)
            #     print(f"📥 Dato recibido del PLC: {self.data_recv}")

            #     #Buscar dato en MySQL si no existe registro previo de serial number
            #     if self.sql_connection_status:
                    
            #         dato_buscado = sqlreg.search_data(self.sql_conexion, 'front_cover', 'SerialNumber', self.data_recv["SerialNumber"]) 
            #         print(f"🔍 Resultado de búsqueda en MySQL: {dato_buscado}")
            #         if dato_buscado == 1 or dato_buscado == 2:
            #             print("⚠️  Datos de pieza encontrados en MySQL, obteniendo datos...")
            #             sqlreg.get_Data_From_MySQL(self.sql_conexion, 'front_cover', self.data_recv, self.data_recv["SerialNumber"])
            #             print("✅ Dato obtenido de MySQL")
            #             print("🔄 Procediendo a cargar al PLC")
            #             print(f"📤 Enviando dato al PLC: {self.data_recv}")
            #             send_data = self.send_data_to_PLC(self.data_recv)
            #             print("✅ Dato enviado al PLC")
            #             #Verificar que el dato fue cargado correctamente al PLC
            #             verify_data = bytearray(60)
            #             verify_data = self.client.db_read(db_read, read_start, read_size)
            #             #Comparar dato enviado con dato leido
            #             if verify_data == send_data:
            #                 print(f"✅ Dato verificado en PLC: {verify_data.decode('UTF-8')}")
            #                 #Borrar fila en MySQL despues de cargar al PLC
            #                 sqlreg.delete_data(self.sql_conexion, 'front_cover', 'SerialNumber', self.data_recv["SerialNumber"])
            #                 #Borrar dato del diccionario interno Recv para proxima lectura
            #                 self.data_recv = {key: None for key in self.data_recv}
            #                 #Borrar dato del bytearray de verificacion
            #                 verify_data = bytearray(60)
            #                 #Borrar dato enviado
            #                 send_data = bytearray(60)
            #             else:
            #                 print(f"❌ Error de verificación en PLC. Dato leído: {verify_data.decode('UTF-8')}")
            #         if dato_buscado == 3:
            #             print("⚠️  Dato no registrado en MySQL, no se cargan datos al PLC")

            #     #Informar al Plc de dato recibido
            #     self.write_bit(ack_incoming_db_offset, ack_incoming_byte_offset, ack_incoming_bit_offset,True)  # Acknowledge
            #     time.sleep(0.1)
            #     self.write_bit(ack_incoming_db_offset, ack_incoming_byte_offset, ack_incoming_bit_offset,False) # Reset Acknowledge
        if delete:
            pass

# if __name__ == "__main__":

#     """"Instanciar objeto de clase Plc con parametros:
#     ip, rack, slot, heartbeat db, heartbeat byte, heartbeat bit
#     heartbeat python byte y heartbeat python bit"""

#     front_cover_plc     = Plc('192.168.0.10', 0, 1, 4, 0, 0, 2, 0)
#     if front_cover_plc.start():
#         try:
#             while True:
#                 continue
#         except KeyboardInterrupt:
#             print("\n👋 Cerrando aplicación...")
#         finally:
#             front_cover_plc.stop()
#     else:
#         print("❌ No se pudo iniciar la aplicación")