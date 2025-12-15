import snap7
import snap7.util
import time
from multiprocessing import Process, Queue, Manager, Event, Lock, cpu_count, Pool
import sqlreg
from datetime import datetime, timedelta
from typing import Callable, Optional
import signal
import sys
import pickle
import traceback
import socket
import errno

# ========== CLASE PLC CON DATOS SERIALIZABLES ==========

class Plc:
    # Constructor de clase plc
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
        
        # Diccionario de datos completo para serialización
        self.data_recv_keys = [
            'OP5_Part_Present', 'OP5_Laser_Mark_Ok', 'OP5_Laser_Mark_Nok',
            'OP5_Code_Verify_Ok', 'OP5_Code_Verify_Nok', 'OP5_Code_Read',
            'OP5_Code_No_Read', 'OP5_Reserved1', 'OP10_Code_Verify_Ok',
            'OP10_Code_Verify_Nok', 'OP10_Code_Read', 'OP10_Code_No_Read',
            'OP10_Reserved1', 'OP10_Reserved2', 'OP10_Reserved3',
            'OP10_Reserved4', 'OP20_PartMachined_Ok', 'OP20_PartMachined_Nok',
            'OP20_FirstPart', 'OP20_PartQC', 'OP20_Reserved1',
            'OP20_Reserved2', 'OP20_Reserved3', 'OP20_Reserved4',
            'OP20_No_MachineProduced', 'OP30_PartMachined_Ok',
            'OP30_PartMachined_Nok', 'OP30_FirstPart', 'OP30_PartQC',
            'OP30_Reserved1', 'OP30_Reserved2', 'OP30_Reserved3',
            'OP30_Reserved4', 'OP30_No_MachineProduced', 'OP40_PartMachined_Ok',
            'OP40_PartMachined_Nok', 'OP40_FirstPart', 'OP40_PartQC',
            'OP40_Reserved1', 'OP40_Reserved2', 'OP40_Reserved3',
            'OP40_Reserved4', 'OP40_No_MachineProduced', 'OP50_PartMachined_Ok',
            'OP50_PartMachined_Nok', 'OP50_FirstPart', 'OP50_PartQC',
            'OP50_Reserved1', 'OP50_Reserved2', 'OP50_Reserved3',
            'OP50_Reserved4', 'OP50_No_MachineProduced', 'OP60_PartMachined_Ok',
            'OP60_PartMachined_Nok', 'OP60_FirstPart', 'OP60_PartQC',
            'OP60_Reserved1', 'OP60_Reserved2', 'OP60_Reserved3',
            'OP60_Reserved4', 'OP60_No_MachineProduced', 'OP70_PartMachined_Ok',
            'OP70_PartMachined_Nok', 'OP70_FirstPart', 'OP70_PartQC',
            'OP70_Reserved1', 'OP70_Reserved2', 'OP70_Reserved3',
            'OP70_Reserved4', 'OP70_No_MachineProduced', 'OP80_PartMachined_Ok',
            'OP80_PartMachined_Nok', 'OP80_FirstPart', 'OP80_PartQC',
            'OP80_Reserved1', 'OP80_Reserved2', 'OP80_Reserved3',
            'OP80_Reserved4', 'OP80_No_MachineProduced', 'OP90_PartMachined_Ok',
            'OP90_PartMachined_Nok', 'OP90_FirstPart', 'OP90_PartQC',
            'OP90_Reserved1', 'OP90_Reserved2', 'OP90_Reserved3',
            'OP90_Reserved4', 'OP90_No_MachineProduced', 'OP100_PartMachined_Ok',
            'OP100_PartMachined_Nok', 'OP100_FirstPart', 'OP100_PartQC',
            'OP100_Reserved1', 'OP100_Reserved2', 'OP100_Reserved3',
            'OP100_Reserved4', 'OP100_No_MachineProduced', 'JobNumber',
            'PartNumberNA', 'PartNumberEESA', 'SerialNumber', 'SupplierID'
        ]
        
        # Datos que SÍ pueden serializarse
        self.config_data = {
            'ip': self.ip,
            'rack': self.rack,
            'slot': self.slot,
            'hb_db': self.hb_db,
            'hb_byte': self.hb_byte,
            'hb_bit': self.hb_bit,
            'hb_byte_python': self.hb_byte_python,
            'hb_bit_python': self.hb_bit_python,
            'host_name': self.host_name,
            'user_name': self.user_name,
            'user_password': self.user_password,
            'data_base_name': self.data_base_name,
            'data_keys': self.data_recv_keys
        }
        
        # Usar Manager para objetos compartidos entre procesos
        self.manager = Manager()
        self.is_running = self.manager.Value('i', 0)  # 0 = False, 1 = True
        self.is_connected = self.manager.Value('i', 0)
        self.last_plc_response = self.manager.Value('d', 0.0)
        
        # Eventos para sincronización
        self.stop_event = Event()
        self.connection_event = Event()
        
        # Procesos
        self.heartbeat_process = None
        self.station_processes = []
        
        # Configuración serializable de estaciones, cada una con su propio proceso
        """
        Aquí se parametrizan las estaciones de trabajo que se quieren monitorear.
        Cada estación tiene una configuración específica que incluye:
        - name: Nombre identificador de la estación.
        - params: Lista de parámetros específicos para la estación.
        - store, update, search, delete: Flags booleanos para operaciones SQL.
        """
        self.station_configs = [
            {'name': 'station_05',          'params' : [4, 2 , 0, 0, 3, 0  , 60, 4, 44, 0], 'store': True , 'update': False, 'search': False, 'delete': False},
            {'name': 'station_10',          'params' : [4, 4 , 1, 1, 3, 60 , 60, 4, 46, 1], 'store': False, 'update': True , 'search': False, 'delete': False},
            {'name': 'station_20_R01',      'params' : [4, 6 , 1, 2, 3, 120, 60, 4, 48, 1], 'store': False, 'update': True , 'search': False, 'delete': False},
            {'name': 'station_20_R02',      'params' : [4, 8 , 1, 3, 3, 180, 60, 4, 50, 1], 'store': False, 'update': True , 'search': False, 'delete': False},
            {'name': 'station_20QC_search', 'params' : [4, 10, 2, 4, 3, 240, 60, 4, 52, 2], 'store': False, 'update': False, 'search': True , 'delete': False},
            {'name': 'station_20QC_update', 'params' : [4, 10, 1, 7, 3, 240, 60, 4, 52, 1], 'store': False, 'update': True , 'search': False , 'delete': False},
            {'name': 'station_30_R01',      'params' : [4, 12, 1, 5, 3, 300, 60, 4, 54, 1], 'store': False, 'update': True , 'search': False, 'delete': False},
            {'name': 'station_60_R01',      'params' : [4, 14, 3, 6, 3, 720, 60, 4, 56, 3], 'store': False, 'update': False, 'search': False, 'delete': True }
        ]
        
        print("✅ Instancia PLC creada (configuración serializable)")
    
    def start(self):
        """Iniciar el sistema con múltiples procesos"""
        if self.is_running.value:
            print("⚠️  El sistema ya está ejecutándose")
            return False
        
        print("🚀 Iniciando sistema con multiprocessing...")
        self.is_running.value = 1
        self.stop_event.clear()
        
        # 1. Iniciar proceso de heartbeat
        self.heartbeat_process = Process(
            target=heartbeat_worker,
            args=(self.config_data, self.is_running, self.is_connected,
                  self.last_plc_response, self.stop_event, self.connection_event),
            name="Heartbeat-Process"
        )
        self.heartbeat_process.start()
        
        print("✅ Proceso de heartbeat iniciado")
        
        # Esperar un momento para conexión inicial
        time.sleep(3)
        
        # 2. Iniciar procesos de estaciones
        for station_config in self.station_configs:
            process = Process(
                target=station_worker,
                args=(station_config, self.config_data, self.is_running, self.is_connected,
                      self.last_plc_response, self.stop_event, self.connection_event),
                name=f"Station-{station_config['name']}"
            )
            process.start()
            self.station_processes.append(process)
            print(f"✅ Proceso {station_config['name']} iniciado")
            time.sleep(0.5)  # Espaciar inicio
        
        print(f"✅ Sistema iniciado con {len(self.station_processes) + 1} procesos")
        return True
    
    def stop(self):
        """Detener el sistema y todos los procesos"""
        print("🛑 Deteniendo sistema...")
        
        # Señalizar parada
        self.is_running.value = 0
        self.stop_event.set()
        
        # Esperar que los procesos terminen
        print("⏳ Esperando que procesos terminen...")
        
        # Esperar procesos de estaciones
        for i, process in enumerate(self.station_processes):
            if process.is_alive():
                print(f"🛑 Terminando proceso {i+1}...")
                process.join(timeout=3)
                if process.is_alive():
                    process.terminate()
        
        # Esperar proceso de heartbeat
        if self.heartbeat_process and self.heartbeat_process.is_alive():
            print("🛑 Terminando proceso de heartbeat...")
            self.heartbeat_process.join(timeout=3)
            if self.heartbeat_process.is_alive():
                self.heartbeat_process.terminate()
        
        # Limpiar listas
        self.station_processes.clear()
        
        print("✅ Sistema detenido completamente")

# ========== FUNCIONES AUXILIARES ESTÁTICAS ==========

def create_empty_data_dict(keys):
    """Crear diccionario vacío con todas las claves"""
    return {key: None for key in keys}

def p_trig_static(signal_value, mem_index: int, mem_value_list):
    """Función de trigger para pruebas (versión estática)"""
    if not mem_value_list[mem_index] and signal_value:
        print("🔔 Trigger activado")
        mem_value_list[mem_index] = True
        return True
    if mem_value_list[mem_index] and not signal_value:
        print("🔔 Trigger desactivado")
        mem_value_list[mem_index] = False
        return False

def recv_data_operationbits(raw_data: bytearray, decode_data: dict):
    """Extraer bits de operación del raw data"""
    mask = [0x01, 0x02, 0x04, 0x08, 0x10, 0x20, 0x40, 0x80]
    
    op_offset = {
        'OP5_': 0,
        'OP10_': 1,
        'OP20_': 2,
        'OP30_': 4,
        'OP40_': 6,
        'OP50_': 8,
        'OP60_': 10,
        'OP70_': 12,
        'OP80_': 14,
        'OP90_': 16,
        'OP100_': 18
    }
    
    contador = 0
    
    for op in op_offset:
        # Filtrar claves que comienzan con el prefijo de operación
        for columna in decode_data:
            if columna.startswith(op):
                if contador <= 7:
                    decode_data[columna] = 1 if raw_data[op_offset[op]] & mask[contador] else 0
                    contador += 1
        contador = 0
    
    return decode_data

def recv_data_NoMachineProd(raw_data: bytearray, decode_data: dict):
    """Extraer contadores de máquinas no producidas"""
    op_offset = {
        'OP5_': 0,
        'OP10_': 1,
        'OP20_': 3,
        'OP30_': 5,
        'OP40_': 7,
        'OP50_': 9,
        'OP60_': 11,
        'OP70_': 13,
        'OP80_': 15,
        'OP90_': 17,
        'OP100_': 19
    }
    
    number_machine_str = 'No_MachineProduced'
    
    for op in op_offset:
        for columna in decode_data:
            if columna.startswith(op + number_machine_str):
                decode_data[columna] = raw_data[op_offset[op]]
    
    return decode_data

def recv_data_part(raw_data: bytearray, decode_data: dict):
    """Extraer datos de pieza del raw data"""
    try:
        raw_data_str = raw_data.decode('utf-8', errors='ignore')
    except:
        raw_data_str = str(raw_data)
    
    part_Number_NA = ''
    part_Number_EE = ''
    serial_Number = ''
    supplier_ID = ''
    
    data_offset = {
        'job_Number': 20,
        'part_Number_NA': 22,
        'part_Number_EE': 32,
        'serial_Number': 43,
        'supplier_ID': 53,
        'fin': 60
    }
    
    # Extraer números de parte
    try:
        for pNNA in range(data_offset['part_Number_NA'], min(data_offset['part_Number_EE'], len(raw_data_str))):
            part_Number_NA += raw_data_str[pNNA]
    except:
        pass
    
    try:
        for pNEE in range(data_offset['part_Number_EE'], min(data_offset['serial_Number'], len(raw_data_str))):
            part_Number_EE += raw_data_str[pNEE]
    except:
        pass
    
    try:
        for sn in range(data_offset['serial_Number'], min(data_offset['supplier_ID'], len(raw_data_str))):
            serial_Number += raw_data_str[sn]
    except:
        pass
    
    try:
        for sID in range(data_offset['supplier_ID'], min(data_offset['fin'], len(raw_data_str))):
            supplier_ID += raw_data_str[sID]
    except:
        pass
    
    # Asignar valores al diccionario
    try:
        decode_data['JobNumber'] = snap7.util.get_int(raw_data, data_offset['job_Number'])
    except:
        decode_data['JobNumber'] = 0
    
    decode_data['PartNumberNA'] = part_Number_NA.strip('\x00')
    decode_data['PartNumberEESA'] = part_Number_EE.strip('\x00')
    decode_data['SerialNumber'] = serial_Number.strip('\x00')
    decode_data['SupplierID'] = supplier_ID.strip('\x00')
    
    return decode_data

def send_data_to_PLC(client, encode_data: dict, db_number: int, start_byte: int):
    """Enviar datos al PLC"""
    outgoing_data = bytearray(60)
    
    # OP20-OP100 Machine Produced Count
    snap7.util.set_byte(outgoing_data, 3, encode_data.get('OP20_No_MachineProduced', 0))
    snap7.util.set_byte(outgoing_data, 5, encode_data.get('OP30_No_MachineProduced', 0))
    snap7.util.set_byte(outgoing_data, 7, encode_data.get('OP40_No_MachineProduced', 0))
    snap7.util.set_byte(outgoing_data, 9, encode_data.get('OP50_No_MachineProduced', 0))
    snap7.util.set_byte(outgoing_data, 11, encode_data.get('OP60_No_MachineProduced', 0))
    snap7.util.set_byte(outgoing_data, 13, encode_data.get('OP70_No_MachineProduced', 0))
    snap7.util.set_byte(outgoing_data, 15, encode_data.get('OP80_No_MachineProduced', 0))
    snap7.util.set_byte(outgoing_data, 17, encode_data.get('OP90_No_MachineProduced', 0))
    snap7.util.set_byte(outgoing_data, 19, encode_data.get('OP100_No_MachineProduced', 0))
    
    # OP5 Operation Bits
    snap7.util.set_byte(outgoing_data, 0,
        (encode_data.get('OP5_Part_Present', 0) << 0) |
        (encode_data.get('OP5_Laser_Mark_Ok', 0) << 1) |
        (encode_data.get('OP5_Laser_Mark_Nok', 0) << 2) |
        (encode_data.get('OP5_Code_Verify_Ok', 0) << 3) |
        (encode_data.get('OP5_Code_Verify_Nok', 0) << 4) |
        (encode_data.get('OP5_Code_Read', 0) << 5) |
        (encode_data.get('OP5_Code_No_Read', 0) << 6) |
        (encode_data.get('OP5_Reserved1', 0) << 7)
    )
    
    # OP10 Operation Bits
    snap7.util.set_byte(outgoing_data, 1,
        (encode_data.get('OP10_Code_Verify_Ok', 0) << 0) |
        (encode_data.get('OP10_Code_Verify_Nok', 0) << 1) |
        (encode_data.get('OP10_Code_Read', 0) << 2) |
        (encode_data.get('OP10_Code_No_Read', 0) << 3) |
        (encode_data.get('OP10_Reserved1', 0) << 4) |
        (encode_data.get('OP10_Reserved2', 0) << 5) |
        (encode_data.get('OP10_Reserved3', 0) << 6) |
        (encode_data.get('OP10_Reserved4', 0) << 7)
    )
    
    # OP20 Operation Bits
    snap7.util.set_byte(outgoing_data, 2,
        (encode_data.get('OP20_PartMachined_Ok', 0) << 0) |
        (encode_data.get('OP20_PartMachined_Nok', 0) << 1) |
        (encode_data.get('OP20_FirstPart', 0) << 2) |
        (encode_data.get('OP20_PartQC', 0) << 3) |
        (encode_data.get('OP20_Reserved1', 0) << 4) |
        (encode_data.get('OP20_Reserved2', 0) << 5) |
        (encode_data.get('OP20_Reserved3', 0) << 6) |
        (encode_data.get('OP20_Reserved4', 0) << 7)
    )
    
    # OP30 Operation Bits
    snap7.util.set_byte(outgoing_data, 4,
        (encode_data.get('OP30_PartMachined_Ok', 0) << 0) |
        (encode_data.get('OP30_PartMachined_Nok', 0) << 1) |
        (encode_data.get('OP30_FirstPart', 0) << 2) |
        (encode_data.get('OP30_PartQC', 0) << 3) |
        (encode_data.get('OP30_Reserved1', 0) << 4) |
        (encode_data.get('OP30_Reserved2', 0) << 5) |
        (encode_data.get('OP30_Reserved3', 0) << 6) |
        (encode_data.get('OP30_Reserved4', 0) << 7)
    )
    
    # OP40 Operation Bits
    snap7.util.set_byte(outgoing_data, 6,
        (encode_data.get('OP40_PartMachined_Ok', 0) << 0) |
        (encode_data.get('OP40_PartMachined_Nok', 0) << 1) |
        (encode_data.get('OP40_FirstPart', 0) << 2) |
        (encode_data.get('OP40_PartQC', 0) << 3) |
        (encode_data.get('OP40_Reserved1', 0) << 4) |
        (encode_data.get('OP40_Reserved2', 0) << 5) |
        (encode_data.get('OP40_Reserved3', 0) << 6) |
        (encode_data.get('OP40_Reserved4', 0) << 7)
    )
    
    # OP50 Operation Bits
    snap7.util.set_byte(outgoing_data, 8,
        (encode_data.get('OP50_PartMachined_Ok', 0) << 0) |
        (encode_data.get('OP50_PartMachined_Nok', 0) << 1) |
        (encode_data.get('OP50_FirstPart', 0) << 2) |
        (encode_data.get('OP50_PartQC', 0) << 3) |
        (encode_data.get('OP50_Reserved1', 0) << 4) |
        (encode_data.get('OP50_Reserved2', 0) << 5) |
        (encode_data.get('OP50_Reserved3', 0) << 6) |
        (encode_data.get('OP50_Reserved4', 0) << 7)
    )
    
    # OP60 Operation Bits
    snap7.util.set_byte(outgoing_data, 10,
        (encode_data.get('OP60_PartMachined_Ok', 0) << 0) |
        (encode_data.get('OP60_PartMachined_Nok', 0) << 1) |
        (encode_data.get('OP60_FirstPart', 0) << 2) |
        (encode_data.get('OP60_PartQC', 0) << 3) |
        (encode_data.get('OP60_Reserved1', 0) << 4) |
        (encode_data.get('OP60_Reserved2', 0) << 5) |
        (encode_data.get('OP60_Reserved3', 0) << 6) |
        (encode_data.get('OP60_Reserved4', 0) << 7)
    )
    
    # OP70 Operation Bits
    snap7.util.set_byte(outgoing_data, 12,
        (encode_data.get('OP70_PartMachined_Ok', 0) << 0) |
        (encode_data.get('OP70_PartMachined_Nok', 0) << 1) |
        (encode_data.get('OP70_FirstPart', 0) << 2) |
        (encode_data.get('OP70_PartQC', 0) << 3) |
        (encode_data.get('OP70_Reserved1', 0) << 4) |
        (encode_data.get('OP70_Reserved2', 0) << 5) |
        (encode_data.get('OP70_Reserved3', 0) << 6) |
        (encode_data.get('OP70_Reserved4', 0) << 7)
    )
    
    # OP80 Operation Bits
    snap7.util.set_byte(outgoing_data, 14,
        (encode_data.get('OP80_PartMachined_Ok', 0) << 0) |
        (encode_data.get('OP80_PartMachined_Nok', 0) << 1) |
        (encode_data.get('OP80_FirstPart', 0) << 2) |
        (encode_data.get('OP80_PartQC', 0) << 3) |
        (encode_data.get('OP80_Reserved1', 0) << 4) |
        (encode_data.get('OP80_Reserved2', 0) << 5) |
        (encode_data.get('OP80_Reserved3', 0) << 6) |
        (encode_data.get('OP80_Reserved4', 0) << 7)
    )
    
    # OP90 Operation Bits
    snap7.util.set_byte(outgoing_data, 16,
        (encode_data.get('OP90_PartMachined_Ok', 0) << 0) |
        (encode_data.get('OP90_PartMachined_Nok', 0) << 1) |
        (encode_data.get('OP90_FirstPart', 0) << 2) |
        (encode_data.get('OP90_PartQC', 0) << 3) |
        (encode_data.get('OP90_Reserved1', 0) << 4) |
        (encode_data.get('OP90_Reserved2', 0) << 5) |
        (encode_data.get('OP90_Reserved3', 0) << 6) |
        (encode_data.get('OP90_Reserved4', 0) << 7)
    )
    
    # OP100 Operation Bits
    snap7.util.set_byte(outgoing_data, 18,
        (encode_data.get('OP100_PartMachined_Ok', 0) << 0) |
        (encode_data.get('OP100_PartMachined_Nok', 0) << 1) |
        (encode_data.get('OP100_FirstPart', 0) << 2) |
        (encode_data.get('OP100_PartQC', 0) << 3) |
        (encode_data.get('OP100_Reserved1', 0) << 4) |
        (encode_data.get('OP100_Reserved2', 0) << 5) |
        (encode_data.get('OP100_Reserved3', 0) << 6) |
        (encode_data.get('OP100_Reserved4', 0) << 7)
    )
    
    # Job Number
    snap7.util.set_int(outgoing_data, 20, encode_data.get('JobNumber', 0))
    
    # Part Number NA
    part_number_na = encode_data.get('PartNumberNA', '')
    part_number_na_bytes = part_number_na.encode('utf-8')[:10]
    outgoing_data[22:22+len(part_number_na_bytes)] = part_number_na_bytes
    
    # Part Number EESA
    part_number_ee = encode_data.get('PartNumberEESA', '')
    part_number_ee_bytes = part_number_ee.encode('utf-8')[:11]
    outgoing_data[32:32+len(part_number_ee_bytes)] = part_number_ee_bytes
    
    # Serial Number
    serial_number = encode_data.get('SerialNumber', '')
    serial_number_bytes = serial_number.encode('utf-8')[:10]
    outgoing_data[43:43+len(serial_number_bytes)] = serial_number_bytes
    
    # Supplier ID
    supplier_id = encode_data.get('SupplierID', '')
    supplier_id_bytes = supplier_id.encode('utf-8')[:7]
    outgoing_data[53:53+len(supplier_id_bytes)] = supplier_id_bytes
    
    # Escribir al PLC
    client.db_write(db_number, start_byte, outgoing_data)
    
    return outgoing_data

def not_found_dict(decode_data: dict):
    #Setear operation bits a 0 en caso de no encontrado
    util_keys = [key for key in decode_data.keys() if key.startswith('OP')]
    for key in util_keys:
        decode_data[key] = 0

    """Crear diccionario para dato no encontrado"""
    decode_data['JobNumber'] = 0
    decode_data['PartNumberNA'] = "NOT FOUND"
    decode_data['PartNumberEESA'] = "NOT FOUND"
    decode_data['SerialNumber'] = "NOT FOUND"
    decode_data['SupplierID'] = "NOFOUND"
    return decode_data

# ========== FUNCIONES WORKER PARA PROCESOS ==========

def heartbeat_worker(config, is_running, is_connected, last_plc_response, stop_event, connection_event):
    """Función worker para el proceso de heartbeat"""
    print("🚀 Proceso de heartbeat iniciado")
    
    # Deserializar configuración
    ip = config['ip']
    rack = config['rack']
    slot = config['slot']
    hb_db = config['hb_db']
    hb_byte = config['hb_byte']
    hb_bit = config['hb_bit']
    hb_byte_python = config['hb_byte_python']
    hb_bit_python = config['hb_bit_python']
    
    # Crear cliente local
    client = None
    hb_state = False
    last_hb_toggle = datetime.now()
    hb_interval = 1.0
    connection_attempts = 0
    max_connection_attempts = 3
    
    def create_new_client():
        """Crear un nuevo cliente snap7 limpio"""
        nonlocal client
        if client is not None:
            try:
                # Intentar desconectar limpiamente
                client.disconnect()
            except:
                pass
        # Forzar garbage collection del cliente viejo
        client = None
        time.sleep(0.5)
        return snap7.client.Client()
    
    def safe_disconnect():
        """Desconectar de forma segura"""
        nonlocal client
        if client is not None:
            try:
                client.disconnect()
            except:
                pass
            client = None
    
    def write_python_heartbeat(state):
        try:
            if client is None:
                return False
            data = client.db_read(hb_db, hb_byte_python, 1)
            snap7.util.set_bool(data, 0, hb_bit_python, state)
            client.db_write(hb_db, hb_byte_python, data)
            return True
        except Exception as e:
            print(f"❌ Error escribiendo heartbeat: {e}")
            return False
    
    def read_plc_heartbeat():
        try:
            if client is None:
                return False
            data = client.db_read(hb_db, hb_byte, 1)
            return snap7.util.get_bool(data, 0, hb_bit)
        except Exception as e:
            print(f"❌ Error leyendo heartbeat PLC: {e}")
            return False
    
    def is_socket_error(exception_str: str) -> bool:
        """Detectar si el error es de socket/red"""
        socket_errors = [
            'ISO : An error occurred during recv TCP : Connection timed out',
            'Connection timed out',
            'Connection reset by peer',
            'Network is unreachable',
            'No route to host',
            'Socket operation on non-socket',
            'Broken pipe',
            'Connection refused'
        ]
        return any(error in str(exception_str) for error in socket_errors)
    
    def test_connection() -> bool:
        """Probar si la conexión es funcional"""
        try:
            if client is None:
                return False
            # Intentar una operación simple
            data = client.db_read(hb_db, hb_byte, 1)
            return True
        except Exception as e:
            if is_socket_error(str(e)):
                print(f"🔌 Error de socket detectado: {e}")
                safe_disconnect()
            return False
    
    while not stop_event.is_set():
        try:
            # Intentar conexión si no está conectado
            if not is_connected.value:
                print(f"🔌 Intentando conectar a {ip}...")
                try:
                    client = create_new_client()
                    client.connect(ip, rack, slot)
                    
                    # Probar la conexión
                    if test_connection():
                        write_python_heartbeat(True)
                        time.sleep(0.1)
                        
                        # Verificar conexión
                        if read_plc_heartbeat():
                            is_connected.value = 1
                            last_plc_response.value = datetime.now().timestamp()
                            connection_event.set()
                            connection_attempts = 0
                            print("✅ Conexión PLC establecida")
                        else:
                            print("⚠️  PLC no responde al heartbeat")
                            safe_disconnect()
                            continue
                    else:
                        print("⚠️  Conexión establecida pero no funcional")
                        safe_disconnect()
                        time.sleep(2)
                        continue
                        
                except Exception as e:
                    print(f"❌ Error de conexión: {e}")
                    safe_disconnect()
                    connection_attempts += 1
                    
                    # Si hay muchos intentos fallidos, esperar más
                    if connection_attempts >= max_connection_attempts:
                        print(f"🔄 Muchos intentos fallidos, esperando 5 segundos...")
                        time.sleep(5)
                        connection_attempts = 0
                    else:
                        time.sleep(2)
                    continue
            
            # Si estamos conectados, mantener heartbeat
            if is_connected.value:
                current_time = datetime.now()
                
                # Toggle heartbeat
                if (current_time - last_hb_toggle) >= timedelta(seconds=hb_interval):
                    hb_state = not hb_state
                    if not write_python_heartbeat(hb_state):
                        print("⚠️  Error en heartbeat, reconectando...")
                        is_connected.value = 0
                        connection_event.clear()
                        safe_disconnect()
                        continue
                    last_hb_toggle = current_time
                
                # Verificar respuesta del PLC
                try:
                    plc_response = read_plc_heartbeat()
                    if plc_response:
                        last_plc_response.value = current_time.timestamp()
                    
                    # Verificar timeout
                    if last_plc_response.value > 0:
                        last_resp_time = datetime.fromtimestamp(last_plc_response.value)
                        if (current_time - last_resp_time) > timedelta(seconds=5):
                            print("⏰ Timeout - PLC no responde")
                            is_connected.value = 0
                            connection_event.clear()
                            safe_disconnect()
                            continue
                    
                    # Probar conexión periódicamente
                    if current_time.timestamp() % 10 < 0.5:  # Cada ~10 segundos
                        if not test_connection():
                            print("🔄 Prueba de conexión fallida, reconectando...")
                            is_connected.value = 0
                            connection_event.clear()
                            safe_disconnect()
                            
                except Exception as e:
                    print(f"❌ Error verificando PLC: {e}")
                    if is_socket_error(str(e)):
                        print("🔌 Error de socket, reconectando...")
                        is_connected.value = 0
                        connection_event.clear()
                        safe_disconnect()
                    else:
                        print(f"⚠️  Otro error: {e}")
            
            time.sleep(0.5)
            
        except KeyboardInterrupt:
            break
        except Exception as e:
            print(f"❌ Error inesperado en heartbeat: {e}")
            safe_disconnect()
            is_connected.value = 0
            connection_event.clear()
            time.sleep(1)
    
    # Limpieza
    try:
        write_python_heartbeat(False)
        time.sleep(0.1)
    except:
        pass
    safe_disconnect()
    print("🛑 Proceso de heartbeat terminado")



def station_worker(station_config, plc_config, is_running, is_connected, 
                   last_plc_response, stop_event, connection_event):
    """Función worker completa para procesos de estación"""
    station_name = station_config['name']
    print(f"🚀 Proceso de estación {station_name} iniciado")
    
    # Deserializar configuraciones
    station_params = station_config['params']
    store = station_config['store']
    update = station_config['update']
    search = station_config['search']
    delete = station_config['delete']
    
    ip = plc_config['ip']
    rack = plc_config['rack']
    slot = plc_config['slot']
    hb_db = plc_config['hb_db']
    host_name = plc_config['host_name']
    user_name = plc_config['user_name']
    user_password = plc_config['user_password']
    data_base_name = plc_config['data_base_name']
    data_keys = plc_config['data_keys']
    
    # Parámetros de la estación
    db_number_incoming = station_params[0]
    incoming_byte_offset = station_params[1]
    incoming_bit_offset = station_params[2]
    incoming_mem_index = station_params[3]
    db_read = station_params[4]
    read_start = station_params[5]
    read_size = station_params[6]
    ack_incoming_db_offset = station_params[7]
    ack_incoming_byte_offset = station_params[8]
    ack_incoming_bit_offset = station_params[9]
    
    # Variables locales
    client: Optional[snap7.client.Client] = None
    sql_con = None
    mem_value = [False] * 8
    
    # Crear diccionario local para datos
    data_recv = create_empty_data_dict(data_keys)
    
    def create_new_client():
        """Crear un nuevo cliente snap7 limpio"""
        nonlocal client
        if client is not None:
            try:
                client.disconnect()
            except:
                pass
        client = None
        time.sleep(0.5)
        return snap7.client.Client()
    
    def safe_disconnect():
        """Desconectar de forma segura"""
        nonlocal client
        if client is not None:
            try:
                client.disconnect()
            except:
                pass
            client = None
    
    def is_socket_error(exception_str: str) -> bool:
        """Detectar si el error es de socket/red"""
        socket_errors = [
            'ISO : An error occurred during recv TCP : Connection timed out',
            'Connection timed out',
            'Connection reset by peer',
            'Network is unreachable',
            'No route to host',
            'Socket operation on non-socket',
            'Broken pipe'
        ]
        return any(error in str(exception_str) for error in socket_errors)
    
    def read_bit_safe(db, byte, bit):
        """Leer bit de forma segura, verificando que client no sea None"""
        if client is None:
            return None
        try:
            data = client.db_read(db, byte, 1)
            return snap7.util.get_bool(data, 0, bit)
        except Exception as e:
            if is_socket_error(str(e)):
                print(f"🔌 {station_name}: Error de socket, reconectando...")
                safe_disconnect()
            else:
                print(f"❌ {station_name}: Error leyendo bit: {e}")
            return None
    
    def write_bit_safe(db, byte, bit, value):
        """Escribir bit de forma segura, verificando que client no sea None"""
        if client is None:
            return False
        try:
            data = client.db_read(db, byte, 1)
            snap7.util.set_bool(data, 0, bit, value)
            client.db_write(db, byte, data)
            return True
        except Exception as e:
            if is_socket_error(str(e)):
                print(f"🔌 {station_name}: Error de socket, reconectando...")
                safe_disconnect()
            else:
                print(f"❌ {station_name}: Error escribiendo bit: {e}")
            return False
    
    def read_data_safe(db, start, size):
        """Leer datos de forma segura, verificando que client no sea None"""
        if client is None:
            return None
        try:
            return client.db_read(db, start, size)
        except Exception as e:
            if is_socket_error(str(e)):
                print(f"🔌 {station_name}: Error de socket, reconectando...")
                safe_disconnect()
            else:
                print(f"❌ {station_name}: Error leyendo datos: {e}")
            return None
    
    def test_connection() -> bool:
        """Probar si la conexión es funcional"""
        try:
            if client is None:
                return False
            # Intentar una operación simple
            data = client.db_read(hb_db, 0, 1)
            return True
        except Exception as e:
            if is_socket_error(str(e)):
                print(f"🔌 {station_name}: Error de socket en test de conexión")
                safe_disconnect()
            return False
    
    while not stop_event.is_set():
        try:
            # Esperar conexión activa
            if not is_connected.value:
                connection_event.wait(timeout=1.0)
                continue
            
            # Crear conexión al PLC si no existe
            if client is None:
                try:
                    client = create_new_client()
                    client.connect(ip, rack, slot)
                    
                    # Probar la conexión
                    if test_connection():
                        print(f"✅ {station_name} conectada al PLC")
                    else:
                        print(f"⚠️  {station_name}: Conexión establecida pero no funcional")
                        safe_disconnect()
                        time.sleep(2)
                        continue
                        
                except Exception as e:
                    print(f"❌ {station_name}: Error conectando al PLC: {e}")
                    safe_disconnect()
                    time.sleep(2)
                    continue
            
            # Crear conexión a SQL si es necesario
            if sql_con is None:
                try:
                    sql_con = sqlreg.create_server_connection(host_name, user_name, user_password)
                    sqlreg.create_database(sql_con, f"CREATE DATABASE IF NOT EXISTS {data_base_name}", data_base_name)
                    print(f"✅ {station_name} conectada a SQL")
                except Exception as e:
                    print(f"⚠️  {station_name}: Error SQL: {e}")
                    time.sleep(2)
                    continue
            
            # Probar conexión periódicamente
            current_time = datetime.now()
            if current_time.timestamp() % 15 < 0.5:  # Cada ~15 segundos
                if not test_connection():
                    print(f"🔄 {station_name}: Prueba de conexión fallida, esperando reconexión...")
                    safe_disconnect()
                    continue
            
            # ========== LÓGICA DE ALMACENAMIENTO (STORE) ==========
            if store:
                # Detectar flanco de dato para almacenar
                signal = read_bit_safe(db_number_incoming, incoming_byte_offset, incoming_bit_offset)
                if signal is not None and p_trig_static(signal, incoming_mem_index, mem_value):
                    print(f"🗣️ {station_name}: Dato recibido desde el PLC para almacenar")
                    
                    # Leer dato de PLC
                    store_data = read_data_safe(db_read, read_start, read_size)
                    if store_data is None:
                        print(f"❌ {station_name}: No se pudo leer datos del PLC")
                        continue
                    
                    # Extraer datos recibidos del PLC
                    data_recv = create_empty_data_dict(data_keys)
                    recv_data_operationbits(store_data, data_recv)
                    recv_data_NoMachineProd(store_data, data_recv)
                    recv_data_part(store_data, data_recv)
                    
                    print(f"📥 {station_name}: Dato para registrar en MySQL: {data_recv.get('SerialNumber', 'N/A')}")
                    
                    # Enviar dato a MySQL si hay conexión
                    if sql_con:
                        try:
                            # Buscar dato en MySQL
                            dato_buscado = sqlreg.search_data(sql_con, 'front_cover', 'SerialNumber', 
                                                            data_recv.get('SerialNumber', ''))
                            
                            if dato_buscado is None or dato_buscado == 3:
                                print(f"🔍 {station_name}: Dato no registrado, insertando...")
                                sqlreg.recv_Data_To_MySQL(sql_con, 'front_cover', data_recv)
                                print(f"✅ {station_name}: Dato insertado en MySQL")
                                
                                # Verificar inserción
                                dato_verificado = sqlreg.search_data(sql_con, 'front_cover', 'SerialNumber', 
                                                                    data_recv.get('SerialNumber', ''))
                                if dato_verificado in [1, 2]:
                                    print(f"✅ {station_name}: Inserción verificada")
                                else:
                                    print(f"❌ {station_name}: Error de verificación")
                            else:
                                print(f"⚠️  {station_name}: Dato ya existente, no se inserta duplicado")
                                
                        except Exception as e:
                            print(f"❌ {station_name}: Error SQL: {e}")
                    
                    # Acknowledge al PLC
                    write_bit_safe(ack_incoming_db_offset, ack_incoming_byte_offset, 
                                 ack_incoming_bit_offset, True)
                    time.sleep(0.1)
                    write_bit_safe(ack_incoming_db_offset, ack_incoming_byte_offset,
                                 ack_incoming_bit_offset, False)
                    
                    # Limpiar datos para próxima iteración
                    data_recv = create_empty_data_dict(data_keys)
            
            # ========== LÓGICA DE ACTUALIZACIÓN (UPDATE) ==========
            elif update:
                # Detectar flanco de dato para actualizar
                signal = read_bit_safe(db_number_incoming, incoming_byte_offset, incoming_bit_offset)
                if signal is not None and p_trig_static(signal, incoming_mem_index, mem_value):
                    print(f"🔄 {station_name}: Dato recibido para actualizar")
                    
                    # Leer datos del PLC
                    update_data = read_data_safe(db_read, read_start, read_size)
                    if update_data is None:
                        print(f"❌ {station_name}: No se pudo leer datos del PLC")
                        continue
                    
                    # Extraer datos
                    data_recv = create_empty_data_dict(data_keys)
                    recv_data_operationbits(update_data, data_recv)
                    recv_data_NoMachineProd(update_data, data_recv)
                    recv_data_part(update_data, data_recv)
                    
                    print(f"📥 {station_name}: Dato para actualizar en MySQL: {data_recv.get('SerialNumber', 'N/A')}")
                    
                    # Actualizar dato en MySQL
                    if sql_con:
                        dato_buscado = sqlreg.search_data(sql_con, 'front_cover', 'SerialNumber', data_recv.get('SerialNumber', ''))
                        if dato_buscado is None or dato_buscado == 3:
                            print(f"⚠️  {station_name}: Dato no encontrado en MySQL, no se puede actualizar")
                        if dato_buscado == 1 or dato_buscado == 2:
                            try:
                                print(f"🔄 {station_name}: Actualizando dato en MySQL...")
                                sqlreg.update_part_data(sql_con, 'front_cover', data_recv)
                                print(f"✅ {station_name}: Dato actualizado en MySQL")
                            except Exception as e:
                                print(f"❌ {station_name}: Error actualizando SQL: {e}")
                    
                    # Acknowledge al PLC
                    write_bit_safe(ack_incoming_db_offset, ack_incoming_byte_offset,
                                 ack_incoming_bit_offset, True)
                    time.sleep(0.1)
                    write_bit_safe(ack_incoming_db_offset, ack_incoming_byte_offset,
                                 ack_incoming_bit_offset, False)
                    
                    # Limpiar datos
                    data_recv = create_empty_data_dict(data_keys)
            
            # ========== LÓGICA DE BÚSQUEDA (SEARCH) ==========
            elif search:
                # Detectar flanco de dato para búsqueda
                signal = read_bit_safe(db_number_incoming, incoming_byte_offset, incoming_bit_offset)
                if signal is not None and p_trig_static(signal, incoming_mem_index, mem_value):
                    print(f"🔍 {station_name}: Buscando dato en MySQL")
                    
                    # Leer datos del PLC
                    search_data = read_data_safe(db_read, read_start, read_size)
                    if search_data is None:
                        print(f"❌ {station_name}: No se pudo leer datos del PLC")
                        continue
                    
                    # Extraer datos
                    data_recv = create_empty_data_dict(data_keys)
                    recv_data_operationbits(search_data, data_recv)
                    recv_data_NoMachineProd(search_data, data_recv)
                    recv_data_part(search_data, data_recv)
                    
                    serial_number = data_recv.get('SerialNumber', '')
                    print(f"📥 {station_name}: Buscando SerialNumber: {serial_number}")
                    
                    # Buscar dato en MySQL
                    if sql_con:
                        try:
                            search_piece = sqlreg.search_data(sql_con, 'front_cover', 'SerialNumber', serial_number)
                            
                            if search_piece == 1 or search_piece == 2:
                                print(f"✅ {station_name}: Dato encontrado en MySQL, obteniendo...")
                                sqlreg.get_Data_From_MySQL(sql_con, 'front_cover', data_recv, serial_number)
                                print(f"✅ {station_name}: Dato obtenido de MySQL")
                                
                                # Enviar dato al PLC
                                print(f"🔄 {station_name}: Enviando dato al PLC...")
                                if client is not None:  # Verificar nuevamente
                                    send_data = send_data_to_PLC(client, data_recv, db_read, read_start)
                                    print(f"✅ {station_name}: Dato enviado al PLC")
                                    
                                    # Verificar que el dato fue cargado correctamente
                                    verify_data = read_data_safe(db_read, read_start, read_size)
                                    if verify_data is not None and verify_data == send_data:
                                        print(f"✅ {station_name}: Dato verificado en PLC")
                                        # Opcional: borrar fila en MySQL después de cargar al PLC
                                        # sqlreg.delete_data(sql_con, 'front_cover', 'SerialNumber', serial_number)
                                    else:
                                        print(f"❌ {station_name}: Error de verificación en PLC")
                            
                            if search_piece is None or search_piece == 3:
                                # Dato no encontrado
                                print(f"⚠️  {station_name}: Dato no encontrado en MySQL")
                                data_recv = not_found_dict(data_recv)
                                print(f"🔄 {station_name}: Enviando notificación al PLC...")
                                if client is not None:  # Verificar nuevamente
                                    send_data = send_data_to_PLC(client, data_recv, db_read, read_start)
                                    print(f"✅ {station_name}: Notificación enviada al PLC")
                                
                        except Exception as e:
                            print(f"❌ {station_name}: Error en búsqueda SQL: {e}")
                            # Enviar notificación de error
                            if client is not None:
                                data_recv = not_found_dict(data_recv)
                                send_data_to_PLC(client, data_recv, db_read, read_start)
                    
                    # Acknowledge al PLC
                    write_bit_safe(ack_incoming_db_offset, ack_incoming_byte_offset,
                                 ack_incoming_bit_offset, True)
                    time.sleep(0.1)
                    write_bit_safe(ack_incoming_db_offset, ack_incoming_byte_offset,
                                 ack_incoming_bit_offset, False)
                    
                    # Limpiar datos
                    data_recv = create_empty_data_dict(data_keys)

            # ========== LÓGICA DE ELIMINACION (DELETE) ==========
            elif delete:
                # Detectar flanco de dato para búsqueda
                signal = read_bit_safe(db_number_incoming, incoming_byte_offset, incoming_bit_offset)
                if signal is not None and p_trig_static(signal, incoming_mem_index, mem_value):
                    print(f"🗑️ {station_name}: Dato recibido para eliminar")
                    
                    # Leer datos del PLC
                    delete_data = read_data_safe(db_read, read_start, read_size)
                    if delete_data is None:
                        print(f"❌ {station_name}: No se pudo leer datos del PLC")
                        continue
                    
                    # Extraer datos
                    data_recv = create_empty_data_dict(data_keys)
                    recv_data_operationbits(delete_data, data_recv)
                    recv_data_NoMachineProd(delete_data, data_recv)
                    recv_data_part(delete_data, data_recv)
                    
                    serial_number = data_recv.get('SerialNumber', '')
                    print(f"📥 {station_name}: Eliminando SerialNumber: {serial_number}")
                    
                    # Eliminar dato en MySQL
                    if sql_con:
                        dato_buscado = sqlreg.search_data(sql_con, 'front_cover', 'SerialNumber', serial_number)
                        if dato_buscado is None or dato_buscado == 3:
                            print(f"⚠️  {station_name}: Dato no encontrado en MySQL, no se puede eliminar")
                        if dato_buscado == 1 or dato_buscado == 2:
                            try:
                                #Copiar contenido de tabla antes de eliminar
                                sqlreg.get_Data_From_MySQL(sql_con, 'front_cover', data_recv, serial_number)
                                #Mover dato a tabla de eliminados
                                sqlreg.recv_Data_To_MySQL(sql_con, 'out_of_process', data_recv)
                                print(f"✅ {station_name}: Dato movido a tabla de eliminados")
                                #Eliminar dato de tabla principal
                                sqlreg.delete_data(sql_con, 'front_cover', 'SerialNumber', serial_number)
                                print(f"✅ {station_name}: Dato eliminado de MySQL")
                            except Exception as e:
                                print(f"❌ {station_name}: Error eliminando SQL: {e}")
                    
                    # Acknowledge al PLC
                    write_bit_safe(ack_incoming_db_offset, ack_incoming_byte_offset,
                                 ack_incoming_bit_offset, True)
                    time.sleep(0.1)
                    write_bit_safe(ack_incoming_db_offset, ack_incoming_byte_offset,
                                 ack_incoming_bit_offset, False)
                    
                    # Limpiar datos
                    data_recv = create_empty_data_dict(data_keys)

            # Pequeña pausa para no saturar CPU
            time.sleep(0.1)
            
        except KeyboardInterrupt:
            break
        except Exception as e:
            print(f"❌ Error en {station_name}: {e}")
            traceback.print_exc()
            
            # Reconexión en caso de error
            safe_disconnect()
            time.sleep(1)
    
    # Limpieza
    safe_disconnect()
    
    if sql_con:
        try:
            sql_con.close()
        except:
            pass
    
    print(f"🛑 Proceso de estación {station_name} terminado")

# ========== FUNCIÓN PRINCIPAL ==========

def main():
    """Función principal para pruebas"""
    # Configuración de ejemplo
    plc_config = ['192.168.0.10', 0, 2, 4, 0, 0, 42, 0]
    sql_config = ["localhost", "root", "acme2019", "plc_data"]
    
    # Validar que podemos serializar la configuración
    try:
        test_config = {
            'ip'        : plc_config[0],
            'rack'      : plc_config[1],
            'slot'      : plc_config[2],
            'hb_db'     : plc_config[3],
            'hb_byte'   : plc_config[4],
            'hb_bit'    : plc_config[5],
            'hb_byte_python': plc_config[6],
            'hb_bit_python' : plc_config[7],
            'host_name'     : sql_config[0],
            'user_name'     : sql_config[1],
            'user_password' : sql_config[2],
            'data_base_name': sql_config[3],
            'data_keys': [
                'OP5_Part_Present', 'OP5_Laser_Mark_Ok', 'OP5_Laser_Mark_Nok',
                'OP5_Code_Verify_Ok', 'OP5_Code_Verify_Nok', 'OP5_Code_Read',
                'OP5_Code_No_Read', 'OP5_Reserved1', 'JobNumber',
                'PartNumberNA', 'PartNumberEESA', 'SerialNumber', 'SupplierID'
            ]
        }
        
        # Test de serialización
        pickle.dumps(test_config)
        print("✅ Configuración serializable")
        
    except Exception as e:
        print(f"❌ Error de serialización: {e}")
        return
    
    # Crear instancia PLC
    plc = Plc(plc_config, sql_config)
    
    def signal_handler(sig, frame):
        print("\n👋 Recibida señal de interrupción")
        plc.stop()
        sys.exit(0)
    
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    try:
        if plc.start():
            print("\n✅ Sistema en ejecución")
            print("📊 Estado:")
            print(f"   - Procesos activos: {len(plc.station_processes) + 1}")
            print(f"   - Conectado al PLC: {'Sí' if plc.is_connected.value else 'No'}")
            print("\nPresiona Ctrl+C para detener...")
            
            # Mantener proceso principal vivo
            try:
                while plc.is_running.value:
                    # Mostrar estado periódicamente
                    time.sleep(5)
                    if plc.is_connected.value:
                        if plc.last_plc_response.value > 0:
                            last_resp = datetime.fromtimestamp(plc.last_plc_response.value)
                            elapsed = (datetime.now() - last_resp).total_seconds()
                            print(f"📡 PLC activo - Última respuesta hace {elapsed:.1f}s")
                        else:
                            print("📡 PLC activo - Esperando primera respuesta")
                    else:
                        print("🔌 Sin conexión al PLC - Reconectando...")
                        
            except KeyboardInterrupt:
                print("\n🛑 Interrupción por usuario")
                
    except Exception as e:
        print(f"❌ Error en main: {e}")
        traceback.print_exc()
    finally:
        plc.stop()

if __name__ == "__main__":
    # Es crucial que __name__ == "__main__" para multiprocessing en Windows
    print("=" * 60)
    print("SISTEMA PLC CON MULTIPROCESSING - MEJOR RECONEXIÓN")
    print("=" * 60)
    main()