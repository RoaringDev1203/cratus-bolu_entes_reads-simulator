import time
import psycopg2
from datetime import datetime, timezone
import struct
from pymodbus.client.sync import ModbusTcpClient
from pymodbus.exceptions import ModbusException

# Define PostgreSQL connection parameters
DB_NAME = "lapistest1"
DB_USER = "postgres"
DB_PASSWORD = "password"
DB_HOST = "localhost"
DB_PORT = "5432"

# Create the TCP connection
client = ModbusTcpClient('localhost', port=5020)

def modbus_registers_to_float(registers):
    raw = (registers[0] << 16) | registers[1]
    return struct.unpack('>f', struct.pack('>I', raw))[0]

def modbus_registers_to_signed_int(registers):
    if len(registers) < 2:
        raise ValueError("At least two registers are required")
    
    # Combine the two 16-bit registers into a 32-bit integer
    raw = (registers[0] << 16) | registers[1]
    
    # If the raw value is greater than or equal to 2^31, treat it as a negative number
    if raw >= 0x80000000:
        # Convert to signed by subtracting 2^32
        signed_value = raw - 0x100000000
    else:
        signed_value = raw

    return signed_value

def modbus_registers_to_long(registers):
    raw = (registers[0] << 48) | (registers[1] << 32) | (registers[2] << 16) | registers[3]
    return raw

modbus_to_key_map = {
    0: ['PhVphA', 'V', 0.1, "uint"],
    2: ['PhVphB', 'V', 0.1, "uint"],
    4: ['PhVphC', 'V', 0.1, "uint"],
    8: ['PPVphAB', 'V', 0.1, "uint"],
    10: ['PPVphBC', 'V', 0.1, "uint"],
    12: ['PPVphCA', 'V', 0.1, "uint"],
    14: ['AphA', 'A', 0.001, "uint"],
    16: ['AphB', 'A', 0.001, "uint"],
    18: ['AphC', 'A', 0.001, "uint"],
    22: ['neutral_current', 'A', 0.001, "uint"],
    24: ['measured_frequency', 'Hz', 0.01, "uint"],
    26: ['active_power_l1n', 'W', 1, "float"],
    28: ['active_power_l2n', 'W', 1, "float"],
    30: ['active_power_l3n', 'W', 1, "float"],
    34: ['total_import_active_power', 'kW', 0.001, "float"],
    36: ['total_export_active_power', 'W', 1, "float"],
    38: ['total_active_power', 'W', 1, "float"],
    40: ['reactive_power_l1', 'var', 1, "float"],
    42: ['reactive_power_l2', 'var', 1, "float"],
    44: ['reactive_power_l3', 'var', 1, "float"],
    48: ['quadrant_1_total_reactive_power', 'var', 1, "float"],
    50: ['quadrant_2_total_reactive_power', 'var', 1, "float"],
    52: ['quadrant_3_total_reactive_power', 'var', 1, "float"],
    54: ['quadrant_4_total_reactive_power', 'var', 1, "float"],
    56: ['total_reactive_power', 'var', 1, "float"],
    58: ['apparent_power_l1n', 'VA', 1, "float"],
    60: ['apparent_power_l2n', 'VA', 1, "float"],
    62: ['apparent_power_l3n', 'VA', 1, "float"],
    66: ['total_import_apparent_power', 'VA', 1, "float"],
    68: ['total_export_apparent_power', 'VA', 1, "float"],
    70: ['total_apparent_power', 'VA', 1, "float"],
    72: ['power_factor_l1', 'n/a', 0.001, "int"],
    74: ['power_factor_l2', 'n/a', 0.001, "int"],
    76: ['power_factor_l3', 'n/a', 0.001, "int"],
    80: ['power_factor_total', 'n/a', 0.001, "int"],
    82: ['cos_phi_l1', 'n/a', 0.001, "int"],
    84: ['cos_phi_l2', 'n/a', 0.001, "int"],
    86: ['cos_phi_l3', 'n/a', 0.001, "int"],
    90: ['cos_phi_total', 'n/a', 0.001, "int"],
    92: ['rotation_field', 'n/a', 1, "int"],
    94: ['voltage_unbalance', '%', 0.1, "uint"],
    96: ['current_unbalance', '%', 0.1, "uint"],
    98: ['l1_phase_voltage_angle', 'Angle', 0.1, "uint"],
    100: ['l2_phase_voltage_angle', 'Angle', 0.1, "uint"],
    102: ['l3_phase_voltage_angle', 'Angle', 0.1, "uint"],
    106: ['l1_phase_current_angle', 'Angle', 0.1, "uint"],
    108: ['l2_phase_current_angle', 'Angle', 0.1, "uint"],
    110: ['l3_phase_current_angle', 'Angle', 0.1, "uint"],
    154: ['hour_meter', 'Hr', 0.001, "uint"],
    158: ['input_status', 'n/a', 1, "uint"],
    160: ['output_sttus', 'n/a', 1, "uint"],

    #energe
    200: ['consumed_active_energy_l1', 'Wh', 1, "ulong"],
    204: ['consumed_active_energy_l2', 'Wh', 1, "ulong"],
    208: ['consumed_active_energy_l3', 'Wh', 1, "ulong"],
    216: ['total_consumed_active_energy', 'Wh', 1, "ulong"],
    220: ['delivered_active_energy_l1', 'Wh', 1, "ulong"],
    224: ['delivered_active_energy_l2', 'Wh', 1, "ulong"],
    228: ['delivered_active_energy_l3', 'Wh', 1, "ulong"],
    236: ['total_delivered_energy', 'Wh', 1, "ulong"],
    240: ['consumed_apparent_energy_l1', 'VAh', 1, "ulong"],
    244: ['consumed_apparent_energy_l2', 'VAh', 1, "ulong"],
    248: ['consumed_apparent_energy_l3', 'VAh', 1, "ulong"],
    256: ['total_consumed_apparent_energy', 'VAh', 1, "ulong"],
    260: ['delivered_apparent_energy_l1', 'VAh', 1, "ulong"],
    264: ['delivered_apparent_energy_l2', 'VAh', 1, "ulong"],
    268: ['delivered_apparent_energy_l3', 'VAh', 1, "ulong"],
    276: ['total_delivered_apparent_energy', 'VAh', 1, "ulong"],
    280: ['quadrant_1_reactive_energy_l1', 'Varh', 1, "ulong"],
    284: ['quadrant_1_reactive_energy_l2', 'Varh', 1, "ulong"],
    288: ['quadrant_1_reactive_energy_l3', 'Varh', 1, "ulong"],
    296: ['quadrant_1_total_reactive_energy', 'Varh', 1, "ulong"],
    300: ['quadrant_2_reactive_energy_l1', 'Varh', 1, "ulong"],
    304: ['quadrant_2_reactive_energy_l2', 'Varh', 1, "ulong"],
    308: ['quadrant_2_reactive_energy_l3', 'Varh', 1, "ulong"],
    316: ['quadrant_2_total_reactive_energy', 'Varh', 1, "ulong"],
    320: ['quadrant_3_reactive_energy_l1', 'Varh', 1, "ulong"],
    324: ['quadrant_3_reactive_energy_l2', 'Varh', 1, "ulong"],
    328: ['quadrant_3_reactive_energy_l3', 'Varh', 1, "ulong"],
    336: ['quadrant_3_total_reactive_energy', 'Varh', 1, "ulong"],
    340: ['quadrant_4_reactive_energy_l1', 'Varh', 1, "ulong"],
    344: ['quadrant_4_reactive_energy_l2', 'Varh', 1, "ulong"],
    348: ['quadrant_4_reactive_energy_l3', 'Varh', 1, "ulong"],
    356: ['quadrant_4_total_reactive_energy', 'Varh', 1, "ulong"],
    360: ['pulse_meter', 'Wh', 1, "uint"],
    362: ['total_pulse_meter_input_1', '-', 1, "uint"],
    364: ['total_pulse_meter_input_2', '-', 1, "uint"],
}

def read_register(register_address):
    try:
        key = modbus_to_key_map[register_address]
        if key[3] == "float":
            value = client.read_holding_registers(address=register_address, count=2, slave=0x01)
            return modbus_registers_to_float(value.registers)
        elif key[3] == "ulong":
            value = client.read_holding_registers(address=register_address, count=4, slave=0x01)
            return (value.registers[0] << 48) | (value.registers[1] << 32) | (value.registers[2] << 16) | value.registers[3]
        elif key[3] == "int":
            value = client.read_holding_registers(address=register_address, count=2, slave=0x01)
            return modbus_registers_to_signed_int(value.registers)
        else:
            value = client.read_holding_registers(address=register_address, count=2, slave=0x01)
            return (value.registers[0] << 16) | value.registers[1]
    except ModbusException as e:
        print(f"Modbus error reading register {register_address}: {e}")
    except Exception as e:
        print(f"Unexpected error reading register {register_address}: {e}")
    return None

def insert_into_table(arr, value):
    try:
        conn = psycopg2.connect(
            dbname=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD,
            host=DB_HOST,
            port=DB_PORT
        )
        data = arr[0]
        unit = arr[1]
        value = value * arr[2]
        print("Inserting", data, value, unit)
        cursor = conn.cursor()
        cursor.execute("INSERT INTO entes_reads (meter, measurement, value, unit, created_at) VALUES (%s, %s, %s, %s, %s)",
                       (1, data, value, unit, datetime.now(timezone.utc)))
        conn.commit()
        cursor.close()
        conn.close()
    except Exception as e:
        print(f"Error inserting data into PostgreSQL table: {e}")

def main():
    while True:
        for address, key in modbus_to_key_map.items():
            value = read_register(address)
            if value is not None:
                insert_into_table(key, value)
        time.sleep(5)

if __name__ == "__main__":
    main()
