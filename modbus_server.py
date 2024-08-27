from pymodbus.server.sync import StartTcpServer
from pymodbus.device import ModbusDeviceIdentification
from pymodbus.datastore import ModbusSequentialDataBlock, ModbusSlaveContext, ModbusServerContext
import logging

# Configure logging
logging.basicConfig()
log = logging.getLogger()
log.setLevel(logging.DEBUG)

# Create data store with some initial values
store = ModbusSlaveContext(
    hr=ModbusSequentialDataBlock(0, [50] * 1000)
)
context = ModbusServerContext(slaves=store, single=True)

# Initialize the server
identity = ModbusDeviceIdentification()
identity.VendorName = 'pymodbus'
identity.ProductCode = 'PM'
identity.VendorUrl = 'http://github.com/riptideio/pymodbus/'
identity.ProductName = 'pymodbus Server'
identity.ModelName = 'pymodbus Server'
identity.MajorMinorRevision = '1.0'

# Start TCP server
StartTcpServer(context, identity=identity, address=("localhost", 5020))
