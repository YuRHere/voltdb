#!/usr/bin/env python

# This file is part of VoltDB.
# Copyright (C) 2008-2010 VoltDB L.L.C.
#
# Permission is hereby granted, free of charge, to any person obtaining
# a copy of this software and associated documentation files (the
# "Software"), to deal in the Software without restriction, including
# without limitation the rights to use, copy, modify, merge, publish,
# distribute, sublicense, and/or sell copies of the Software, and to
# permit persons to whom the Software is furnished to do so, subject to
# the following conditions:
#
# The above copyright notice and this permission notice shall be
# included in all copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
# EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
# MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.
# IN NO EVENT SHALL THE AUTHORS BE LIABLE FOR ANY CLAIM, DAMAGES OR
# OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE,
# ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
# OTHER DEALINGS IN THE SOFTWARE.

import array
import socket
import struct
import datetime
import time
import decimal
try:
    from hashlib import sha1 as sha
except ImportError:
    from sha import sha

decimal.getcontext().prec = 38

def isNaN(d):
    """Since Python has the weird behavior that a float('nan') is not equal to
    itself, we have to test it by ourselves.
    """

    # work-around for Python 2.4
    s = array.array("d", [d])
    return (s.tostring() == "\x00\x00\x00\x00\x00\x00\xf8\x7f" or
            s.tostring() == "\x00\x00\x00\x00\x00\x00\xf0\x7f")

class FastSerializer:
    "Primitive type de/serialization in VoltDB formats"

    LITTLE_ENDIAN = '<'
    BIG_ENDIAN = '>'

    ARRAY = -99

    # VoltType enumerations
    VOLTTYPE_NULL = 1
    VOLTTYPE_TINYINT = 3  # int8
    VOLTTYPE_SMALLINT = 4 # int16
    VOLTTYPE_INTEGER = 5  # int32
    VOLTTYPE_BIGINT = 6   # int64
    VOLTTYPE_FLOAT = 8    # float64
    VOLTTYPE_STRING = 9
    VOLTTYPE_TIMESTAMP = 11 # 8 byte long
    VOLTTYPE_DECIMAL = 22  # 16 byte long
    VOLTTYPE_DECIMAL_STRING = 23  # 9 byte long
    VOLTTYPE_MONEY = 20     # 8 byte long
    VOLTTYPE_VOLTTABLE = 21

    # SQL NULL indicator for object type serializations (string, decimal)
    NULL_STRING_INDICATOR = -1
    NULL_DECIMAL_INDICATOR = -170141183460469231731687303715884105728

    # default decimal scale
    DEFAULT_DECIMAL_SCALE = 12

    # procedure call result codes
    PROC_OK = 0

    # there are assumptions here about datatype sizes which are
    # machine dependent. the program exits with an error message
    # if these assumptions are not true. it is further assumed
    # that host order is little endian. See isNaN().

    def __init__(self, host = None, port = None, username = None,
                 password = None):
        # connect a socket to host, port and get a file object
        self.wbuf = array.array('c')
        self.rbuf = ""
        self.host = host
        self.port = port

        self.socket = None
        if self.host != None and self.port != None:
            self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self.socket.setblocking(1)
            self.socket.setsockopt(socket.SOL_TCP, socket.TCP_NODELAY, 1)
            self.socket.connect((self.host, self.port))

        # input can be big or little endian
        self.inputBOM = self.BIG_ENDIAN  # byte order if input stream
        self.localBOM = self.LITTLE_ENDIAN  # byte order of host

        # Type to reader/writer mappings
        self.READER = {self.VOLTTYPE_NULL: self.readNull,
                       self.VOLTTYPE_TINYINT: self.readByte,
                       self.VOLTTYPE_SMALLINT: self.readInt16,
                       self.VOLTTYPE_INTEGER: self.readInt32,
                       self.VOLTTYPE_BIGINT: self.readInt64,
                       self.VOLTTYPE_FLOAT: self.readFloat64,
                       self.VOLTTYPE_STRING: self.readString,
                       self.VOLTTYPE_TIMESTAMP: self.readDate,
                       self.VOLTTYPE_DECIMAL: self.readDecimal,
                       self.VOLTTYPE_DECIMAL_STRING: self.readDecimalString}
        self.WRITER = {self.VOLTTYPE_NULL: self.writeNull,
                       self.VOLTTYPE_TINYINT: self.writeByte,
                       self.VOLTTYPE_SMALLINT: self.writeInt16,
                       self.VOLTTYPE_INTEGER: self.writeInt32,
                       self.VOLTTYPE_BIGINT: self.writeInt64,
                       self.VOLTTYPE_FLOAT: self.writeFloat64,
                       self.VOLTTYPE_STRING: self.writeString,
                       self.VOLTTYPE_TIMESTAMP: self.writeDate,
                       self.VOLTTYPE_DECIMAL: self.writeDecimal,
                       self.VOLTTYPE_DECIMAL_STRING: self.writeDecimalString}
        self.ARRAY_READER = {self.VOLTTYPE_TINYINT: self.readByteArray,
                             self.VOLTTYPE_SMALLINT: self.readInt16Array,
                             self.VOLTTYPE_INTEGER: self.readInt32Array,
                             self.VOLTTYPE_BIGINT: self.readInt64Array,
                             self.VOLTTYPE_FLOAT: self.readFloat64Array,
                             self.VOLTTYPE_STRING: self.readStringArray,
                             self.VOLTTYPE_TIMESTAMP: self.readDateArray,
                             self.VOLTTYPE_DECIMAL: self.readDecimalArray,
                             self.VOLTTYPE_DECIMAL_STRING:
                                 self.readDecimalStringArray}

        self.__compileStructs()

        self.NULL_DECIMAL_INDICATOR = \
            self.__intToBytes(self.__class__.NULL_DECIMAL_INDICATOR, 0)

        if username != None and password != None:
            self.authenticate(username, password)

    def __compileStructs(self):
        # Compiled structs for each type
        self.byteType = lambda length : '%c%db' % (self.inputBOM, length)
        self.ubyteType = lambda length : '%c%dB' % (self.inputBOM, length)
        self.int16Type = lambda length : '%c%dh' % (self.inputBOM, length)
        self.int32Type = lambda length : '%c%di' % (self.inputBOM, length)
        self.int64Type = lambda length : '%c%dq' % (self.inputBOM, length)
        self.uint64Type = lambda length : '%c%dQ' % (self.inputBOM, length)
        self.float64Type = lambda length : '%c%dd' % (self.inputBOM, length)
        self.stringType = lambda length : '%c%ds' % (self.inputBOM, length)

    def close(self):
        self.socket.close()

    def authenticate(self, username, password):
        # Requires sending a length preceded username and password even if
        # authentication is turned off.

        #protocol version
        self.writeByte(0)

        if username:
            # utf8 encode supplied username
            self.writeString(username)
        else:
            # no username, just output length of 0
            self.writeString("")

        # password supplied, sha-1 hash it
        m = sha()
        m.update(password)
        pwHash = m.digest()
        self.wbuf.extend(pwHash)

        self.prependLength()
        self.flush()

        # A length, version number, and status code is returned
        self.rbuf = self.socket.recv(4)
        self.rbuf = self.socket.recv(self.readInt32())
        self.readByte()

        if self.readByte() != 0:
            raise SystemExit("Authentication failed.")

        self.readInt32()
        self.readInt64()
        self.readInt64()
        self.readInt32()
        for x in range(self.readInt32()):
            self.readByte()

    def setInputByteOrder(self, bom):
        # assuming bom is high bit set?
        if bom == 1:
            self.inputBOM = self.LITTLE_ENDIAN
        else:
            self.inputBOM = self.BIG_ENDIAN

        # recompile the structs
        self.__compileStructs()

    def prependLength(self):
        # write 32 bit array length at offset 0, NOT including the
        # size of this length preceding value. This value is written
        # in the network order.
        ttllen = self.wbuf.buffer_info()[1] * self.wbuf.itemsize
        lenBytes = struct.pack(self.inputBOM + 'i', ttllen)
        map(lambda x: self.wbuf.insert(0, x), lenBytes[::-1])

    def size(self):
        """Returns the size of the write buffer.
        """

        return (self.wbuf.buffer_info()[1] * self.wbuf.itemsize)

    def flush(self):
        if self.socket == None:
            print "ERROR: not connected to server."
            exit(-1)

        self.socket.sendall(self.wbuf.tostring())
        self.wbuf = array.array('c')

    def bufferForRead(self):
        if self.socket == None:
            print "ERROR: not connected to server."
            exit(-1)

        # fully buffer a new length preceded message from socket
        # read the length. the read until the buffer is completed.
        responseprefix = ""
        while (len(responseprefix) < 4):
            responseprefix += self.socket.recv(4 - len(responseprefix))
            if responseprefix == "":
                raise IOError("Connection broken")
        responseLength = struct.unpack(self.int32Type(1), responseprefix)[0]
        while (len(self.rbuf) < responseLength):
            self.rbuf += self.socket.recv(responseLength - len(self.rbuf))

    def read(self, type):
        if type not in self.READER:
            print "ERROR: can't read wire type(", type, ") yet."
            exit(-2)

        return self.READER[type]()

    def write(self, type, value):
        if type not in self.WRITER:
            print "ERROR: can't write wire type(", type, ") yet."
            exit(-2)

        return self.WRITER[type](value)

    def readWireType(self):
        type = self.readByte()
        return self.read(type)

    def writeWireType(self, type, value):
        if type not in self.WRITER:
            print "ERROR: can't write wire type(", type, ") yet."
            exit(-2)

        self.writeByte(type)
        return self.write(type, value)

    def getRawBytes(self):
        return self.wbuf

    def writeRawBytes(self, value):
        """Appends the given raw bytes to the end of the write buffer.
        """

        self.wbuf.extend(value)

    def __str__(self):
        return repr(self.wbuf)

    def readArray(self, type):
        if type not in self.ARRAY_READER:
            print "ERROR: can't read wire type(", type, ") yet."
            exit(-2)

        return self.ARRAY_READER[type]()

    def readNull(self):
        return None

    def writeNull(self, value):
        return

    def writeArray(self, type, array):
        if (not array) or (len(array) == 0) or (not type):
            return

        if type not in self.WRITER:
            print "ERROR: Unsupported date type (", type, ")."
            exit(-2)

        self.writeInt16(len(array))

        for i in array:
            self.WRITER[type](i)

    def writeWireTypeArray(self, type, array):
        if type not in self.WRITER:
            print "ERROR: can't write wire type(", type, ") yet."
            exit(-2)

        self.writeByte(type)
        self.writeArray(type, array)

    # byte
    def readByteArrayContent(self, cnt):
        offset = cnt * struct.calcsize('b')
        val = struct.unpack(self.byteType(cnt), self.rbuf[:offset])
        self.rbuf = self.rbuf[offset:]
        return val

    def readByteArray(self):
        length = self.readInt16()
        return self.readByteArrayContent(length)

    def readByte(self):
        return self.readByteArrayContent(1)[0]

    def writeByte(self, value):
        self.wbuf.extend(struct.pack(self.byteType(1), value))

    # int16
    def readInt16ArrayContent(self, cnt):
        offset = cnt * struct.calcsize('h')
        val = struct.unpack(self.int16Type(cnt), self.rbuf[:offset])
        self.rbuf = self.rbuf[offset:]
        return val

    def readInt16Array(self):
        length = self.readInt16()
        return self.readInt16ArrayContent(length)

    def readInt16(self):
        return self.readInt16ArrayContent(1)[0]

    def writeInt16(self, value):
        self.wbuf.extend(struct.pack(self.int16Type(1), value))

    # int32
    def readInt32ArrayContent(self, cnt):
        offset = cnt * struct.calcsize('i')
        val = struct.unpack(self.int32Type(cnt), self.rbuf[:offset])
        self.rbuf = self.rbuf[offset:]
        return val

    def readInt32Array(self):
        length = self.readInt16()
        return self.readInt32ArrayContent(length)

    def readInt32(self):
        return self.readInt32ArrayContent(1)[0]

    def writeInt32(self, value):
        self.wbuf.extend(struct.pack(self.int32Type(1), value))

    # int64
    def readInt64ArrayContent(self, cnt):
        offset = cnt * struct.calcsize('q')
        val = struct.unpack(self.int64Type(cnt), self.rbuf[:offset])
        self.rbuf = self.rbuf[offset:]
        return val

    def readInt64Array(self):
        length = self.readInt16()
        return self.readInt64ArrayContent(length)

    def readInt64(self):
        return self.readInt64ArrayContent(1)[0]

    def writeInt64(self, value):
        self.wbuf.extend(struct.pack(self.int64Type(1), value))

    # float64
    def readFloat64ArrayContent(self, cnt):
        offset = cnt * struct.calcsize('d')
        val = struct.unpack(self.float64Type(cnt), self.rbuf[:offset])
        self.rbuf = self.rbuf[offset:]
        return val

    def readFloat64Array(self):
        length = self.readInt16()
        return self.readFloat64ArrayContent(length)

    def readFloat64(self):
        return self.readFloat64ArrayContent(1)[0]

    def writeFloat64(self, value):
        # work-around for python 2.4
        tmp = array.array("d", [value])
        if self.inputBOM != self.localBOM:
            tmp.byteswap()
        self.wbuf.extend(tmp.tostring())

    # string
    def readStringContent(self, cnt):
        if cnt == 0:
            return ""

        offset = cnt * struct.calcsize('c')
        val = struct.unpack(self.stringType(cnt), self.rbuf[:offset])
        self.rbuf = self.rbuf[offset:]
        return val[0].decode("utf-8")

    def readString(self):
        # length preceeded (2 byte value) string
        length = self.readInt32()
        if length == self.NULL_STRING_INDICATOR:
            return None
        return self.readStringContent(length)

    def readStringArray(self):
        retval = []
        cnt = self.readInt16()

        for i in xrange(cnt):
            retval.append(self.readString())

        return tuple(retval)

    def writeString(self, value):
        if value is None:
            self.writeInt32(self.NULL_STRING_INDICATOR)
            return

        encoded_value = value.encode("utf-8")
        self.writeInt32(len(encoded_value))
        self.wbuf.extend(encoded_value)

    # date
    def readDate(self):
        # microseconds before or after Jan 1, 1970
        return datetime.datetime.fromtimestamp(self.readInt64()/1000000.0)

    def readDateArray(self):
        retval = []
        raw = self.readInt64Array()

        for i in raw:
            retval.append(datetime.datetime.fromtimestamp(i/1000000.0))

        return tuple(retval)

    def writeDate(self, value):
        val = int(time.mktime(value.timetuple())*1000000)
        self.wbuf.extend(struct.pack(self.int64Type(1), val))

    def readDecimal(self):
        offset = 16 * struct.calcsize('b')
        if self.rbuf[:offset] == self.NULL_DECIMAL_INDICATOR:
            self.rbuf = self.rbuf[offset:]
            return None
        val = list(struct.unpack(self.ubyteType(16), self.rbuf[:offset]))
        self.rbuf = self.rbuf[offset:]
        mostSignificantBit = 1 << 7
        isNegative = (val[0] & mostSignificantBit) != 0
        val[0] &= mostSignificantBit
        unscaledValue = 0
        for x in xrange(1, 16):
            unscaledValue += val[x] << ((15 - x) * 8)
        unscaledValue = tuple(str(unscaledValue))
        unscaledValueDigits = []
        for x in unscaledValue:
            unscaledValueDigits.append(int(x))
        return decimal.Decimal((isNegative, tuple(unscaledValueDigits),
                                -self.__class__.DEFAULT_DECIMAL_SCALE))

    def readDecimalArray(self):
        retval = []
        cnt = self.readInt16()
        for i in xrange(cnt):
            retval.append(self.readDecimal())
        return tuple(retval)

    def readDecimalString(self):
        encoded_string = self.readString()
        if encoded_string == None:
            return None
        val = decimal.Decimal(encoded_string)
        (sign, digits, exponent) = val.as_tuple()
        if -exponent > self.__class__.DEFAULT_DECIMAL_SCALE:
            raise ValueError("Scale of this decimal is %d and the max is 12"
                             % (-exponent))
        if len(digits) > 38:
            raise ValueError("Precision of this decimal is %d and the max is 38"
                             % (len(digits)))
        return val

    def readDecimalStringArray(self):
        retval = []
        cnt = self.readInt16()
        for i in xrange(cnt):
            retval.append(self.readDecimalString())
        return tuple(retval)

    def __intToBytes(self, value, sign):
        value_bytes = ""
        while value > 0:
            value_bytes = struct.pack(self.uint64Type(1),
                                      value & 0xffffffffffffffffL) + value_bytes
            value = value >> 64
        if len(value_bytes) > 15:
            raise ValueError("Precision of this decimal is >38 digits");
        if sign == 1:
            ret = struct.pack(self.ubyteType(1), 1 << 7)
        else:
            ret = struct.pack(self.ubyteType(1), 0)
        ret += struct.pack(self.ubyteType(1), 0) * (15 - len(value_bytes))
        ret += value_bytes
        return ret

    def writeDecimal(self, num):
        if num is None:
            self.wbuf.extend(self.NULL_DECIMAL_INDICATOR)
            return
        if not isinstance(num, decimal.Decimal):
            raise TypeError("num must be of the type decimal.Decimal")
        (sign, digits, exponent) = num.as_tuple()
        precision = len(digits)
        scale = -exponent
        if (scale > self.__class__.DEFAULT_DECIMAL_SCALE):
            raise ValueError("Scale of this decimal is %d and the max is 12"
                             % (scale))
        rest = precision - scale
        if rest > 26:
            raise ValueError("Precision to the left ot the decimal point is %d"
                             " and the max is 26" % (rest))
        scale_factor = self.__class__.DEFAULT_DECIMAL_SCALE - scale
        unscaled_int = int(decimal.Decimal((0, digits, scale_factor)))
        data = self.__intToBytes(unscaled_int, sign)
        self.wbuf.extend(data)

    def writeDecimalString(self, num):
        if num is None:
            self.writeString(None)
            return
        if not isinstance(num, decimal.Decimal):
            raise TypeError("num must be of type decimal.Decimal")
        self.writeString(num.to_eng_string())

    # cash!
    def readMoney(self):
        # money-unit * 10,000
        return self.readInt64()

class VoltColumn:
    "definition of one VoltDB table column"
    def __init__(self, fser = None, type = None, name = None):
        if fser != None:
            self.type = fser.readByte()
        elif type != None and name != None:
            self.type = type
            self.name = name

    def __str__(self):
        # If the name is empty, use the default "modified tuples". Has to do
        # this because HSQLDB doesn't return a column name if the table is
        # empty.
        return "(%s: %d)" % (self.name and self.name or "modified tuples",
                             self.type)

    def __eq__(self, other):
        # For now, if we've been through the query on a column with no name,
        # just assume that there's no way the types are matching up cleanly
        # and there ain't no one for to give us no pain
        if (not self.name or not other.name):
            return True
        return (self.type == other.type and self.name == other.name)

    def readName(self, fser):
        self.name = fser.readString()

    def writeType(self, fser):
        fser.writeByte(self.type)

    def writeName(self, fser):
        fser.writeString(self.name)

class VoltTable:
    "definition and content of one VoltDB table"
    def __init__(self, fser):
        self.fser = fser
        self.columns = []  # column defintions
        self.tuples = []

    def __str__(self):
        result = ""

        result += "column count: %d\n" % (len(self.columns))
        result += "row count: %d\n" % (len(self.tuples))
        result += "cols: "
        result += ", ".join(map(lambda x: str(x), self.columns))
        result += "\n"
        result += "rows -\n"
        result += "\n".join(map(lambda x: str(x), self.tuples))

        return result

    def __getstate__(self):
        return (self.columns, self.tuples)

    def __setstate__(self, state):
        self.fser = None
        self.columns, self.tuples = state

    def __eq__(self, other):
        if len(self.tuples) > 0:
            return (self.columns == other.columns) and \
                (self.tuples == other.tuples)
        return (self.tuples == other.tuples)

    # The VoltTable is always serialized in big-endian order.
    #
    # How to read a table off the wire.
    # 1. Read the length of the whole table
    # 2. Read the columns
    #    a. read the column header size
    #    a. read the column count
    #    b. read column definitions.
    # 3. Read the tuples count.
    #    a. read the row count
    #    b. read tuples recording string lengths
    def readFromSerializer(self):
        # 1.
        tablesize = self.fser.readInt32()

        # 2.
        headersize = self.fser.readInt32()
        columncount = self.fser.readInt16()
        for i in xrange(columncount):
            column = VoltColumn(fser = self.fser)
            self.columns.append(column)
        map(lambda x: x.readName(self.fser), self.columns)

        # 3.
        rowcount = self.fser.readInt32()
        for i in xrange(rowcount):
            rowsize = self.fser.readInt32()
            # list comprehension: build list by calling read for each column in
            # row/tuple
            row = [self.fser.read(self.columns[j].type)
                   for j in xrange(columncount)]
            self.tuples.append(row)

        return self

    def writeToSerializer(self):
        table_fser = FastSerializer()

        # We have to pack the header into a buffer first so that we can
        # calculate the size
        header_fser = FastSerializer()

        header_fser.writeInt16(len(self.columns))
        map(lambda x: x.writeType(header_fser), self.columns)
        map(lambda x: x.writeName(header_fser), self.columns)

        table_fser.writeInt32(header_fser.size() - 4)
        table_fser.writeRawBytes(header_fser.getRawBytes())

        table_fser.writeInt32(len(self.tuples))
        for i in self.tuples:
            row_fser = FastSerializer()

            map(lambda x: row_fser.write(self.columns[x].type, i[x]),
                xrange(len(i)))

            table_fser.writeInt32(row_fser.size())
            table_fser.writeRawBytes(row_fser.getRawBytes())

        table_fser.prependLength()
        self.fser.writeRawBytes(table_fser.getRawBytes())


class VoltException:
    # Volt SerializableException enumerations
    VOLTEXCEPTION_NONE = 0
    VOLTEXCEPTION_EEEXCEPTION = 1
    VOLTEXCEPTION_SQLEXCEPTION = 2
    VOLTEXCEPTION_CONSTRAINTFAILURE = 3
    VOLTEXCEPTION_GENERIC = 4

    def __init__(self, fser):
        self.length = fser.readInt16()
        if self.length == 0:
            self.type = self.VOLTEXCEPTION_NONE
            return
        self.type = fser.readByte()
        # quick and dirty exception skipping
        if self.type == self.VOLTEXCEPTION_NONE:
            return

        self.message = []
        self.message_len = fser.readInt16()
        for i in xrange(0, self.message_len):
            self.message.append(chr(fser.readByte()))
        self.message = ''.join(self.message)

        if self.type == self.VOLTEXCEPTION_GENERIC:
            print "Python client got a generic serializable exception:", \
                self.message
        elif self.type == self.VOLTEXCEPTION_EEEXCEPTION:
            # serialized size from EEException.java is 4 bytes
            self.error_code = fser.readInt32()
            print "Exception was a Volt EE Exception, error code: %d" % \
                (self.error_code)
        elif self.type == self.VOLTEXCEPTION_SQLEXCEPTION or \
                self.type == self.VOLTEXCEPTION_CONSTRAINTFAILURE:
            self.sql_state_bytes = []
            for i in xrange(0, 5):
                self.sql_state_bytes.append(chr(fser.readByte()))
            self.sql_state_bytes = ''.join(self.sql_state_bytes)

            if self.type == self.VOLTEXCEPTION_SQLEXCEPTION:
                print "Exception was a Volt SQL Exception ", self.sql_state_bytes
            else:
                self.constraint_type = fser.readInt32()
                self.table_id = fser.readInt32()
                self.buffer_size = fser.readInt32()
                self.buffer = []
                for i in xrange(0, self.buffer_size):
                    self.buffer.append(fser.readByte())
                print "Exception was a Volt Constraint Failure Exception" \
                    " of type %d on table ID %d" % (self.constraint_type,
                                                    self.table_id)
        else:
            for i in xrange(0, self.length - 3 - 2 - self.message_len):
                fser.readByte()
            print "Python client deserialized unknown VoltException."

class VoltResponse:
    "VoltDB called procedure response (ClientResponse.java)"
    def __init__(self, fser):
        self.fser = fser
        # serialization order: response-length, status, roundtripTime, exception,
        # tables[], info, id.
        self.fser.bufferForRead()
        self.version = self.fser.readByte()
        self.status = self.fser.readByte()
        self.roundtripTime = self.fser.readInt32()
        self.exception = VoltException(self.fser)

        # tables[]
        tablecount = self.fser.readInt16()
        self.tables = []
        for i in xrange(tablecount):
            table = VoltTable(self.fser)
            self.tables.append(table.readFromSerializer())
        # info, id
        self.info = self.fser.readString()
        self.clientHandle = self.fser.readInt64()

    def __str__(self):
        tablestr = "\n\n".join([str(i) for i in self.tables])
        return "Status: %d\nInformation: %s\n%s" % (self.status, self.info,
                                                    tablestr)

class VoltProcedure:
    "VoltDB called procedure interface"
    def __init__(self, fser, name, paramtypes = []):
        self.fser = fser             # FastSerializer object
        self.name = name             # procedure class name
        self.paramtypes = paramtypes # list of fser.WIRE_* values

    def call(self, params = None, response = True, timeout = None):
        self.fser.writeByte(0)  # version number
        self.fser.writeString(self.name)
        self.fser.writeInt64(1)            # client handle
        self.fser.writeInt16(len(self.paramtypes))
        for i in xrange(len(self.paramtypes)):
            try:
                iter(params[i]) # Test if this is an array
                if isinstance(params[i], basestring): # String is a special case
                    raise TypeError

                self.fser.writeByte(FastSerializer.ARRAY)
                self.fser.writeByte(self.paramtypes[i])
                self.fser.writeArray(self.paramtypes[i], params[i])
            except TypeError:
                self.fser.writeWireType(self.paramtypes[i], params[i])
        self.fser.prependLength() # prepend the total length of the invocation
        self.fser.flush()
        self.fser.socket.settimeout(timeout) # timeout exception will be raised
        return response and VoltResponse(self.fser) or None
