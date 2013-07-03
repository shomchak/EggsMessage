'''
@author Shomik Chakravarty (shomik.chakravarty@gmail.com)

Constants for the epc_utils.
'''

EPC_METHOD_NAME = "METHOD_NAME:"  # delimiter for an upcoming method name
EPC_ARGUMENTS = "ARGUMENTS:"  # delimiter for upcoming arguments
EPC_REMOTE_PREFIX = "_remote__"  # prefix for wrapped RPC methods
EPC_RETURN_VALUE = "RETURN_VALUE:"  # delimiter for a return value
EPC_EXCEPTION = "EXCEPTION: "  # delimiter for an exception


class UnmarshallingError(Exception):
    pass


class MarshallingError(Exception):
    pass


class RemoteError(Exception):
    pass


class TimeoutError(Exception):
    pass
