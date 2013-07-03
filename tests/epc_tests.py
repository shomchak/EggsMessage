'''
@author Shomik Chakravarty (shomik.chakravarty@gmail.com)

Tests for the EggsServer, EggsServerMeta, EggsClient, and EggsClientMeta classes.
'''


import sys
import os
import signal
import time
import unittest
import abc
import zmq
import multiprocessing

import epc_test_buffs_pb2 as buffs

sys.path.append("../lib")
from epc_server import EggsServer, EggsServerMeta
from epc_client import EggsClient, EggsClientMeta
from epc_constants import *


class AbstractClass1(object):
    __metaclass__ = abc.ABCMeta

    @abc.abstractmethod
    def method1(self):
        return None

    @abc.abstractmethod
    def method2(self, a=buffs.Integer()):
        return buffs.String()

    @abc.abstractmethod
    def returns1(self):
        return buffs.String()

    @abc.abstractmethod
    def returns2(self):
        return buffs.String(), buffs.Integer()

    @abc.abstractmethod
    def bad_method1(self):
        raise Exception

    @abc.abstractmethod
    def bad_method2():
        return None

    @abc.abstractmethod
    def bad_method3(self, a):
        return None

    @abc.abstractmethod
    def bad_method4(self):
        return None

    @abc.abstractmethod
    def mismatched1(self):
        return buffs.String()

    @abc.abstractmethod
    def mismatched2(self):
        return buffs.String(), buffs.String()

    @abc.abstractmethod
    def takes2(self, a=buffs.String(), b=buffs.Integer()):
        return buffs.String(), buffs.Integer()

    @abc.abstractmethod
    def non_proto_arg(self, a=1):
        return None

    @abc.abstractmethod
    def echo(self, a=buffs.String()):
        return buffs.String()

    @abc.abstractmethod
    def wrong_types1(self, a=buffs.String()):
        return buffs.String()


class AbstractClass2(object):
    __metaclass__ = abc.ABCMeta

    @abc.abstractmethod
    def method2(self, a=buffs.String()):
        return buffs.String()

    @abc.abstractmethod
    def returns1(self):
        return buffs.String()

    @abc.abstractmethod
    def returns2(self):
        return buffs.String(), buffs.Integer()

    @abc.abstractmethod
    def bad_method1(self):
        raise Exception

    @abc.abstractmethod
    def bad_method2():
        return None

    @abc.abstractmethod
    def bad_method3(self, a):
        return None

    @abc.abstractmethod
    def bad_method4(self):
        return None

    @abc.abstractmethod
    def mismatched1(self):
        return buffs.String()

    @abc.abstractmethod
    def mismatched2(self):
        return buffs.String(), buffs.String()

    @abc.abstractmethod
    def takes2(self, a=buffs.String(), b=buffs.Integer()):
        return buffs.String(), buffs.Integer()

    @abc.abstractmethod
    def non_proto_arg(self, a=1):
        return None

    @abc.abstractmethod
    def echo(self, a=buffs.String()):
        return buffs.String()

    @abc.abstractmethod
    def wrong_types1(self, a=buffs.Big()):
        return buffs.String()


class AbstractClass3(object):
    __metaclass__ = abc.ABCMeta

    @abc.abstractmethod
    def method1(self):
        return buffs.Integer(), buffs.Integer(), buffs.String()


class Server1(AbstractClass1):
    __metaclass__ = EggsServerMeta

    def method1(self):
        return None

    def method2(self, a=buffs.Integer()):
        return buffs.String(value='method2 return value')

    def returns1(self):
        return buffs.String(value='returns1 return value')

    def returns2(self):
        return buffs.String(value='1'), buffs.Integer(value=2)

    def bad_method1(self):
        raise Exception

    def bad_method2(self):
        return Exception

    def bad_method3(self, a):
        return None

    def bad_method4(self):
        raise Exception

    def mismatched1(self):
        return buffs.String()

    def mismatched2(self):
        return buffs.String(), buffs.String()

    def takes2(self, a, b):
        return buffs.String(value='takes2 return value'), buffs.Integer(value=3)

    def non_proto_arg(self, a):
        return None

    def echo(self, a):
        return a

    def wrong_types1(self, a):
        return buffs.String()


class Server2(AbstractClass2):
    __metaclass__ = EggsServerMeta

    def method2(self, a):
        return buffs.String(value='method2 return value')

    def returns1(self):
        return buffs.String(value='returns1 return value')

    def returns2(self):
        return buffs.String(value='1'), buffs.Integer(value=2)

    def bad_method1(self):
        raise Exception

    def bad_method2(self):
        return Exception

    def bad_method3(self, a):
        return None

    def bad_method4(self):
        raise Exception

    def mismatched1(self):
        return buffs.String()

    def mismatched2(self):
        return buffs.String(), buffs.String()

    def takes2(self, a, b):
        return buffs.String(value='takes2 return value'), buffs.Integer(value=3)

    def non_proto_arg(self, a):
        return None

    def echo(self, a):
        return a

    def wrong_types1(self, a):
        return buffs.String()


class Server3(AbstractClass3):
    __metaclass__ = EggsServerMeta

    def __init__(self):
        self.i = 0
        print self.i

    def main(self, a=0, b='b'):
        self.a = a
        self.b = b
        with self.eggs_lock:
            self.i += 1
        print self.i
        time.sleep(0.25)

    def method1(self):
        self.i += 1
        print self.i
        return buffs.Integer(value=self.i), buffs.Integer(value=self.a), buffs.String(value=self.b)


class Client1(AbstractClass1):
    __metaclass__ = EggsClientMeta


class Client2(AbstractClass2):
    __metaclass__ = EggsClientMeta


class Client3(AbstractClass3):
    __metaclass__ = EggsClientMeta


class ClientServerInteractionTests(unittest.TestCase):
    '''
    Tests the usual use cases of making remote procedure calls.
    Makes a bunch of RPCs in a row and makes sure we can handle
    exceptions properly on both ends. These tests shouldn't have
    to change when messing with implementation details; these test
    the outside-facing interaction of the client-server model.
    '''

    def test_calls_same_abstract(self):
        '''
        When both the client and server have exactly the same abstract
        classes.
        '''
        with Client1() as client:
            def run_server():
                with Server1() as server:
                    server.run()

            server_process = multiprocessing.Process(target=run_server)
            try:
                server_process.start()
                time.sleep(0.01)
                pid = server_process.pid

                # spam that poop
                for i in xrange(100):
                    # CASE 1: no args, no return
                    # invoke an RPC, and try to get a response
                    response = client.method1()
                    # check the return value
                    self.assertEqual(response, None)

                    # CASE 2: an arg, a return value
                    # invoke an RPC, and try to get a response
                    response = client.echo(buffs.String(value="echo this"))
                    # check the return value
                    self.assertEqual(response.value, "echo this")

                    # CASE 3: kwargs, multiple return values, success
                    # invoke an RPC, and try to get a response
                    response = client.takes2(buffs.String(value='sup'), b=buffs.Integer(value=2))
                    # check the return value
                    self.assertEqual(len(response), 2)
                    self.assertEqual(response[0].value, 'takes2 return value')
                    self.assertEqual(response[1].value, 3)

                    # CASE 4: remote exception
                    with self.assertRaises(RemoteError):
                        client.bad_method4()

                    # CASE 5a: local exception from bad abstract definition
                    with self.assertRaises(TypeError):
                        client.bad_method1()
                    # CASE 5b: local exception from too many args
                    with self.assertRaises(SyntaxError):
                        client.method2(1, 3)
                    # CASE 5b: local exception from args of wrong type
                    with self.assertRaises(TypeError):
                        client.method2(a=buffs.String())
                    # CASE 5b: local exception from args of wrong type again
                    with self.assertRaises(TypeError):
                        client.method2(b=buffs.Integer())

                    # CASE 6: eggs_ping
                    response = client.eggs_ping()
                    # check the return value
                    self.assertEqual(response, 'eggs_pong')

            finally:
                # kill the server
                print "Killing server..."
                os.kill(pid, signal.SIGTERM)
                server_process.join()

    def test_call_different_abstract(self):
        '''
        Test the case where the abstract classes have fallen out of sync.
        '''

        with Client1() as client:
            def run_server():
                # this server doesn't have method1
                with Server2() as server:
                    server.run()

            server_process = multiprocessing.Process(target=run_server)
            try:
                server_process.start()
                time.sleep(0.01)
                pid = server_process.pid

                # confuse the server by invoking a method it doesn't know about
                with self.assertRaises(TimeoutError):
                    client.method1()
                # but it should recover and we should still be able to ping it
                response = client.eggs_ping()
                self.assertEqual(response, "eggs_pong")

                # invoke an RPC, and try to get a response
                response = client.echo(buffs.String(value="echo this"))
                # check the return value
                self.assertEqual(response.value, "echo this")

            finally:
                # kill the server
                print "Killing server..."
                os.kill(pid, signal.SIGTERM)
                server_process.join()

    def test_address(self):
        '''
        Tests that we can set the server endpoint properly.
        '''
        ep1 = "tcp://127.0.0.1:8082"
        ep2 = "tcp://127.0.0.1:8083"

        # CASE 1: same non default endpoints
        with Client1(server_endpoint=ep1) as client:
            def run_server():
                with Server1(server_endpoint=ep1) as server:
                    server.run()

            server_process = multiprocessing.Process(target=run_server)
            try:
                server_process.start()
                time.sleep(0.01)
                pid = server_process.pid

                response = client.eggs_ping()
                self.assertEqual(response, "eggs_pong")

            finally:
                # kill the server
                print "Killing server..."
                os.kill(pid, signal.SIGTERM)
                server_process.join()

        # CASE 2: different endpoints
        with Client1(server_endpoint=ep1) as client:
            def run_server():
                with Server1(server_endpoint=ep2) as server:
                    server.run()

            server_process = multiprocessing.Process(target=run_server)
            try:
                server_process.start()
                time.sleep(0.01)
                pid = server_process.pid

                with self.assertRaises(TimeoutError):
                    client.eggs_ping()

            finally:
                # kill the server
                print "Killing server..."
                os.kill(pid, signal.SIGTERM)
                server_process.join()


    def test_main_call(self):
        '''
        Test that a custom __init__ method gets called for the server class,
        and that a custom main method runs while the server is also handling
        requests.
        '''
        with Client3() as client:
            def run_server():
                with Server3() as server:
                    server.run(a=333, b='ggg')

            server_process = multiprocessing.Process(target=run_server)
            try:
                server_process.start()
                time.sleep(0.01)
                pid = server_process.pid

                # The __init__ method sets server.i to 0, the main method
                # increments it by 1, the RPC method1() below increments
                # it by 1 again, so after everything server.i should be 2
                response = client.method1()
                self.assertEqual(len(response), 3)
                self.assertEqual(response[0].value, 2)
                # this tests that we can pass args to our main method
                self.assertEqual(response[1].value, 333)
                self.assertEqual(response[2].value, 'ggg')
                time.sleep(0.3)  # wait for the server to stop

            finally:
                # kill the server
                print "Killing server..."
                os.kill(pid, signal.SIGTERM)
                server_process.join()


class EggsServerTests(unittest.TestCase):
    '''
    Tests the individual functions in the EggsServer. These tests will have to
    change in sync with change to the inner workings of these functions.
    '''
    def test_init(self):
        with Server1() as server:
            # these should exist, so referencing them shouldn't raise an exception
            server._EggsServer__context
            server._EggsServer__server_socket
            self.assertEqual(server._EggsServer__server_socket.getsockopt(zmq.LINGER), 0)

    def test_serialize_return(self):
        with Server1() as server:
            # CASE 1: hand it a template and return value of mismatch iterablenesses
            with self.assertRaises(TypeError):
                server._serialize_return(['iterable'], 2)
            with self.assertRaises(TypeError):
                server._serialize_return(2, ['iterable'])

            # CASE 2: they are both iterable and have different lengths
            with self.assertRaises(MarshallingError):
                server._serialize_return(['length', 'two'], ['length one'])

            # CASE 3: they have the same lengths but different types
            with self.assertRaises(TypeError):
                server._serialize_return([1], ['string'])

            # CASE 4: they have the same lengths but the return value isn't serializeable
            with self.assertRaises(TypeError):
                server._serialize_return(['sup'], ['sup'])

            # CASE 5: neither is iterable and the types are wrong
            with self.assertRaises(TypeError):
                server._serialize_return(2, None)

            # CASE 6: non iterable and not serializeable
            with self.assertRaises(TypeError):
                server._serialize_return(2, 2)

            # CASE 7: successfully return nothing
            return_message = server._serialize_return(None, None)
            self.assertEqual(return_message, [EPC_RETURN_VALUE])

            # CASE 8: successfully return a single value
            a = buffs.String(value='sup')
            return_message = server._serialize_return(buffs.String(), a)
            self.assertEqual(return_message, [EPC_RETURN_VALUE, a.SerializeToString()])

            # CASE 9: successfully return multiple values
            a = buffs.String(value='sup')
            b = buffs.Integer(value=3)
            return_message = server._serialize_return([buffs.String(), buffs.Integer()], [a, b])
            self.assertEqual(return_message, [EPC_RETURN_VALUE, a.SerializeToString(), b.SerializeToString()])

    def test_unserialize_and_parse(self):
        with Server1() as server:
            # CASE 1a: hand it nothing
            with self.assertRaises(UnmarshallingError):
                server._unserialize_and_parse()

            # CASE 1b: hand it a non-iterable request
            with self.assertRaises(UnmarshallingError):
                server._unserialize_and_parse(0)

            # CASE 1c: hand it a request of length < 3
            with self.assertRaises(UnmarshallingError):
                server._unserialize_and_parse(['only has 1 element'])

            # CASE 2: hand it a request without the right method delimiter
            with self.assertRaises(UnmarshallingError):
                server._unserialize_and_parse(['not method delimiter', 'el2', 'el3'])

            # CASE 3: hand it a method_name not in the abstract class
            with self.assertRaises(AttributeError):
                server._unserialize_and_parse([EPC_METHOD_NAME, 'not_a_method', 'el3'])

            # CASE 4: try to get a non-method
            with self.assertRaises(AttributeError):
                server._unserialize_and_parse([EPC_METHOD_NAME, '__dict__', 'el3'])

            # CASE 5: hand it a request without the right argument delimiter
            with self.assertRaises(UnmarshallingError):
                server._unserialize_and_parse([EPC_METHOD_NAME, 'method1', 'not argument delmiter'])

            # CASE 6: hand it a non even number of name-argument pairs
            with self.assertRaises(UnmarshallingError):
                server._unserialize_and_parse([EPC_METHOD_NAME, 'method1', EPC_ARGUMENTS, 'name1'])

            # CASE 7: hand it an arg not declared in the abstract class
            with self.assertRaises(TypeError):
                server._unserialize_and_parse([EPC_METHOD_NAME, 'method1', EPC_ARGUMENTS, 'arg1_name', 'arg1'])

            # CASE 8: have the abstract class take a non-proto-buff arg type
            with self.assertRaises(UnmarshallingError):
                server._unserialize_and_parse([EPC_METHOD_NAME, 'non_proto_arg', EPC_ARGUMENTS, 'a', 'arg1'])

            # CASE 9: try to unmarshall a non-proto-buff type
            with self.assertRaises(UnmarshallingError):
                server._unserialize_and_parse([EPC_METHOD_NAME, 'method2', EPC_ARGUMENTS, 'a', 'arg1'])

            # CASE 10: successfully get a method name with no arguments
            method_name, kwargs = server._unserialize_and_parse([EPC_METHOD_NAME, 'returns1', EPC_ARGUMENTS])
            self.assertEqual(method_name, 'returns1')
            self.assertEqual(kwargs, {})

            # CASE 11: successfully get a method name with arguments
            a = buffs.Integer(value=1)
            method_name, kwargs = server._unserialize_and_parse([EPC_METHOD_NAME, 'method2', EPC_ARGUMENTS, 'a', a.SerializeToString()])
            self.assertEqual(method_name, 'method2')
            self.assertEqual(len(kwargs), 1)
            self.assertEqual(kwargs['a'].value, 1)

    def test_get_return_template(self):
        with Server1() as server:
            bad_method1 = AbstractClass1.bad_method1

            # CASE 1a: hand it something not in the abstract class
            no_such_method = None
            with self.assertRaises(AttributeError):
                server._get_return_template(no_such_method)

            # CASE 1b: hand it a non-method
            with self.assertRaises(AttributeError):
                server._get_return_template("not a method")

            # CASE 2: hand it an exception-raising method
            with self.assertRaises(TypeError):
                server._get_return_template(bad_method1)

    def test_initialize(self):
        with Server1() as server:
            self.assertFalse(server._EggsServer__server_socket.closed)
            self.assertFalse(server._EggsServer__context.closed)
            server._reset()
            self.assertFalse(server._EggsServer__server_socket.closed)
            self.assertFalse(server._EggsServer__context.closed)

    def test_reset(self):
        with Server1() as server:
            self.assertFalse(server._EggsServer__server_socket.closed)
            self.assertFalse(server._EggsServer__context.closed)
            server._reset()
            self.assertFalse(server._EggsServer__server_socket.closed)
            self.assertFalse(server._EggsServer__context.closed)

    def test_term(self):
        with Server1() as server:
            self.assertFalse(server._EggsServer__server_socket.closed)
            self.assertFalse(server._EggsServer__context.closed)
            server._term()
            self.assertTrue(server._EggsServer__server_socket.closed)
            self.assertTrue(server._EggsServer__context.closed)


class EggsServerMetaTests(unittest.TestCase):
    def test_new(self):
        '''
        Tests that the metaclass does all the stuff we need it to do
        when we build our server class.
        '''
        with Server1() as server:
            # check we added EggsServer to the bases
            self.assertTrue(EggsServer in server.__class__.__bases__)

            # check that we have all the methods from the abstract class
            for func in AbstractClass1.__abstractmethods__:
                self.assertTrue((EPC_REMOTE_PREFIX + func) in server.__class__.__dict__)

            # check that we have our custom __init__
            self.assertEqual(getattr(server.__class__, "__init__").__name__, "init_wrapper")


class EggsClientTests(unittest.TestCase):
    '''
    Tests the individual functions in the EggsClient. These tests will have to
    change in sync with change to the inner workings of these functions.
    '''
    def test_init(self):
        with Client1() as client:
            self.assertFalse(client._EggsClient__client_socket.closed)
            self.assertFalse(client._EggsClient__context.closed)
            # our poller should exist so this shouldn't raise an exception
            client._EggsClient__poller
            self.assertEqual(client._EggsClient__request_timeout, 2000)
            self.assertEqual(client._EggsClient__interface.__name__, "AbstractClass1")

    def test_term(self):
        with Client1() as client:
            self.assertFalse(client._EggsClient__client_socket.closed)
            self.assertFalse(client._EggsClient__context.closed)
            client.term()
            self.assertTrue(client._EggsClient__client_socket.closed)
            self.assertTrue(client._EggsClient__context.closed)

    def test_remote_call(self):
        with Client1() as client:
            # CASE 1: hand it a method name that's not in the abstract class
            inner_func = client.remote_call("no_such_method")
            with self.assertRaises(AttributeError):
                inner_func((), {})

            def run_server():
                with Server1() as server:
                    server.run()

            #
            # CASE 2a: make a successful method call with no args
            #

            server_process = multiprocessing.Process(target=run_server)
            try:
                server_process.start()
                time.sleep(0.01)
                pid = server_process.pid

                # invoke an RPC, and try to get a response
                response = client.returns1()
            finally:
                # kill the server
                print "Killing server..."
                os.kill(pid, signal.SIGTERM)
                server_process.join()

            # check the return value
            self.assertEqual(response.value, 'returns1 return value')

            #
            # CASE 2b: make a successful method call with args
            #

            server_process = multiprocessing.Process(target=run_server)
            try:
                server_process.start()
                time.sleep(0.01)
                pid = server_process.pid

                # invoke an RPC, and try to get a response
                response = client.method2(buffs.Integer())
            finally:
                # kill the server
                print "Killing server..."
                os.kill(pid, signal.SIGTERM)
                server_process.join()

            # check the return value
            self.assertEqual(response.value, 'method2 return value')

            #
            # CASE 3: get a remote error
            #

            server_process = multiprocessing.Process(target=run_server)
            try:
                server_process.start()
                time.sleep(0.01)
                pid = server_process.pid

                # invoke an RPC, and try to get a remote exception
                with self.assertRaises(RemoteError):
                    client.bad_method4()
            finally:
                # kill the server
                print "Killing server..."
                os.kill(pid, signal.SIGTERM)
                server_process.join()

            #
            # CASE 3: get a local error
            #

            server_process = multiprocessing.Process(target=run_server)
            try:
                server_process.start()
                time.sleep(0.01)
                pid = server_process.pid

                # invoke an RPC, and try to get a local exception
                try:
                    client.bad_method4(2)
                except:
                    # we want to get here, this means we pass
                    self.assertTrue(True)
                else:
                    # if we get here we fail
                    self.assertTrue(False)
            finally:
                # kill the server
                print "Killing server..."
                os.kill(pid, signal.SIGTERM)
                server_process.join()

    def test_determine_kwargs(self):
        with Client1() as client:
            method1 = AbstractClass1.method1
            takes2 = AbstractClass1.takes2
            bad_method2 = AbstractClass1.bad_method2
            bad_method3 = AbstractClass1.bad_method3

            # CASE 1: no args
            kwargs = client._determine_kwargs(method1, (), {})
            self.assertEqual(kwargs, {})

            # CASE 2: all args, no kwargs
            first = buffs.String()
            second = buffs.Integer()
            kwargs = client._determine_kwargs(takes2, (first, second), {})
            self.assertEqual(kwargs, {'a': first, 'b': second})

            # CASE 3: no args, all kwargs
            first = buffs.String()
            second = buffs.Integer()
            kwargs = client._determine_kwargs(takes2, (), {'a': first, 'b': second})
            self.assertEqual(kwargs, {'a': first, 'b': second})

            # CASE 4: mixed args and kwargs
            first = buffs.String()
            second = buffs.Integer()
            kwargs = client._determine_kwargs(takes2, (first,), {'b': second})
            self.assertEqual(kwargs, {'a': first, 'b': second})

            # CASE 5: too many args
            with self.assertRaises(MarshallingError):
                kwargs = client._determine_kwargs(bad_method2, (), {})
            with self.assertRaises(MarshallingError):
                kwargs = client._determine_kwargs(bad_method3, (), {})
            with self.assertRaises(SyntaxError):
                kwargs = client._determine_kwargs(takes2, (1, 2, 3), {})
            with self.assertRaises(SyntaxError):
                kwargs = client._determine_kwargs(takes2, (1, 2), {'a': 3})
            with self.assertRaises(SyntaxError):
                kwargs = client._determine_kwargs(takes2, (), {'1': 1, '2': 2, '3': 3})

            # CASE 6: wrong kwargs
            with self.assertRaises(TypeError):
                kwargs = client._determine_kwargs(takes2, (), {'a': 1, 'd': 2})

            # CASE 7: wrong types
            with self.assertRaises(TypeError):
                kwargs = client._determine_kwargs(takes2, (), {'a': 1, 'b': 2})
            with self.assertRaises(TypeError):
                kwargs = client._determine_kwargs(takes2, (1), {'b': 2})

            # CASE 8: mixed correct and incorrect types
            with self.assertRaises(TypeError):
                kwargs = client._determine_kwargs(takes2, (buffs.String()), {'b': 2})

    def test_get_return_value_for(self):
        with Client1() as client:
            method1 = AbstractClass1.method1
            returns1 = AbstractClass1.returns1
            returns2 = AbstractClass1.returns2

            # CASE 1: test that it times out properly
            with self.assertRaises(TimeoutError):
                client._get_return_value_for(method1)

            # start a server. make sure you have no other servers
            # running that are listening on localhost:8085
            def run_server():
                with Server1() as server:
                    server.run()

            #
            # CASE 2: test that it gets a single return value
            #

            server_process = multiprocessing.Process(target=run_server)
            try:
                server_process.start()
                time.sleep(0.01)
                pid = server_process.pid

                # invoke an RPC, and try to get a response
                client._EggsClient__client_socket.send_multipart([EPC_METHOD_NAME, 'returns1', EPC_ARGUMENTS])
                response = client._get_return_value_for(returns1)
            finally:
                # kill the server
                print "Killing server..."
                os.kill(pid, signal.SIGTERM)
                server_process.join()

            # check the return value
            self.assertEqual(response.value, 'returns1 return value')

            #
            # CASE 3: test that it gets a nonetype return value
            #

            server_process = multiprocessing.Process(target=run_server)
            try:
                server_process.start()
                time.sleep(0.01)
                pid = server_process.pid

                # invoke an RPC, and try to get a response
                client._EggsClient__client_socket.send_multipart([EPC_METHOD_NAME, 'method1', EPC_ARGUMENTS])
                response = client._get_return_value_for(method1)
            finally:
                # kill the server
                print "Killing server..."
                os.kill(pid, signal.SIGTERM)
                server_process.join()

            # check the return value
            self.assertEqual(response, None)

            #
            # CASE 4: test that it gets a multiple return values
            #

            server_process = multiprocessing.Process(target=run_server)
            try:
                server_process.start()
                time.sleep(0.01)
                pid = server_process.pid

                # invoke an RPC, and try to get a response
                client._EggsClient__client_socket.send_multipart([EPC_METHOD_NAME, 'returns2', EPC_ARGUMENTS])
                response = client._get_return_value_for(returns2)
            finally:
                # kill the server
                print "Killing server..."
                os.kill(pid, signal.SIGTERM)
                server_process.join()

            # check the return value
            self.assertEquals(len(response), 2)
            self.assertEqual(response[0].value, '1')
            self.assertEqual(response[1].value, 2)

            #
            # CASE 5: test that it will raise exceptions during unmarshalling
            #

            server_process = multiprocessing.Process(target=run_server)
            try:
                server_process.start()
                time.sleep(0.01)
                pid = server_process.pid

                # invoke an RPC, and try to get a response
                client._EggsClient__client_socket.send_multipart([EPC_METHOD_NAME, 'returns2', EPC_ARGUMENTS])
                with self.assertRaises(UnmarshallingError):
                    response = client._get_return_value_for(method1)
            finally:
                # kill the server
                print "Killing server..."
                os.kill(pid, signal.SIGTERM)
                server_process.join()

    def test_reset_socket(self):
        with Client1() as client:
            # put the socket into a receive state so it can't send again
            client._EggsClient__client_socket.send_multipart(['blah', 'blah'])

            # this should raise an exception because the socket isn't in a send state
            with self.assertRaises(zmq.ZMQError):
                client._EggsClient__client_socket.send_multipart(['blah', 'blah'])

            # reset the socket and we should be able to send again
            client._reset_socket()
            # this should not raise an exception
            client._EggsClient__client_socket.send_multipart(['blah', 'blah'])

    def test_serialize_method_call(self):
        with Client1() as client:
            # CASE 1: hand it non-dictionary for kwargs
            with self.assertRaises(AttributeError):
                client._serialize_method_call("methodname", None)

            # CASE 2: hand it some non-protobuff arguments
            kwargs = {'arg1': buffs.String(), 'arg2': 'plain string'}
            with self.assertRaises(AttributeError):
                client._serialize_method_call("methodname", kwargs)

            # CASE 3a: make sure it works with no args
            message_list = client._serialize_method_call("methodname", {})
            self.assertEqual(len(message_list), 3)
            self.assertEqual(message_list[0], EPC_METHOD_NAME)
            self.assertEqual(message_list[1], 'methodname')
            self.assertEqual(message_list[2], EPC_ARGUMENTS)

            # CASE 3b: make sure it works with args
            kwargs = {'arg1': buffs.String(value='arg1'), 'arg2': buffs.String(value='arg2')}
            message_list = client._serialize_method_call('methodname', kwargs)
            self.assertEqual(len(message_list), 7)
            self.assertEqual(message_list[0], EPC_METHOD_NAME)
            self.assertEqual(message_list[1], 'methodname')
            self.assertEqual(message_list[2], EPC_ARGUMENTS)
            self.assertEqual(message_list[3], 'arg1')
            unserialized = buffs.String()
            unserialized.ParseFromString(message_list[4])
            self.assertEqual(unserialized.value, 'arg1')
            self.assertEqual(message_list[5], 'arg2')
            unserialized = buffs.String()
            unserialized.ParseFromString(message_list[6])
            self.assertEqual(unserialized.value, 'arg2')

    def test_unserialize_return_value_for(self):
        with Client1() as client:
            method1 = AbstractClass1.method1
            bad_method1 = AbstractClass1.bad_method1
            mismatched1 = AbstractClass1.mismatched1
            mismatched2 = AbstractClass1.mismatched2
            returns1 = AbstractClass1.returns1
            returns2 = AbstractClass1.returns2

            # CASE 1a: hand it a non-method
            with self.assertRaises(TypeError):
                client._unserialize_return_value_for(None, [])

            # CASE 1b: hand it a non-message
            with self.assertRaises(TypeError):
                client._unserialize_return_value_for(method1, None)

            # CASE 2: hand it a method that raises an exception
            with self.assertRaises(TypeError):
                client._unserialize_return_value_for(bad_method1, [])

            # CASE 3: hand it an empty list
            with self.assertRaises(UnmarshallingError):
                client._unserialize_return_value_for(method1, [])

            # CASE 4a: hand it a well-formed remote exception
            with self.assertRaises(RemoteError):
                e = Exception
                client._unserialize_return_value_for(method1, [EPC_EXCEPTION, 'traceback', str(e)])

            # CASE 4b: hand it a malformed remote exception
            with self.assertRaises(RemoteError):
                client._unserialize_return_value_for(method1, [EPC_EXCEPTION])

            # CASE 5: hand it a non-delimiter
            with self.assertRaises(UnmarshallingError):
                client._unserialize_return_value_for(method1, ['blah'])

            # CASE 6: hand it a nonempty return template and empty return value
            with self.assertRaises(UnmarshallingError):
                client._unserialize_return_value_for(mismatched1, [EPC_RETURN_VALUE])

            # CASE 7: hand it a mismatched single template and single value
            with self.assertRaises(UnmarshallingError):
                client._unserialize_return_value_for(mismatched1, [EPC_RETURN_VALUE, 'blah'])

            # CASE 8a: hand it a non-iterable template and an iterable return value
            with self.assertRaises(UnmarshallingError):
                client._unserialize_return_value_for(mismatched1, [EPC_RETURN_VALUE, 'el1' 'el2'])

            # CASE 8b: hand it an iterable template and a non-iterable return value
            with self.assertRaises(UnmarshallingError):
                client._unserialize_return_value_for(mismatched2, [EPC_RETURN_VALUE, 'el1'])

            # CASE 9: hand it a template and value of differnt lengths
            with self.assertRaises(UnmarshallingError):
                client._unserialize_return_value_for(returns2, [EPC_RETURN_VALUE, 'el2', 'el2', 'el3'])

            # CASE 10: hand it iterable and same length template and value, but mismatched types
            with self.assertRaises(UnmarshallingError):
                client._unserialize_return_value_for(mismatched2, [EPC_RETURN_VALUE, 'el2', 'el2'])

            # CASE 11: successful empty return value
            output = client._unserialize_return_value_for(method1, [EPC_RETURN_VALUE])
            self.assertEqual(output, [])

            # CASE 12: successful single return value
            unserialized = buffs.String()
            serialized = unserialized.SerializeToString()
            output = client._unserialize_return_value_for(returns1, [EPC_RETURN_VALUE, serialized])
            self.assertEqual(len(output), 1)
            self.assertEqual(type(output[0]), type(unserialized))
            self.assertEqual(output[0].value, unserialized.value)

            # CASE 13: successful multiple return value
            unserialized1 = buffs.String()
            unserialized2 = buffs.Integer()
            serialized1 = unserialized1.SerializeToString()
            serialized2 = unserialized2.SerializeToString()
            output = client._unserialize_return_value_for(returns2, [EPC_RETURN_VALUE, serialized1, serialized2])
            self.assertEqual(len(output), 2)
            self.assertEqual(type(output[0]), type(unserialized1))
            self.assertEqual(type(output[1]), type(unserialized2))
            self.assertEqual(output[0].value, unserialized1.value)
            self.assertEqual(output[1].value, unserialized2.value)


class EggsClientMetaTests(unittest.TestCase):
    def test_new(self):
        '''
        Tests that the metaclass does all the stuff we need it to do
        when we build our client class.
        '''
        with Client1() as client:
            # check we added EggsClient to the bases
            self.assertTrue(EggsClient in client.__class__.__bases__)

            # check that we have all the methods from the abstract class
            for func in AbstractClass1.__abstractmethods__:
                self.assertTrue(func in client.__class__.__dict__)
                self.assertTrue(getattr(client, func).__name__ == "send_args_and_get_response")

            # check that we have our custom __init__
            self.assertTrue("__init__" in client.__class__.__dict__)


if __name__ == "__main__":
    unittest.main()
