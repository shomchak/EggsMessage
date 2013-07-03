'''
@author Shomik Chakravarty (shomik.chakravarty@gmail.com)

EGGS PROCEDURE CALL

Provides methods and decorators to be used by a service to provide client functionality
using zeromq for transport and protocol buffers for marshalling. This class has to be
used in conjuction with an abstract base class that defines the interface for your
client-server model.

EggsClient can be used in two ways: direct subclassing (not preferred) or assigning
EggsClientMeta to the __metaclass__ attribute of your class (preferred). See
eggsample.py for a full walkthrough of using this framework.
'''


import abc
import zmq
import inspect
import traceback
from epc_constants import *


class EggsClient(object):

    def __init__(self, server_endpoint="tcp://localhost:8085", request_timeout=2000, io_threads=1):
        '''
        NOTE: If you choose to subclass EggsClient, be sure to call this explicitly
        in your subclass, and pass in your subclass instance as self.

        Don't do it this way. Use the metaclass method.
        '''

        # set up our instance vars
        self.__server_endpoint = server_endpoint
        self.__context = zmq.Context(io_threads)
        self.__client_socket = self.__context.socket(zmq.REQ)
        self.__client_socket.connect(self.__server_endpoint)
        self.__client_socket.setsockopt(zmq.LINGER, 0)

        self.__poller = zmq.Poller()
        self.__poller.register(self.__client_socket, zmq.POLLIN)
        self.__request_timeout = request_timeout

        # this checks that our subclass inherits first from an abstract base class,
        # and binds that class to self.__interface
        self.__interface = type(self).__bases__[0]
        if not inspect.isabstract(self.__interface):
            raise TypeError("Server object must inherit first from an abstract base class.")

    # ===================
    # CONVENIENCE METHODS
    # ===================

    def eggs_ping(self):
        '''
        Use this to see if the server is alive! You should get an "eggs_pong" back.
        '''
        try:
            self.__client_socket.send_multipart(['eggs_ping'])
        except zmq.ZMQError:
            self._reset_socket()

        # poll our socket for a receive event for <self.__request_timeout> ms
        changed_sockets = dict(self.__poller.poll(self.__request_timeout))
        if changed_sockets.get(self.__client_socket) == zmq.POLLIN:
            reply = self.__client_socket.recv_multipart()[0]

        # if we don't receive anything, we have to close and reset our socket
        else:
            self._reset_socket()
            raise TimeoutError("Request timed out after {0} ms.".format(self.__request_timeout))
        return reply

    @staticmethod
    def remote_call(method_name):
        '''
        NOTE: This is referred to as a "decorator", but it really isn't one; instead
        of taking in a method it just takes in a method name. Otherwise it acts like
        a decorator acting on the abstract method of the name passed in.

        NOTE AGAIN: If you are using the metaclass approach, you won't have to worry
        about using this decorator (it's done for you). If not, you will have
        to use this to decorate each method that is actuating a remote method
        on the server (that is also defined in your abstract base class). Because
        your class is just a client, the body and type signature of your method
        (pre-decoration) makes no difference; only the method name is used to match
        against the abstract base class.

        This decorator will wrap your method in lovely RPC goodness. It will take
        the arguments you supply to the method, type check them against the abstract
        class, attempt to serialize them, send them to the remote server, wait for
        a response, attempt to unserialize the response, and return the value just
        like a local function call. If the server implementation raises an exception,
        the exception will be piped back to the client and this will raise a
        RemoteError, hopefully with some useful debugging information.
        '''

        def send_args_and_get_response(self, *args, **kwargs):
            # check that the name of the decorated method is actually a method
            # declared in the abstract class
            abstract_unbound_method = getattr(self.__interface, method_name)
            if not inspect.ismethod(abstract_unbound_method):
                raise AttributeError("{0}() is not declared in {1}".format(method_name, self.__interface.__name__))

            kwargs = self._determine_kwargs(abstract_unbound_method, args, kwargs)

            # marshall our arguments and construct the multi-part message.
            # don't catch exceptions here; they won't affect the socket, and
            # we need the user to know if they screwed up the arguments
            message_list = self._serialize_method_call(method_name, kwargs)

            try:
                # send that poop
                self.__client_socket.send_multipart(message_list, copy=False)

                # wait for a response and try to unmarshall it
                response = self._get_return_value_for(abstract_unbound_method)
            except Exception as e:
                print("Encountered an error. Resetting...")
                print("LOCAL TRACEBACK:")
                self._reset_socket()
                raise e
            else:
                # return the response like a good method
                return response

        # return the wrapper function like a good decorator
        return send_args_and_get_response

    def term(self):
        '''
        Performs some zeromq cleanup you need when you're done with your client.
        '''
        self.__client_socket.close()
        self.__context.term()

    # ===============
    # PRIVATE METHODS
    # ===============

    def __enter__(self):
        return self

    def __exit__(self, type, value, traceback):
        self.term()

    def _determine_kwargs(self, abstract_unbound_method, args, kwargs):
        '''
        Takes an abstract_unbound_method and the arguments passed to a method
        called by the client, converts all the arguments into keyword args, typechecks
        the args, and returns the kwargs dictionary.
        '''
        # get a dictionary of expected argument types from the abstract method definition
        abst_arg_spec = inspect.getargspec(abstract_unbound_method)
        arg_names = abst_arg_spec.args
        arg_defaults = tuple() if abst_arg_spec.defaults is None else abst_arg_spec.defaults
        if not arg_names:
            raise MarshallingError("Abstract method {0}() takes no args, it must have a reference to self.".format(abstract_unbound_method.__name__))
        if len(arg_names) != (len(arg_defaults) + 1):
            raise MarshallingError("Abstract method {0}() has some arguments without default values (not including 'self').".format(abstract_unbound_method.__name__))
        expected_types = dict(zip(arg_names[-len(arg_defaults):], [type(arg) for arg in arg_defaults]))  # wat

        # make sure the number of received arguments doesn't exceed the number of declared args
        num_recvd_args = len(args) + len(kwargs)
        if num_recvd_args > len(arg_names[1:]):
            raise SyntaxError("{0}() expected {2} arguments ({1} given)".format(abstract_unbound_method.__name__, num_recvd_args, len(arg_names[1:])))

        # turn all positional arguments into keyword arguments
        for i, arg in enumerate(args):
            kwargs.setdefault(arg_names[1:][i], arg)

        # type check our received arguments with the expected_types dictionary
        for name, value in kwargs.iteritems():
            if expected_types.get(name) is None:
                raise TypeError("Argument {0} not declared in abstract method {1}()".format(name, abstract_unbound_method.__name__))
            if not isinstance(value, expected_types.get(name)):
                raise TypeError("{0} is not of type {1}".format(value, expected_types[name]))

        return kwargs

    def _get_return_value_for(self, abstract_unbound_method):
        '''
        Tries to receive a multipart message and attempts to parse it as
        a return value for the given abstract method. Returns the return
        value. That's not a redundant statement in this context. Get off
        my back!
        '''

        response = []

        # poll our socket for a receive event for <self.__request_timeout> ms
        changed_sockets = dict(self.__poller.poll(self.__request_timeout))
        if changed_sockets.get(self.__client_socket) == zmq.POLLIN:
            reply = self.__client_socket.recv_multipart()

            # attempt to unmarshall the reply. don't catch exceptions it raises,
            # this is hard mode
            response = self._unserialize_return_value_for(abstract_unbound_method, reply)

        # if we don't receive anything, we have to close and reset our socket
        else:
            self._reset_socket()
            raise TimeoutError("Request timed out after {0} ms.".format(self.__request_timeout))

        if response:
            if len(response) == 1:
                return response[0]
            return tuple(response)
        return None

    def _reset_socket(self):
        '''
        We need this if we try to stop and start our server several times in
        one session. This deletes previous contexts and sockets, and
        rebinds them with only the freshest sockets. And it's all natural!
        '''

        self.__client_socket.close()
        self.__poller.unregister(self.__client_socket)
        try:
            # try to clean up our old socket
            del self.__client_socket
        except (NameError, AttributeError):
            # ain't no thang
            pass

        self.__client_socket = self.__context.socket(zmq.REQ)
        self.__client_socket.connect(self.__server_endpoint)
        self.__client_socket.setsockopt(zmq.LINGER, 0)
        self.__poller.register(self.__client_socket)

    def _serialize_method_call(self, method_name, kwargs):
        '''
        Takes a method name and a dictionary of python objects and returns a list
        of serialized data. Raises MarshallingError if an object cannot be serialized.
        The the serialized data looks like the following (it is a list):

        ["EPC_METHOD_NAME:", <method_name>, "EPC_ARGUMENTS:", <arg1_name>, <serialized_arg1>, ...]
        '''

        message_list = list()
        message_list.append(EPC_METHOD_NAME)
        message_list.append(str(method_name))
        message_list.append(EPC_ARGUMENTS)

        for name, value in kwargs.iteritems():
            # don't catch exceptions, I said this was hard mode
            message_list.extend((str(name), value.SerializeToString()))

        return message_list

    def _unserialize_return_value_for(self, abstract_unbound_method, message):
        '''
        Takes an abstract method, and a zmq message list, and tries to parse the
        message as a return value for that method.
        '''

        try:
            # bind the unbound method
            abstract_bound_method = abstract_unbound_method.__get__(self, type(self))
        except Exception:
            raise TypeError("Couldn't bind abstract unbound method {0}".format(abstract_unbound_method))

        try:
            # try to call it to get a return template
            return_template = abstract_bound_method()
        except Exception:
            traceback.print_exc()
            raise TypeError("Error in abstract method definition {0}()".format(abstract_unbound_method.__name__))

        #
        # begin constructing a list of return values
        #

        return_values = []

        # no return value should still have a delimiter
        if len(message) == 0:
            raise UnmarshallingError("Got an empty response. Gafug?")

        # if the remote end send back an exception, log it and raise it
        if str(message[0]) == EPC_EXCEPTION:
            try:
                tb = str(message[1])
                e = str(message[2])
            except Exception:
                print("Got a malformed remote exception.")
                raise RemoteError("Got an unkown remote exception (traceback and exception type not provided)")
            else:
                print("Got a remote exception.\nREMOTE TRACEBACK:")
                print tb
                raise RemoteError(e)

        if str(message.pop(0)) != EPC_RETURN_VALUE:
            raise UnmarshallingError("Malformed response.")

        # if we get here we know the delimiter is <EPC_RETURN_VALUE>, and it has been popped off our list
        if len(message) == 0:
            if return_template is not None:
                raise UnmarshallingError("Mismatched return values. Is {0} up to date?".format(self.__interface.__name__))
        elif len(message) == 1:
            try:
                pb_object = type(return_template)()
                pb_object.ParseFromString(message[0])
            except Exception:
                traceback.print_exc()
                raise UnmarshallingError
            return_values.append(pb_object)
        else:
            try:
                # if we get here we know there are multiple return values, so the template must be iterable
                iter(return_template)
            except TypeError:
                raise UnmarshallingError("Mismatched return values. Is {0} up to date?".format(self.__interface.__name__))
            else:
                if len(message) != len(return_template):
                    raise UnmarshallingError("Mismatched return values. Is {0} up to date?".format(self.__interface.__name__))
                else:
                    for value, v_type in zip(message, [type(i) for i in return_template]):
                        try:
                            pb_object = v_type()
                            pb_object.ParseFromString(str(value))
                        except Exception:
                            traceback.print_exc()
                            raise UnmarshallingError
                        return_values.append(pb_object)

        return return_values


class EggsClientMeta(abc.ABCMeta):
    '''
    Hijacks the creation of your client class and does all the scary stuff
    so you don't have to. (Actually I just don't trust you.)

    Set the __metaclass__ attribute of your class equal to this class, and
    you'll be all set. You don't have to do anything else. Your client class
    definition should look like this:

    class ShinyClient(AbstractShinyInterface):
        __metaclass__ = EggsClientMeta

    That's it.
    '''

    def __new__(cls, cls_name, bases, dct):
        '''Get all the abstract methods declared in our abstract base class,
        RPC-ify them, and bind them to the class we are creating.'''

        # we add EggsClient as one of our parent classes, so we can get all
        # the cool functions it provides for free.
        bases += (EggsClient,)

        # check that the first parent class is abstract. yes, at the moment
        # this is only reliable if your class only inherits from one abstract
        # class. Because of that I have the following todo:
        # TODO(shomik): make it so that the abstract class doesn't have to be
        #               the first parent class, but rather it is found dynamically
        interface = bases[0]
        if not inspect.isabstract(interface):
            raise TypeError("Client class must inherit first from its abstract interface.")

        # for each abstract method, create an RPC-ified version of it and
        # add it to our class dict
        for method_name in interface.__abstractmethods__:
            dct[method_name] = EggsClient.remote_call(method_name)

        # define an __init__ method for our class that will take
        def init(self, server_endpoint="tcp://127.0.0.1:8085"):
            EggsClient.__init__(self, server_endpoint=server_endpoint)

        # rebind our wrapped __init__ to "__init__"
        dct["__init__"] = init

        # let our super class finish creating the class
        return super(EggsClientMeta, cls).__new__(cls, cls_name, bases, dct)
