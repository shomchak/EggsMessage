'''
@author Shomik Chakravarty (shomik.chakravarty@gmail.com)

EGGS PROCEDURE CALL

Provides methods and decorators to be used by a service to provide server functionality
using zeromq for transport and protocol buffers for marshalling. This class has to be
used in conjuction with an abstract base class that defines the interface for your
client-server model.

EggsServer can be used in two ways: direct subclassing (not preferred) or assigning
EggsServerMeta to the __metaclass__ attribute of your class (preferred). See
eggsample.py for a full walkthrough of using this framework.
'''


import abc
import zmq
import inspect
import traceback
import threading
from epc_constants import *


class EggsServer(object):

    def __init__(self, server_endpoint="tcp://127.0.0.1:8085", hwm=0, io_threads=1):
        '''
        NOTE: If you choose to subclass EggsServer, be sure to call this explicitly
        in your subclass, and pass in your subclass instance as self.

        Don't do it this way. Use the metaclass method.
        '''

        # set up our instance vars
        self.__server_endpoint = server_endpoint
        self.__hwm = hwm
        self.__io_threads = io_threads
        self.eggs_lock = threading.Lock()

        # initialize the context and socket
        self._initialize()

        # this checks that our subclass inherits first from an abstract base class,
        # and binds that class to self.__interface
        self.__interface = type(self).__bases__[0]
        if not inspect.isabstract(self.__interface):
            raise TypeError("Server object must inherit first from an abstract base class.")

    # ===================
    # CONVENIENCE METHODS
    # ===================

    def eggs_reply(self, message):
        '''
        NOTE: You should never have to explicitly call this, though it is provided
        for you if you think you are sufficiently special.

        Takes a list of serialized data [strings OK] and sends it back to the
        client as a multipart message. If the elements of your list aren't serialized,
        it will fail. It only works when our socket is in a send state
        (i.e. the last i/o it performed was a receive). If it fails it will reset
        the socket so it can continue receiving requests and so you don't look like
        an asshole.
        '''

        try:
            self.__server_socket.send_multipart(message)
        except Exception:
            print("{0} got an exception sending message {0}.".format(type(self).__name__), message)
            traceback.print_exc()
            print("resetting...")
            self._reset()
        else:
            print("Replying with '{0}'".format(message))

    @staticmethod
    def remote_reply(method):
        '''
        NOTE: If you are using the metaclass approach, you won't have to worry
        about using this decorator (it's done for you). If not, you will have
        to use this to decorate each method that is implementing one of the
        interface methods defined in your abstract base class.

        This decorator will wrap your method in lovely RPC goodness. It will
        try to call your method with the arguments requested by the client,
        attempt to marshall the return value, and send it (or an exception)
        back to the client.
        '''

        # This is the wrapper function that calls your function and does RPC
        # stuff to it. This function is returned by the decorator.
        def call_method_with_rpc(self, *args, **kwargs):
            serialized = []
            return_value = None
            try:
                # get the return signature provded by the abstract method
                return_template = self._get_return_template(method)
            except Exception as e:
                # if it errors, pipe the exception back to the client
                tb = traceback.format_exc()
                print(tb)
                serialized = [EPC_EXCEPTION, tb, str(e)]
            else:
                try:
                    # try to call the user-defined method in our server
                    return_value = method(self, *args, **kwargs)
                except Exception as e:
                    # if it errors, pipe the exception back to the client
                    tb = traceback.format_exc()
                    print(tb)
                    serialized.extend([EPC_EXCEPTION, tb, str(e)])
                else:
                    try:
                        # try to serialize the return value
                        serialized = self._serialize_return(return_template, return_value)
                    except Exception as e:
                        # if it errors, pipe the exception back to the client
                        tb = traceback.format_exc()
                        print tb
                        serialized = [EPC_EXCEPTION, tb, str(e)]

            # actually send the reply
            self.eggs_reply(serialized)

            # return the return value of the original method like a good function
            return return_value

        # return the wrapper function like a good decorator
        return call_method_with_rpc

    def run_server(self, stop_event=threading.Event()):
        '''
        Runs the server. Blocks until CTRL+C. Cleans up after CTRL+C.

        This will respond to incoming requests. It will absorb any exceptions
        that are raised while handling a request, log them, and pipe them back
        to the requester.
        '''

        # # initialize the context and socket
        # self._initialize()

        print("Running...\nCTRL+C to end it all...")
        try:
            while not stop_event.is_set():
                stop_event.wait(0.001)
                try:
                    # try to get a request, but don't block for it
                    request = self.__server_socket.recv_multipart(flags=zmq.NOBLOCK, copy=False)
                    # this is the lock we use to manage our shared resources
                    with self.eggs_lock:
                        self._handle_request(request)
                except zmq.Again:
                    # This is if there is no message to receive
                    pass
                except zmq.ZMQError:
                    # if we error, reset the context and socket and get the
                    # hell off the ground, we got requests to handle
                    traceback.print_exc()
                    print("Resetting...")
                    self._reset()
        except KeyboardInterrupt:
            pass
        except Exception:
            traceback.print_exc()
        finally:
            self._term()
            print("\nBye!")

    # ===============
    # PRIVATE METHODS
    # ===============

    def __enter__(self):
        try:
            self._initialize()
        finally:
            return self

    def __exit__(self, type, value, traceback):
        self._term()
        print("\nBye!")

    def _initialize(self):
        '''
        We need this if we try to stop and start our server several times in
        one session. This replaces our socket and client them with only the
        freshest contexts and sockets. And it's all natural!
        '''

        # reinitialize our favorite context and socket
        self.__context = zmq.Context(self.__io_threads)
        # it's a reply socket
        self.__server_socket = self.__context.socket(zmq.REP)
        self.__server_socket.set_hwm(self.__hwm)
        # tell it where to look
        self.__server_socket.bind(self.__server_endpoint)
        # it won't wait for pending messages when it is told to close
        self.__server_socket.setsockopt(zmq.LINGER, 0)

    def _handle_request(self, request):
        '''
        Takes a request list, tries to parse it into a method name and
        key word arguments, unserializes the arguments, and calls the
        method with the arguments.
        '''

        print("Got a request.")

        if self._handle_eggs_ping(request):
            return

        try:
            # try to parse the request into a method name and kwargs
            method_name, kwargs = self._unserialize_and_parse(request)
        except Exception as e:
            # if it errors, log the errors, reset, and return to
            # handling requests
            traceback.print_exc()
            print(e)
            print("Resetting...")
            self._reset()
            return

        # check if our request matches the name of a method declared in our interface.
        # we can't use inspect.ismethod() here because that only checks for bound methods;
        # at this point our method is still unbound to our instance
        if inspect.isfunction(self.__interface.__dict__.get(method_name)):
            print("It's one of the methods in our interface. Calling it!")

            # retrieve and bind the method to our instance
            bound_method = getattr(self, EPC_REMOTE_PREFIX + method_name)
            # call that poop. keep in mind this is the decorated version of the method,
            # not the raw version definied in your server class. this means the control
            # flow continues at the "call_method_with_rpc()" method defined above.
            # Also, that method handles its own exceptions, so we don't have to here.
            print("Calling {0}() with args {1}".format(bound_method.__name__, kwargs))
            bound_method(**kwargs)
        else:
            # if it doesn't match with any of the methods defined in our interface,
            # return an exception
            error_message = "Message '{0}' is not defined in our interface.".format(method_name)
            print(error_message)
            message_list = [EPC_EXCEPTION, traceback.format_exc(), TypeError(error_message)]
            self.eggs_reply(message_list)

    def _handle_eggs_ping(self, request):
        '''
        Tests if a request is just a eggs_ping, and if it is returns True.
        '''
        if len(request) == 1:
            if str(request[0]) == 'eggs_ping':
                print("It's a eggs_ping. Replying with eggs_pong.")
                try:
                    self.__server_socket.send_multipart(['eggs_pong'])
                except zmq.ZMQError:
                    self._reset()
                return True
            return False
        return False

    def _serialize_return(self, template, return_value):
        '''
        Takes a return template (which is the return value of the abstract method)
        and the return value of the implemented method, and tries to use the
        template to serialize the return value. Returns a sendable message.
        '''

        #
        # figure out if both the template and return value have the same
        # iterableness, and if so, what is it (iterable or not iterable)
        #

        template_iterable = False
        rv_iterable = False

        try:
            iter(template)
        except TypeError:
            template_iterable = False
        else:
            template_iterable = True

        try:
            iter(return_value)
        except TypeError:
            rv_iterable = False
        else:
            rv_iterable = True

        # if the have different iterablenesses, raise an exception
        if template_iterable != rv_iterable:
            raise TypeError("Return value and abstract return value don't have the same iterableness.")

        #
        # if we got here, they have the same iterableness. proceed to
        # construct the return message.
        #

        # start it off with the return value delimiter
        return_message = [EPC_RETURN_VALUE]

        # if they are both None, then we are done
        if template is None and return_value is None:
            pass
        # if they are both iterable, try to match up the elements
        elif rv_iterable:
            # if they have different lengths, end it all
            if len(template) != len(return_value):
                raise MarshallingError("Return value doesn't have the same length as the abstract return value.")
            # check the types of the return value elements with those of the template elements
            for value, v_type in zip(return_value, [type(i) for i in template]):
                if not isinstance(value, v_type):
                    raise TypeError("Return value '{0}' not of type {1}".format(value, v_type))
                try:
                    return_message.append(value.SerializeToString())
                except Exception:
                    traceback.print_exc()
                    raise TypeError("Error serializing '{0}' to string.".format(value))
        # if we get here we know they're both not iterable, so they must be single objects
        elif isinstance(return_value, type(template)):
            try:
                return_message.append(return_value.SerializeToString())
            except Exception:
                traceback.print_exc()
                raise TypeError("Error serializing '{0}' to string.".format(return_value))
        else:
            raise TypeError("Return value '{0}' is not of type of it's corresponding abstract return value.".format(return_value))

        return return_message

    def _unserialize_and_parse(self, request=[]):
        '''
        Takes a list of serialzed data, tries to unserialize it, and tries to
        parse the data into a method name and keyword arguments. Returns a
        tuple (method_name, {kwargs}).
        '''

        #
        # try to get a method name
        #

        # the shortest sensical request has length 3: method name delimiter, method
        # name, and argument delimiter (the argument delimiter can be followed by
        # nothing)
        try:
            iter(request)
        except:
            raise UnmarshallingError("Request is not iterable.")
        if len(request) < 3:
            raise UnmarshallingError("Request is less than 3 frames long. Discarding request.")

        if str(request.pop(0)) != EPC_METHOD_NAME:
            raise UnmarshallingError("First frame is not '{0}'. Discarding request".format(EPC_METHOD_NAME))

        # if we get here we know this next element should be parsed as an argument name
        method_name = str(request.pop(0))

        #
        # try to get a dictionary of expected argument types from the abstract class
        #

        # try to get the method from the abstract class whose name is the method_name
        # we just parsed
        abstract_unbound_method = getattr(self.__interface, method_name)
        if not inspect.ismethod(abstract_unbound_method):
            raise AttributeError("{0}() is not declared in {1}".format(method_name, self.__interface.__name__))

        # ok, so we got the abstract method. use inspect's infinite power to get all
        # the argument names and types from that method in a dictionary called expected_types.
        abst_arg_spec = inspect.getargspec(abstract_unbound_method)
        arg_names = abst_arg_spec.args
        arg_defaults = tuple() if abst_arg_spec.defaults is None else abst_arg_spec.defaults
        expected_types = dict(zip(arg_names[-len(arg_defaults):], [type(arg) for arg in arg_defaults]))  # wat

        #
        # try to parse the arguments according to our expected arguments dictionary
        #

        if str(request.pop(0)) != EPC_ARGUMENTS:
            raise UnmarshallingError("Third frame is not '{0}'. Discarding request".format(EPC_ARGUMENTS))

        # make sure there are an even number of elements left, because the arguments
        # need to come in pairs of names and values
        if len(request) % 2:
            raise UnmarshallingError("Mismatched keyword args. Discarding request.")

        kwargs = {}
        # that expression with iter() is a neat way to divide a 1D list into groups of n
        for name, value in zip(*([iter(request)] * 2)):
            name = str(name)
            if expected_types.get(name) is None:
                raise TypeError("Argument {0} not declared in abstract method {1}()".format(name, method_name))

            try:
                pb_object = expected_types[name]()
                pb_object.ParseFromString(str(value))
            except Exception:
                traceback.print_exc()
                raise UnmarshallingError("Error unmarshalling argument {0} to proto buff.".format(name))

            kwargs[name] = pb_object

        return method_name, kwargs

    def _get_return_template(self, method):
        '''
        Takes an implemented method, looks up its abstract counterpart, and
        calls it to get the return value, which should be a signature of
        the return value of the implemented method. Returns the abstract
        return value.
        '''

        # this is what happens when you call a method from an instance, we
        # just have to write all this explicitly here because we're trying
        # to get the overriden abstract method. python let's you do anything
        # if you're willing to add enough underscores.
        abst_method = getattr(self.__interface, method.__name__).__get__(self, type(self))
        try:
            # the abstract method should have default values for all its
            # arguments, so we should be fine calling it with no args
            return_value = abst_method()
        except Exception:
            traceback.print_exc()
            raise TypeError("Error in abstract method definition {0}()".format(method.__name__))
        return return_value

    def _term(self):
        '''
        Performs the necessary zeromq cleanup for when we're done running
        the server.
        '''
        self.__server_socket.close()
        self.__context.term()

    def _reset(self):
        '''
        Calls _term() and _initialize(). Don't believe me? Look:
        '''
        self._term()
        self._initialize()


class EggsServerMeta(abc.ABCMeta):
    '''
    Hijacks the creation of your server class and does all the scary stuff
    so you don't have to. (Actually I just don't trust you.)

    Set the __metaclass__ attribute of your class equal to this class, and
    you'll be all set. All you'll have to worry about is implementing the
    methods you defined in your abstract class, with the same type signatures.
    '''

    def __new__(cls, cls_name, bases, dct):
        # we add EggsServer as one of our parent classes, so we can get all
        # the cool functions it provides for free.
        bases += (EggsServer,)

        # check that the first parent class is abstract. yes, at the moment
        # this is only reliable if your class only inherits from one abstract
        # class. Because of that I have the following todo:
        # TODO(shomik): make it so that the abstract class doesn't have to be
        #               the first parent class, but rather it is found dynamically
        interface = bases[0]
        if not inspect.isabstract(interface):
            raise TypeError("Client class must inherit first from its abstract interface.")

        # get a list of all the methods declared in your class that are also
        # declared in the abstract class
        impl_methods = [method for (name, method) in dct.iteritems() if (inspect.isfunction(method) and
                                                                         name in interface.__abstractmethods__)]

        # here we are going to wrap the __init__ provided to us with a call to
        # EggsServer.__init__, because we just added that parent above. this
        # wrapper also provides an argument to __init__ that specifies the server
        # endpoint, namely "server_endpoint". this might have more underscores
        # added to it in the future to avoid namespace collisions with the
        # __init__ provided our subclass.
        subclass_init = dct.get("__init__")

        def init_wrapper(self, server_endpoint="tcp://127.0.0.1:8085", *args, **kwargs):
            return_value = None
            if subclass_init:
                return_value = subclass_init(self, *args, **kwargs)
            EggsServer.__init__(self, server_endpoint=server_endpoint)
            return return_value

        dct["__init__"] = init_wrapper

        # Here we are implementing our run method, the method you call to start
        # the server. If the subclass has provided it's own main() loop, run that
        # in the main thread while running EggsServer.run_server() in another
        # thread. If they provide no main thread, just call run_server() in the
        # main thread.
        subclass_main = dct.get("main")

        def run(self, *args, **kwargs):
            return_value = None
            if subclass_main:
                stop_event = threading.Event()
                run_server_thread = threading.Thread(target=self.run_server, args=(stop_event,))
                run_server_thread.daemon = True
                run_server_thread.start()
                try:
                    return_value = subclass_main(self, *args, **kwargs)
                except KeyboardInterrupt:
                    pass
                finally:
                    stop_event.set()
                    print "\nStopping server..."
                    run_server_thread.join()
                    print "Server stopped."
                    return return_value
            else:
                self.run_server()

        dct["run"] = run

        # for each implemented method, create an RPC-ified version of it and
        # add it to our class dict with some extra stuff added to the front
        # of the name, so your original method still exists intact with its
        # birthname. Remember that decorater EggsServer.remote_reply,
        # and how I said you wouldn't have to worry about using it if you used
        # the metaclass approach? That's because we're calling it here:
        for method in impl_methods:
            dct[EPC_REMOTE_PREFIX + method.__name__] = EggsServer.remote_reply(method)

        # let our super class finish creating the class. fun fact, the name
        # for the built-in function super() has a double meaning: the second
        # meaning is that calls the super class
        return super(EggsServerMeta, cls).__new__(cls, cls_name, bases, dct)
