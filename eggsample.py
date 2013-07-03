'''
@author Shomik Chakravarty (shomik.chakravarty@gmail.com)

An example of how to use the epc_utils to create a client-server model.
'''


import abc  # abstract base class, part of the standard library
from epc_server import EggsServerMeta
from epc_client import EggsClientMeta
import my_proto_definitions as buffs  # this should be your own proto defs


'''
The whole point of all this is so the client can call methods on a remote
server (RPC), so we need to somehow declare the methods we would like
our server to offer. This was previously done in the slice defs.

To do it here all pythonic like, we are going to use an abstract base class
to declare the names of our methods, and the types they should expect and
return.

NOTE: all data provided to an RPC method must be in the form of proto-buffs,
because that is how we are serializing our objects. More on this later.

The advantage of using an abstract base class rather than a regular class
to define our interface is that an ABC will guarantee that any subclass
of it will have implemented all of the abstract methods (otherwise when
you try to instantiate an instance of the subclass, it will fail). Also,
an abstract class allows us more interesting reflection, but you shouldn't
have to worry about that in your implementation.

Here's how we define our interface:
'''


class MyAbstractInterface(object):

    # to make it an abstract class, you have to set the metaclass like this
    __metaclass__ = abc.ABCMeta

    # to mark a method as abstract, just use this decorator
    @abc.abstractmethod
    def my_method(self, first_arg=buffs.Satellite(), second_arg=buffs.Bool()):
        '''
        The abstract method declaration MUST have default values for all
        the arguments, and those defaults MUST be proto-buff objects. This
        is how your server and client know what types the args need to be.
        This DOES NOT mean your server implementation has to have default
        values for the args (those can be defined just like regular python).
        At the moment, our abstract method defs need to have a reference to
        self or cls as the first arg (staticmethods are not yet supported).

        The abstract method has no body. Except comments on what the server
        implementation should do when the method is called.

        The return value is also the type of a proto-buff, or a list of
        proto buffs, or None.

        The following return values are valid:

        return buffs.Satellite()  # ok
        return buffs.Satellite(), buffs.Antenna()  # ok
        return None  # ok

        Invalid:
        return True  # NOT ok, not a protobuff type
        return lambda x: 'blah'  # NOT ok, not a protobuff type

        Here we are going to return a satellite and an antenna
        '''
        return buffs.Satellite(), buffs.Antenna()

'''
Now, to implement our server, we just need to inherit from the abstract interface
and make sure our arg names are identical to that of the interface.

Keep in mind, abstract base class only guarantees that for every abstract method
in the abstract class, there is exactly one method in the subclass of the same
name; it doesn't care about arguments or return values. It is your job to make sure
these are correct. But don't worry, it's not like it would work if it's incorrect;
you will probably get a very helpful error message on what is wrong.

So here we go:
'''


class MyServerImplementation(MyAbstractInterface):

    # to make it a server, all you have to do is assign this metaclass
    __metaclass__ = EggsServerMeta

    # now implement your abstract methods
    def my_method(self, first_arg, second_arg=buffs.Bool(value=True)):
        # This is just regular python. You can use default arg values and errthang.
        # The cool thing is, this method has no RPC stuff built into it. It does
        # exactly what it says it does, so you can call it for unit testing with
        # no worries about setting up ZMQ. The EggsServerMeta takes care of all the
        # RPC stuff in a special and secret way.

        my_sat = buffs.Satellite()
        my_sat.hw_id = "duck1"

        my_ant = buffs.Antenna()

        return my_sat, my_ant

    # This class is just a regular old class. you can have whatever methods you want
    # even if they're not declared on the abstract class. The abstract class just tells
    # the EggsServerMeta which methods need to be RPC-ified.

    def my_own_auxilliary_method_not_in_the_abstract_class(self, blah, blah2):
        stuff = 'stuff'
        return stuff

    @staticmethod
    def my_static_because_static_is_cool():
        return "eggs"


    # You can also have a main loop that will run alongside the server handling
    # requests. All you have to do is to call it "main".

'''
For the client, there really isn't anything to it:
'''


class MyClientImplementation(MyAbstractInterface):
    __metaclass__ = EggsClientMeta

'''
That's it, bru.


SAMPLE INTERPRETER SESSION FOR THE SERVER

>>> server = MyServerImplementation()
    # By default the server is built on address tcp://127.0.0.1:8085, which is
    # the loopback IP address. Say you want to deploy this on another address?
    # Just pass in an argument called "server_endpoint" to the constructor
    # and that'll do the trick:
>>> server = MyServerImplementation(server_endpoint="tcp://192.168.172.148:10000")
    # To make it go, just call the run method!
>>> server.run()
Running...
CTRL+C to end it all...

    # What if you have a custom main loop you want to run? Well, as long as you
    # called it main in your class, it will be run automatically, with whatever
    # arguments you pass into run()
>>> server.run(my_args_that_I_want_to_go_to_my_main_function)
Running...
CTRL+C to end it all...

    # You can also call methods directly on your class with no RPC functionality,
    # for unit testing or something like that:
>>> server.my_method(buffs.Satellite(), buffs.Bool())
(my_sat, my_ant)

    # when you CTRL+C out of the server, it cleans up after itself so you don't
    # have to

    # you can also use a context manager to start and clean up a server:
>>> with MyServerImplementation() as server:
>>>     # do stuff with server
>>>     server.run()
Running...
CTRL+C to end it all...



SAMPLE INTERPRETER SESSION FOR THE CLIENT

>>> client = MyClientImplementation(server_endpoint="tcp://192.168.172.148:10000")
>>> client.my_method(buffs.Satellite(), buffs.Bool())
(my_sat, my_ant)

    # that's it.
    # you can also call eggs_ping:
>>> client.eggs_ping()
'eggs_pong'

    # if the server can't be reached, you'll get a timeout error
>>> client.eggs_ping()  # this can be any rpc call, not just ping
TimeoutError: Request timed out after 2000 ms.

    # when you're done with a client, call the term() method to cleanup.
    # you can call this as many times as you want
>>> client.term()

    # or you can use a context manager to clean up for you:
>>> with MyClientImplementation() as client:
>>>     # do unspeakable things with client
>>>     client.eggs_ping()  # so unspeakable
'eggs_pong'



'''
