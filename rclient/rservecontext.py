"""
Manages the pyRserve Rconnector instance to provide extra control.

Allows for uploading files into the connection's tmp folder, and removing the tmp folder once the connection has been
lost.

fixme: Do we need to worry about blocking here?
fixme: Should we specify a working directory in which a model has been previously uploaded?
       If so, prevent scripts from overwriting the model somehow.
"""

import pyRserve
import os
import shutil
import logging
import re

# fixme: move somewhere central
# NOTE: conf['tmp'] should be configured to avoid doing rm -rf on the wrong folder
conf = {
    'tmp': '/private/tmp/Rserv',
}


class RContext(object):
    """ RContext with localhost connection and local file storage.
        A proxy for the pyRserve connection object which is documented at:
            https://pythonhosted.org/pyRserve/manual.html

        :param save_files: If False, the cleanup step will remove the temporary folder set up by RServe
                           CAUTION: Be sure your Rserve instance is not setting its working directory to some place important

        :param conn_type: Used to determine how to upload files. "local" type copies files directly

        Usage:
            Use a context manager

            with RContext(save_files=False) as r:
                r.upload('mybundle.tar.gz')
                r.eval('myfunction()')

            Uploaded files will be removed if save_files == False

            You can use without a context manager.

            rcon = RContext(save_files=False)
            rcon.upload('mybundle.tar.gz')
            rcon.eval('myfunction()')
            rcon.close()

            Some more interesting functions
            rconn.voidEval("r code that as no output, such as a function definition")
            rconn.r.someRFunction(arguments)  # any r function available to the interpreter may be called this way.
                                              # with the exception of dotted functions which are allowed by r


        fixme: what's the best way to abstract this so we can have non-local connections?
        upload and remove_tmp_files will be different if files aren't local.

        idea: could use __new__ to populate a pool?

    """

    def __init__(self, pool=None, save_files=True, *args, **kwargs):
        self._save_files = save_files
        self.connection = None
        self._connection_home = None  # define the working dir after connection
        self._pool = pool
        self._connect(*args, **kwargs)

    def __del__(self):
        """ return to pool when refcount -> 0
            problem with this is that it will have a ref in pool.checked_out
        """
        self.disconnect()

    def __getattr__(self, item):
        """ Any undefined attributes will be proxied to the pyRserve connection
            note that pyRserve uses camelCase for its methods.
        """
        return getattr(self.connection, item)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        """ todo: refer to the pool about how to tear us down. """
        self.close()

    def reconnect(self):
        if self.connection is None:
            self._connect()
        else:
            self._disconnect()
            self._connect()

    def upload(self, source_archive):
        """ Local 'upload' and decompress
            Archive types supported:
            zip, tar, tar.gz, tar.bz2, tar.xz
        """
        if self.connection_home is not None:
            try:
                shutil.unpack_archive(source_archive, self.connection_home)
            except ValueError:
                # Couldn't find a suitable archive format
                # Let's assume this is just an uncompressed file
                shutil.copy(source_archive, self.connection_home)

    def close(self):
        """ override close for tmp file removal and possible pool checkin
            if there is a pool, it will decide
        """

        if self._pool is None:
            self._disconnect()
        else:
            self._return_to_pool()

        if self._save_files is False:
            self._remove_tmp_files()

    def shutdown(self):
        """ don't allow shutting down of rserve
            self.connection.shutdown may still be used, but it's not recommended
        """
        raise NotImplementedError("""Please don't shut down the RServe from here""")

    @property
    def connection_home(self):
        """ this is set once on connection. don't modify this, because it is the target of a rm -rf cleanup operation
            when connection's temp files are cleaned up
        """
        return self._connection_home

    def _remove_tmp_files(self):
        """ We ensure conf['tmp'] is defined, and _r_working_dir is a direct child of conf['tmp'].
            Don't set conf['tmp'] to something stupid, or you could lose data.
        """
        try:
            tmp = conf['tmp']

        except IndexError:
            logging.warning("""Configure rservecontext with the Rserver's temp folder in order to enable deleting.""")

        else:
            if os.path.dirname(self.connection_home) == tmp:
                try:
                    shutil.rmtree(self.connection_home)
                except:
                    logging.error("Could not remove {}".format(self.connection_home))

    def _connect(self, *args, **kwargs):
        """ calls connection function with no args because we're local
            Only allows one connection per context
        """
        if self.connection is None:
            self.connection = pyRserve.connect(*args, **kwargs)
            self._connection_home = self.connection.r.getwd()

    def _disconnect(self):
        """ simply close the connection leaving everything else in tact"""
        self.connection.close()
        self.connection = None

    def _return_to_pool(self):
        """ Put this connection back in the pool """
        self._pool.checkin(self)
