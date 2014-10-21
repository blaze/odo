""" web server management """

import logging

class Web(object):

    def __init__(self, parent):
        self.parent = parent
        self.is_initialized = False

    def start(self):
        """ start the web service """
        self.is_initialized = True
        return self

    def stop(self):
        """ stop the web service """
        self.is_initialized = False
        return self
