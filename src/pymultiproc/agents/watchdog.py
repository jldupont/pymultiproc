"""
    Watchdog Agent
    
    Child behavior:
    - receives "beat" message-type & counts them
        - when missing MISSING_BEATS, generate "%bark"
        
    
    Message Bus
    ===========
    Publishes:
    - "%bark"    : error detected
    - "alive"    : meant for the Main process
    - "_sigterm" : the process received the SIGTERM signal
    - "_ready"   : (only Main) destined to Children processes
    
    Subscribes:
    - "proc_starting" : updates the personality of the Watchdog
    - "_started"      : accumulates to generate "_ready"
    - "proc"  : accumulate process details
    - "beat"  : from Main process
    - "%beat" : local `heart` beat
    
    @author: Jean-Lou Dupont

    Created on 2010-02-18
"""
__all__=[]
import signal
from pymultiproc.system.mbus import Bus

class Child_WatchDogAgent(object):
    """ Child specialized WatchDog
    """
    MISSING_BEATS = 3
    
    def __init__(self):
        self.pname=None
        self.is_child=False
        self._term=False
        self._termSent=False
        self._beat_count=0
        self._barked=False

    def _sterm(self, signum, _):
        """ TERM signal handler"""
        print "*** Watchdog._sterm"
        self._term=True
        
    def _hlbeat(self, *p):
        """ Process-local Heart-Beat
        """
        if self._barked:
            return
        
        self._beat_count=self._beat_count+1
        
        if self._beat_count >= self.MISSING_BEATS:
            Bus.publish(self, "%log", "warning", "watchdog expired, proc(%s)" % self.pname)
            Bus.publish(self, "%bark", self.pname)
            self._barked=True 

        Bus.publish(self, "alive", self.pname)
        
    def _hbeat(self, *p):
        """ Heart-beat from Main process
        """
        self._beat_count=0
        
        if self._termSent:
            return

        if self._term:
            self._termSent=True
            Bus.publish(self, "%log", "warning", "SIGTERM received, proc(%s)" % self.pname)
            Bus.publish(self, "_sigterm", self.pname)

    def _hproc_starting(self, (pname, _)):
        self._beat_count=0
        self.is_child=True
        self.pname=pname
        signal.signal(signal.SIGTERM, self._sterm)
        
        ### Only subscribe to the signals
        ###  when we know we are a Child process
        Bus.subscribe("beat",   _cwd._hbeat)        
        Bus.subscribe("%beat",  _cwd._hlbeat)
        

_cwd=Child_WatchDogAgent()
Bus.subscribe("proc_starting", _cwd._hproc_starting)





class Main_WatchDogAgent(object):
    
    BEATS_THRESHOLD = 3
    
    def __init__(self):
        self.pname="__main__"
        self._subToAlive=False
        self._ready=False
        self.alives=[]
        self.is_child=False
        self._procs={}
        self._started=[]
        self._term=False
        self._termSent=False
        self._beat_count=0
        self._shutdown_initiated=False
        signal.signal(signal.SIGTERM, self._sterm)

    def _sterm(self, signum, _):
        """ TERM signal handler"""
        print "*** Watchdog._sterm"
        self._term=True
    
    def _halive(self, pname):
        """ Alive message-type
        
            Originates from Child processes
        """
        if self.is_child:
            return
        
        #print "@@ Alive from pname: ", pname
        
        if pname not in self.alives:
            self.alives.extend([pname])
    
    def _hbeat(self, _):
        """ Local "beat" message-type
        """
        if self.is_child:
            return
    
        if self._termSent or self._shutdown_initiated:
            return
        
        if self._term:
            Bus.publish(self, "%log", "warning", "SIGTERM received, Main Proc")
            Bus.publish(self, "shutdown")
            self._shutdown_initiated=True
        
        if not self._subToAlive:
            if self._ready:
                self._subToAlive=True
                Bus.subscribe("alive", self._halive)
        
        ### Make sure we have a response from each of the
        ###  Child processes within the Time Interval
        self._beat_count=self._beat_count+1
        if self._beat_count == self.BEATS_THRESHOLD:
            self._beat_count=0
            if len(self.alives) != len(self._procs):
                Bus.publish(self, "%log", "warning", "child process(es) missing")
                Bus.publish(self, "%bark", self.pname)
                Bus.publish(self, "shutdown")
                self._shutdown_initiated
            self.alives=[]
        
    def _hproc_starting(self, (pname, _)):
        """ Only "child" processes receives this message

            Intercepting this message serves to distinguish
            the Main process from the Child process(es)  
        """
        self.is_child=True
        self.pname=pname
        self._beat_count=0
    
    def _hproc(self, pdetails):
        if self.is_child:
            return
        
        try:    pname=pdetails.get("name", None)
        except: pname=None
        
        ## This error would have been caught upstream
        ## Paranoia in action ;-)
        if pname is not None:      
            procDetails=self._procs.get(pname, {})
            procDetails.update(pdetails)
            self._procs[pname]=procDetails
        
    def _hstarted(self, pname):
        """ Generates `ready` message when all the
            Child processes report as being started
        """
        if self.is_child:
            return
        
        self._started.extend([pname])
        if len(self._started) == len(self._procs):
            Bus.publish(self, "_ready")
            self._ready=True
                    
## ============================================================================
## ============================================================================
    
_watchdog=Main_WatchDogAgent()
Bus.subscribe("beat",          _watchdog._hbeat)
Bus.subscribe("proc",          _watchdog._hproc)
Bus.subscribe("proc_starting", _watchdog._hproc_starting)
Bus.subscribe("started",       _watchdog._hstarted)
