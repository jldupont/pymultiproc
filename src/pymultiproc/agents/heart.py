"""
    `Heart` Agent
    
    Child behavior:
    - generates the "%beat" message
    
    Main behavior:
    - generates the "beat" message
    
    
    - Responds to the question "%beat?"
    
    @author: Jean-Lou Dupont

    Created on 2010-02-18
"""
__all__=[]

from threading import Event, Thread

from pymultiproc.system.mbus import Bus


class IntervalTimer(Thread):
    """ Threading based Interval Timer
    
        This interval timer is better suited than
        using "signal" based one as it doesn't
        generate IOError exceptions (which disrupts
        communications).
    """
    def __init__(self, interval, callable):
        Thread.__init__(self)
        self.interval = interval
        self.callable = callable
        self.finished = Event()
 
    def run(self):
        while not self.finished.is_set():
            self.finished.wait(self.interval)
            if not self.finished.is_set():
                self.callable()
 
    def cancel(self):
        self.finished.set()



class HeartAgent(object):
    """ CAUTION: This Agent straddles 2 threads
    """
    _INTERVAL=1  ## seconds
    
    def __init__(self):
        self._child=False
        self._beat=False
        self._reset()
        self.pname="__main__"
    
    def _reset(self):
        self._timer=IntervalTimer(self._INTERVAL, self._tick)
        self._timer.start()
        
    def _hproc_starting(self, (pname, _)):
        """ When a child process starts,
            we need to initialize the Timer
        """
        self._child=True
        self._reset()
    
    def _tick(self):
        self._beat=True ## atomic assignment

    def _qbeat(self):
        if self._beat:
            if self._child:
                Bus.publish(self, "%beat", self.pname)
            else:
                Bus.publish(self, "beat", self.pname)

        self._beat=False ## atomic assignment
        
        
    
_heart=HeartAgent()
Bus.subscribe("%beat?",        _heart._qbeat)
Bus.subscribe("proc_starting", _heart._hproc_starting)





## =====================================================

if __name__=="__main__":
    from time import sleep
    
    Bus.debug=True
    
    class Cb(object):
        def beat(self, state):
            print "state: ",state
        
    _cb=Cb()
        
    Bus.subscribe("%beat", _cb.beat)
    Bus.subscribe("beat",  _cb.beat)
    
    while True:
        Bus.publish(None, "%beat?")
        sleep(0.1)

