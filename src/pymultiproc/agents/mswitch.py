"""
    Message Switch
    ==============
    
    The Message Switch is first initialized as if it
    was part of the Main Process.
    
    When a "proc_starting" message is received, the
    "personality" of the MSWITCH is updated (i.e. Child process responsibilities).
    
    Bus Messages
    ============
    
    Switch-to-Switch:
    - "_started" : Child -> Main
    - "_ready"   : Main  -> Children
    - "_sub"     : Child -> Main -> other Children
    
    Publishes:
    - "started" : Child -> Main - denotes start of Child process
    
    Subscribes:
    - "*"      : promiscuous handler - used for bridging
    - "_sub"   : subscription notice - used to configure the bridge function
    - "_ready" : bridge this message to Child processes
    - "proc"   : intercepts & updates internal dictionary - used for forwarding
    - "proc_starting" : trigger the change the switch's personality
    
    - "mswitch_pump"   : message queue processing 
    - "mswitch_params" : switch configuration parameters
     
     
     
    @author: Jean-Lou Dupont
    Created on 2010-02-17
"""
__all__=[]

from pymultiproc.system.mbus import Bus

from Queue import Empty, Full
from multiprocessing import Queue



class MessageSwitch(object):
    """
    Message Format:
    
        [$mtype, $process_name, ...]
    """
    
    MAIN_PNAME="__main__"
    
    def __init__(self):
        self.mq=Queue()
        
        self._procs={}
        
        ## Subscription map
        ##  when acting as "main process switch"
        self._subs={}
        
        ## assume we start as being
        ## the Main process
        self._child=False
        self._pname=self.MAIN_PNAME
        self._mainq = self.mq
        
        self._term=False
        self._reset()
        
    def _reset(self):
        self.block=True
        self.timeout=0.1

    def _hpump(self):
        """ Pulls message(s) (if any) from the message queue
        
            Must be called periodically - it serves as "event loop"
            for the whole process (normally)
        
            A Child process:
            - "_sub"  : locally subscribe the Main process to msgType
                        This will send all local messages of msgType
                        up to the Main process where it can
                        be further distributed (multicasted)
                        
            - "_ready" : just pass along to Message Bus
            
            The Main process:
            - "_sub" : 
                - subscribe locally from Message Bus
                - send down to all Children in "split-horizon"
                
            - "_started" : just pass along on Message Bus
                
        """
        #print "mswitch._hpump: (%s) msg: %s" % (self._pname, "begin")
        
        Bus.publish(self, "%beat?")
        msg = self._getMsg()
        while (msg is not None) and (not self._term):
            self._processMsg(msg)
            msg=self._getMsgNoWait()
        Bus.publish(self, "%beat?")

    def _getMsg(self):
        
        try:          msg=self.mq.get(self.block, self.timeout)
        except Full:  msg=None
        except Empty: msg=None
        
        return msg

    def _getMsgNoWait(self):
        
        try:          msg=self.mq.get(False)
        except Full:  msg=None
        except Empty: msg=None
        
        return msg
        
    def _hproc(self, procDetails):
        """ Grab all the registered process details
        
            This information is essential prior to when 
            the Child processes are forked
        """
        try:    pname=procDetails["name"]
        except: 
            raise RuntimeError("missing `name` from `proc` message")
        
        self._procs[pname]=procDetails
        
        
    def _hproc_starting(self, (pname, pqueue)):
        """ Child process starting
            Change pname name and "personality"            
        """
        self._subs=[]
        self._child=True
        self._pname=pname
        self.mq=pqueue
        if self._child:
            self._sendToMain(["_started", pname])
                
    def _hready(self):
        """ Passes the `ready` message down to the Child procs
        """ 
        if not self._child:
            self._sendToChildren(["_ready", self.MAIN_PNAME])
        
    def _hsub(self, msgType):
        """ A local Client is interested in "msgType"
        
            If we are a Child process, we need to propagate
            this request to the Main process
        """
        
        ## filter-out the "strictly local" message-types
        if msgType.startswith("%"):
            return
        
        msg=["_sub", self._pname, msgType]  #### MSG ####
        if self._child:
            self._sendToMain(msg)
        else:
            self._addSub(self.MAIN_PNAME, msgType)
            self._sendToChildren(msg)
        
    def _sendToMain(self, msg):
        """ Sends a message to the Main process
        """
        #print "mswitch._sendToMain: msg:", msg
        self._mainq.put(msg)
        
    def _sendToChildren(self, msg):
        """ Propagate a message down to all Child processes
        """
        for pname in self._procs:
            details=self._procs[pname]
            
            try: q=details["queue"]
            except:
                raise RuntimeError("missing `queue` field from process details")
            if pname != self.MAIN_PNAME:
                q.put(msg)
        

    def _hparams(self, params):
        """
            @param params: dictionary 
        """
        self.block   = params.get("block",   True)
        self.timeout = params.get("timeout", 0.1)

    def _hsigterm(self, *p):
        """Handler for `sigterm`"""
        self._term=True

        
    def _processMsg(self, msg):
        
        try:   mtype=msg.pop(0)
        except:
            raise RuntimeError("invalid message format, missing `mtype` field in list")
        
        try:   spname=msg.pop(0)
        except:
            raise RuntimeError("invalid message format, missing `pname` field in list")
        
        if self._child:
            self._hpumpChild(mtype, spname, msg)
        else:
            self._hpumpMain(mtype, spname, msg)
            
    def _hpumpChild(self, mtype, spname, msg):
        #print "** mswitch._hpumpChild: mtype(%s) spname(%s)" % (mtype, spname)
        
        if mtype=="_ready":
            #print "mswitch._hpumpChild: sending `_ready` on local Bus"
            Bus.publish(self, "_ready")
            return
        
        if mtype=="_sub":
            
            try:    msgtype=msg.pop(0)
            except:
                raise RuntimeError("missing `msgType` from `_sub` message")
            
            self._addSub(self.MAIN_PNAME, msgtype)
            #print "*** mswitch_hpumpChild: child(%s) subscribing to msgtype(%s)" % (self._pname, msgtype)
            return
        
        ## All other "system" messages are ignored
        if mtype.startswith("_"):
            return
        
        ## Finally, publish whatever message we receive
        ##  from the Main process: we should have subscribed
        ##  to these anyhow (unless of course there is a bug ;-)
        #print "mswitch._hpumpChild: mtype(%s) msg(%s)" % (mtype, msg)
        Bus.publish(self, mtype, *tuple(msg))
    
    def _hpumpMain(self, mtype, spname, msg):
        
        #print "@@ mswitch._hpumpMain: mtype(%s) spname(%s)" % (mtype, spname)
        
        if mtype=="_started":
            Bus.publish(self, "started", spname)
            return
            
        if mtype=="_sub":
            try:    msgtype=msg.pop(0)
            except:
                raise RuntimeError("missing `msgType` from `_sub` message")           
            self._addSub(spname, msgtype)

            #print "*** mswitch._hpumpMain: subscribing to msgtype: ", msgtype
            
            ## repeat source message
            self._sendSplitHorizon(mtype, spname, [msgtype])
            return
        
        self._sendToSubscribers(mtype, spname, msg)

    def _promiscuousHandler(self, *p):
        #print "Promiscuous, msg: ", p
        params=list(p)
        mtype=params.pop(0)
        
        if mtype.startswith("%"):
            return
        
        msg=[mtype, self._pname]
        msg.extend(params)
        
        if self._child:
            if mtype in self._subs:
                self._sendToMain(msg)
        else:
            self._sendToSubscribers(mtype, self.MAIN_PNAME, params)
       

    def _addSub(self, pname, msgType):
        """ Adds a subscriber (a process) to
            a publishing list for "msgType"
        """
        if self._child:
            if msgType not in self._subs:
                self._subs.extend([msgType])
                return
        else:
            #print "mswitch._addSub: mtype(%s) pname(%s)" % (msgType, pname)
            subs=self._subs.get(msgType, [])
            if pname not in subs:
                subs.extend([pname])
                self._subs[msgType]=subs

    def _sendSplitHorizon(self, mtype, spname, msgTail):
        msg=[mtype, spname]
        msg.extend(msgTail)
        for pname in self._procs:
            
            ## split horizon i.e. not back to source
            ## not to self also !!
            if pname==spname or pname==self.MAIN_PNAME:
                continue

            #print "mswitch._sendSplitHorizon: mtype(%s) pname(%s) msg(%s) msgTail(%s)" % (mtype, pname, msg, msgTail)
            q=self._getQueue(pname)
            q.put(msg)


    def _sendToSubscribers(self, mtype, spname, msgTail):
        msg=[mtype, spname]
        msg.extend(msgTail)
        subs=self._subs.get(mtype, [])
        #print "mswitch._sendToSubscribers: mtype(%s) subs: %s " % (mtype, subs)
        for pname in subs:
            if pname==spname:# or pname==self.MAIN_PNAME:
                continue
            
            if pname==self.MAIN_PNAME:
                Bus.publish(self, mtype, *tuple(msgTail))
            else:
                #print "mswitch._sendToSubscribers: mtype(%s) pname(%s) msg(%s)" % (mtype, pname, msg)
                q=self._getQueue(pname)
                q.put(msg)

    def _getQueue(self, pname):
        try:    details=self._procs[pname]
        except:
            raise RuntimeError("missing proc from proc list")
        
        try:    q=details["queue"]
        except:
            raise RuntimeError("missing `queue` parameter for proc(%s)" % pname)
        
        return q

## ===============================================================================
## ===============================================================================


_mswitch=MessageSwitch()
        
Bus.subscribe("*",              _mswitch._promiscuousHandler)
Bus.subscribe("_sigterm",       _mswitch._hsigterm)
        
Bus.subscribe("_sub",           _mswitch._hsub)
Bus.subscribe("proc",           _mswitch._hproc)
Bus.subscribe("_ready",         _mswitch._hready)

Bus.subscribe("mswitch_pump",   _mswitch._hpump)
Bus.subscribe("mswitch_params", _mswitch._hparams)

Bus.subscribe("proc_starting",  _mswitch._hproc_starting)

