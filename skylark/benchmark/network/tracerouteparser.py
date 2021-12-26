"""
A traceroute output parser, structuring the traceroute into a
sequence of hops, each containing individual probe results.

Courtesy of the Netalyzr project: http://netalyzr.icsi.berkeley.edu
"""
# ChangeLog
# ---------
#
# 1.0:  Initial release, tested on Linux/Android traceroute inputs only.
#       Also Python 2 only, most likely. (Send patches!)
#
# Copyright 2013 Christian Kreibich. All rights reserved.
#
# Redistribution and use in source and binary forms, with or without
# modification, are permitted provided that the following conditions are
# met:
#
#    1. Redistributions of source code must retain the above copyright
#       notice, this list of conditions and the following disclaimer.
#
#    2. Redistributions in binary form must reproduce the above
#       copyright notice, this list of conditions and the following
#       disclaimer in the documentation and/or other materials provided
#       with the distribution.
#
# THIS SOFTWARE IS PROVIDED "AS IS" AND ANY EXPRESS OR IMPLIED
# WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF
# MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
# DISCLAIMED. IN NO EVENT SHALL THE AUTHORS BE LIABLE FOR ANY DIRECT,
# INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
# (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR
# SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION)
# HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT,
# STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING
# IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
# POSSIBILITY OF SUCH DAMAGE.

from io import StringIO 
import re

class Probe(object):
    """
    Abstraction of an individual probe in a traceroute.
    """
    def __init__(self):
        self.ipaddr = None
        self.name = None
        self.rtt = None # RTT in ms
        self.anno = None # Annotation, such as !H, !N, !X, etc

    def clone(self):
        """
        Return a copy of this probe, conveying the same endpoint.
        """
        copy = Probe()
        copy.ipaddr = self.ipaddr
        copy.name = self.name
        return copy

class Hop(object):
    """
    A traceroute hop consists of a number of probes.
    """
    def __init__(self):
        self.idx = None # Hop count, starting at 1
        self.probes = [] # Series of Probe instances

    def add_probe(self, probe):
        """Adds a Probe instance to this hop's results."""
        self.probes.append(probe)

    def __str__(self):
        res = []
        last_probe = None
        for probe in self.probes:
            if probe.name is None:
                res.append('*')
                continue
            anno = '' if probe.anno is None else ' ' + probe.anno
            if last_probe is None or last_probe.name != probe.name:
                res.append('%s (%s) %1.3f ms%s' % (probe.name, probe.ipaddr,
                                                   probe.rtt, anno))
            else:
                res.append('%1.3f ms%s' % (probe.rtt, anno))
            last_probe = probe
        return '  '.join(res)

class TracerouteParser(object):
    """
    A parser for traceroute text. A traceroute consists of a sequence of
    hops, each of which has at least one probe. Each probe records IP,
    hostname and timing information.
    """
    HEADER_RE = re.compile(r'traceroute to (\S+) \((\d+\.\d+\.\d+\.\d+)\)')

    def __init__(self):
        self.dest_ip = None
        self.dest_name = None
        self.hops = []

    def __str__(self):
        res = ['traceroute to %s (%s)' % (self.dest_name, self.dest_ip) ]
        ctr = 1
        for hop in self.hops:
            res.append('%2d  %s' % (ctr, str(hop)))
            ctr += 1
        return '\n'.join(res)

    def parse_data(self, data):
        """Parser entry point, given string of the whole traceroute output."""
        self.parse_hdl(StringIO(data))

    def parse_hdl(self, hdl):
        """Parser entry point, given readable file handle."""
        self.dest_ip = None
        self.dest_name = None
        self.hops = []

        for line in hdl:
            line = line.strip()
            if line == '':
                continue
            if line.lower().startswith('traceroute'):
                # It's the header line at the beginning of the traceroute.
                mob = self.HEADER_RE.match(line)
                if mob:
                    self.dest_ip = mob.group(2)
                    self.dest_name = mob.group(1)
            else:
                hop = self._parse_hop(line)
                self.hops.append(hop)

    def _parse_hop(self, line):
        """Internal helper, parses a single line in the output."""
        parts = line.split()
        parts.pop(0) # Drop hop number, implicit in resulting sequence
        hop = Hop()
        probe = None

        while len(parts) > 0:
            probe = self._parse_probe(parts, probe)
            if probe:
                hop.add_probe(probe)

        return hop

    def _parse_probe(self, parts, last_probe=None):
        """Internal helper, parses the next probe's results from a line."""
        try:
            probe = Probe() if last_probe is None else last_probe.clone()

            tok1 = parts.pop(0)
            if tok1 == '*':
                return probe

            tok2 = parts.pop(0)
            if tok2 == 'ms':
                # This is an additional RTT for the same endpoint we
                # saw before.
                probe.rtt = float(tok1)
                if len(parts) > 0 and parts[0].startswith('!'):
                    probe.anno = parts.pop(0)
            else:
                # This is a probe result from a different endpoint
                probe.name = tok1
                probe.ipaddr = tok2[1:][:-1]
                probe.rtt = float(parts.pop(0))
                parts.pop(0) # Drop "ms"
                if len(parts) > 0 and parts[0].startswith('!'):
                    probe.anno = parts.pop(0)

            return probe

        except (IndexError, ValueError):
            return None

def demo():
    """A simple example."""

    tr_data = """
traceroute to edgecastcdn.net (72.21.81.13), 30 hops max, 38 byte packets
 1  *  *
 2  *  *
 3  *  *
 4  10.251.11.32 (10.251.11.32)  3574.616 ms  0.153 ms
 5  10.251.10.2 (10.251.10.2)  465.821 ms  2500.031 ms
 6  172.18.68.206 (172.18.68.206)  170.197 ms  78.979 ms
 7  172.18.59.165 (172.18.59.165)  151.123 ms  525.177 ms
 8  172.18.59.170 (172.18.59.170)  150.909 ms  172.18.59.174 (172.18.59.174)  62.591 ms
 9  172.18.75.5 (172.18.75.5)  123.078 ms  68.847 ms
10  12.91.11.5 (12.91.11.5)  79.834 ms  556.366 ms
11  cr2.ptdor.ip.att.net (12.123.157.98)  245.606 ms  83.038 ms
12  cr81.st0wa.ip.att.net (12.122.5.197)  80.078 ms  96.588 ms
13  gar1.omhne.ip.att.net (12.122.82.17)  363.800 ms  12.122.111.9 (12.122.111.9)  72.113 ms
14  206.111.7.89.ptr.us.xo.net (206.111.7.89)  188.965 ms  270.203 ms
15  xe-0-6-0-5.r04.sttlwa01.us.ce.gin.ntt.net (129.250.196.230)  706.390 ms  ae-6.r21.sttlwa01.us.bb.gin.ntt.net (129.250.5.44)  118.042 ms
16  xe-9-3-2-0.co1-96c-1b.ntwk.msn.net (207.46.47.85)  675.110 ms  72.21.81.13 (72.21.81.13)  82.306 ms

"""
    # Create parser instance:
    trp = TracerouteParser()

    # Give it some data:
    trp.parse_data(tr_data)

if __name__ == '__main__':
    demo()
