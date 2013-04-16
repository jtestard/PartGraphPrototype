'''
Created on 2013-04-09

@author: jtestard
'''
from zen.graph import Graph as ZenGraph
from subprocess import check_output, Popen
import os
import sys
import yaml

class Graph(ZenGraph):
    '''
    This is the graph that does partitions. This is first prototyped in
    Python before maybe be translated into Cython. Cython is too hard to
    start right now, with no prototype.
    '''

    def __init__(self, path, filename, power):
        '''
        Constructor
        '''
        self.path = path  # Path to the .edges file
        self.filename = filename  # name of the .edges file (without its path)
        self.power = power  # power of 2 corresponding to the number of partitions
        self.numPartitions = self.numberOfPartitions()  # There are 2^power partitions and this number is stored here.
        self.partitions = []
        '''
        Read properties file
        '''
        fullpath = os.path.dirname(os.path.realpath(__file__)) + '/../properties.yaml'
        stream = open(fullpath, 'r')
        self.properties = yaml.load(stream)
        self.properties['jar'] = os.path.dirname(os.path.realpath(__file__)) + self.properties['jar']
        
    def numberOfPartitions(self):
        k = 1
        i = 1
        while i <= self.power:
            k = k << 1
            i += 1
        return k
    
    def convertE2V(self):
        pyname, pyFileExtension = os.path.splitext(str(self.path) + '/' + str(self.filename))
        print pyname, pyFileExtension
        if pyFileExtension.lower() != '.edges':
            raise 'Wrong file extension for edges file!'
        edge_fullpath = pyname + pyFileExtension
        node_fullpath = pyname + ".nodes"
        # FIXME: Warning, this solution can lead to data corruption 
        # if the edge file is in the wrong format.
        command = ["wc", "-l" , edge_fullpath]
        numberOfEdges = int(check_output(command).strip()[0])  # Will raise an exception if not integer.
        command = ["java", '-jar', self.properties['jar'], edge_fullpath, str(numberOfEdges), node_fullpath]
        return Popen(command)
    
    # echo -e "storage/barabasi/barabasi41000.nodes\nstorage/barabasi/barabasi41000.parts\n1\n100\n4\n1\nn" | chaco >> storage/barabasi/barabasi41000.run
    def chaco(self):
        system('rm *.run')
        pyname, pyFileExtension = os.path.splitext(str(self.path) + '/' + str(self.filename))
        node_fullpath = pyname + ".nodes"
        print 'Partitioning '+ node_fullpath + "..."
        part_fullpath = pyname + ".parts"
        run_fullpath = pyname + ".run"
        chaco_cmd = "\"" + node_fullpath + "\n" + part_fullpath + "\n" + str(self.properties['partition']['method']) + "\n" + str(self.properties['partition']['vtx']) + "\n" + str(self.power) + "\n" + str(self.properties['partition']['div_by']) + "\nn\"" 
        command = ' '.join(["echo", "-e", chaco_cmd, "|", self.properties['chaco'],'>>',run_fullpath])
        return Popen(command,shell=True,stdout=True)
    
    def partition(self):
        part_index = 0 
        pyname, pyfileExtension = os.path.splitext(self.filename)
        for i in range(self.numPartitions):
            self.partitions[i] = pyname + str(i) + pyfileExtension
    
    def describe(self):
        print 'This is a graph whose original filename is : ' + str(self.path) + '/' + str(self.filename) + ". It will be split into " + str(self.numPartitions) + " partitions."

if __name__ == "__main__":
    import graph
    G = graph.Graph(os.path.dirname(os.path.realpath(__file__)) + "/../storage/barabasi", sys.argv[1], 5)
    G.describe()
    #G.convertE2V()
    G.chaco()
    print 'Done.'