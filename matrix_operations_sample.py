# coding: utf8
# author: Peter

'''
The aim of this sample code is to
- present functions that read data from db
- present functions that transform dyadic dataset to matrix dataset
    enabling the data scientist to identify patterns (frequencies of triads, mutual relations etc) in graph datasets

'''


class Matrix(object):
    # Matrix that can be used for matrix operations

    def __init__(self, node_sender, node_receivers, nodes_in_graph):
        self.senders = senders
        self.receivers = receivers
        self.list_of_nodes = nodes_in_graph
        self.graph = self.create_matrix()

    def order_of_nodes(self):
        # create a list of nodes in dictionary

        order_of_nodes_in_graph = {}

        for i in range(len(self.list_of_nodes)):
            order_of_nodes_in_graph[self.list_of_nodes[i]] = i

        return(order_of_nodes_in_graph)

    def create_matrix(self):
        # create data matrix from dyads where
        # {node1: [0,0,0,0,0,0,0], node2: [0,0,0,0,0,0,0]}

        order_of_nodes_in_graph = Matrix.order_of_nodes(self)
        matrix = {}

        for i in (self.list_of_nodes):
            matrix[i] = [0 for i in range(len(self.list_of_nodes))]

        for i in range(len(self.senders)):
            matrix[self.senders[i]][order_of_nodes_in_graph[self.receivers[i]]] += 1

        return(matrix)

class Database(object):

    # connect to database and read data for Matrix function

    def __init__(self, driver, server, database, uid, password, table):
        self.driver = driver
        self.server = server
        self.database = database
        self.uid = uid
        self.password = password
        self.table = table

    def connect_to_db(self):
        import pyodbc
        
        # connect db and create graph attributes (sender id, receiver id, edgeattrib id) 

        connect = pyodbc.connect('DRIVER='+self.driver+';SERVER='+self.server+';DATABASE='+database+';UID='+self.uid+';PWD='+self.password)
        cursor = connect.cursor()

        cursor.execute("SELECT "+self.table+".SENDER, "+self.table+".RECEIVER, "+self.table+".EDGEATTRIB FROM "+self.table)
        rows = cursor.fetchall()
        
        sender = []
        receiver = []
        edge_attrib = []

        for row in rows:
            sender.append(row.sender)
            receiver.append(row.receiver)
            edge_attrib.append(row.edgeattrib)

        return(sender, receiver, edge_attrib)
