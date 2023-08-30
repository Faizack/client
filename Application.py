from BCCommunicator import BCCommunicator
from FSCommunicator import FSCommunicator
from Model import Model
import torch
import os
from Requester import Requester
# from Worker import Worker1
from Worker1 import Worker
from dotenv import load_dotenv
from FSCommunicator import FSCommunicator
import ipfshttpclient
import socket
import pickle
import json 
import random

from collections import OrderedDict

from config_app import HOST,PORT


# Main class to simulate the distributed application
class Application:

    def __init__(self, num_workers, num_rounds, ipfs_folder_hash, num_evil=0):
        self.client = ipfshttpclient.connect()
    
        self.num_workers = num_workers
        self.num_rounds = num_rounds
        self.DEVICE = torch.device(
            "cuda:0" if torch.cuda.is_available() else "cpu")
        self.fspath ='QmdzVYP8EqpK8CvH7aEAxxms2nCRNc98fTFL2cSiiRbHxn'
        self.workers = []
        self.topk = num_workers
        self.worker_dict =  OrderedDict()
        self.num_evil = num_evil
        self.ffi = None  # Add the FFI attribute and set it to None
        self.contract_address = None
        self.worker_address = {}

        
 
    def send_data(self,socket, data):
        serialized_data = pickle.dumps(data)
        data_size = len(serialized_data)
        socket.sendall(data_size.to_bytes(4, 'big'))  # Sending the size of data first
        socket.sendall(serialized_data)              # Sending the actual data   


    def receive_data(self,socket):
        # Receiving the size of data first
        data_size_bytes = socket.recv(4)
        data_size = int.from_bytes(data_size_bytes, 'big')

        # Receiving the serialized data
        serialized_data = b""
        while len(serialized_data) < data_size:
            remaining_bytes = data_size - len(serialized_data)
            serialized_data += socket.recv(remaining_bytes)

        # Decoding the serialized data using pickle
        data = pickle.loads(serialized_data)
        return data
    


    def average(self, worker_weights):
        all_keys_list = list(worker_weights.keys())

        print("all keys: ", all_keys_list)

        averaged_weights = OrderedDict()
        for layer_key in worker_weights[all_keys_list[0]]:
            layer_weights = [worker_weights[worker][layer_key] for worker in worker_weights]
            averaged_weights[layer_key] = torch.stack(layer_weights).mean(dim=0)


        return averaged_weights

    def save_worker_data_to_json(self, worker_data):
        # Save the worker data dictionary to JSON file
        worker_data_1 = []
        worker_data_2 = []

        for data in worker_data:
            if data['location'] == 'INDIA':
                worker_data_1.append(data)
            elif data['location'] == 'USA':
                worker_data_2.append(data)

        # Save Cluster 1 data to JSON file
        with open('C1_worker_data.json', 'w') as file:
            json.dump(worker_data_1, file)

        # Save Cluster 2 data to JSON file
        with open('C2_worker_data.json', 'w') as file:
            json.dump(worker_data_2, file)

    def load_worker_data_from_json(self,files):
        # Load the worker data from JSON file
        with open(files, 'r') as file:
            worker_data = json.load(file)
        return worker_data
    
    def shuffle_worker_head(self,worker_head_ids):
    # Randomly shuffle the worker head IDs to select one for each round
        random.shuffle(worker_head_ids)
        return worker_head_ids[0]
    
    def load_worker_head_ids(self,json_file):
    # Load the JSON file and extract worker head IDs
        with open(json_file, 'r') as f:
            worker_head_ids = json.load(f)
        return worker_head_ids


    def choose_random_worker(self):
        # Load worker data from the JSON file
        worker_data = self.load_worker_data_from_json()
        # Choose a random worker ID
        random_worker_id = random.choice(list(worker_data.keys()))
        return worker_data[random_worker_id]

    def send_worker_data_to_clients(self, worker_data):
        # Serialize the worker data
        serialized_worker_data = json.dumps(worker_data).encode()

        # Send the worker data to all clients
        for client_socket in client_socket:
            client_socket.send(serialized_worker_data)

    
    def run(self):
        load_dotenv()
        self.requester = Requester(os.getenv('REQUESTER_KEY'))
        contract_address=self.requester.deploy_contract()
        print("Contract Address:", contract_address)
        self.requester.init_task(10000000000000000000, self.fspath, self.num_rounds)
        print("Task initialized")

        # Create a socket

        server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server_socket.bind((HOST, PORT))
        server_socket.listen(4)  # Allow Six client connections
        print("Waiting for connections from clients...")

        # Accept connections from two clients
        # Create a list to store client sockets and worker data
        client_sockets = []
        worker_info_list = []
        # Accept connections from worker clients and store their socket information
        for i in range(4):
            client_socket, addr = server_socket.accept()
            print("Connection from:", addr)
            client_sockets.append(client_socket)

            # You can also add any additional information about the worker here if needed
            client_info = {
                'address': addr[0],
                'port': addr[1],
                'workerid': i+1,
                
            }
            worker_info_list.append(client_info)

        # Save the worker information to JSON file
        # self.save_worker_data_to_json(worker_info_list)
        # print("Worker information saved")

        
        print("Received all client connections")


        try:
                for idx,client_socket in enumerate(client_sockets):
                    print("Sending Contract Address to client:",idx+1)
                    self.send_data(client_socket, contract_address)

        except ConnectionResetError:
                # Handle the case when a client disconnects unexpectedly
                print("Client", idx + 1, "disconnected.")
                client_sockets.pop(idx)



        new_port=[]

        self.requester.start_task()

        print("Requester Start Task")

        
        # Receive Meta Data
        for idx, client_socket in enumerate(client_sockets):
            meta = self.receive_data(client_socket)
            print(f"{idx+1} meta data : {meta}" )


            port=meta['port'] 
            locations=meta['locations'] 
            cluster_head=meta['cluster_head_port']
            # print("Port", port)
            # print("Location", locations)
            new_port.append(port)
            # Update the 'port,location and cluster_head_port' value in the client_info dictionary
            worker_info_list[idx]['new_port'] = port
            worker_info_list[idx]['location'] = locations
            worker_info_list[idx]['cluster_head_port']=cluster_head
        
        # Storing worker blockchan Address
        for idx, client_socket in enumerate(client_sockets):
            worder_addr = self.receive_data(client_socket)
            self.worker_address[idx] = worder_addr


        print("Worker Address",self.worker_address)

        # Save the updated worker information with new ports to the JSON file
        self.save_worker_data_to_json(worker_info_list)
        print("Worker information with new ports saved")


        while True :

            # file_json=self.load_worker_data_from_json()

            file_name_1='C1_worker_data.json'
            file_name_2='C2_worker_data.json'

            file_json_1=self.load_worker_data_from_json(file_name_1)
            file_json_2=self.load_worker_data_from_json(file_name_2)




            worker_head_ids_1 = self.load_worker_head_ids(file_name_1)
            worker_head_id_1 = self.shuffle_worker_head(worker_head_ids_1)

            worker_head_ids_2 = self.load_worker_head_ids(file_name_2)
            worker_head_id_2 = self.shuffle_worker_head(worker_head_ids_2)


            print("suffle_id id ",worker_head_id_1)

            try:
                for idx,client_socket in enumerate(client_sockets[:2]):
                    print("Sending json file to client for Cluster 1:",idx+1)
                    self.send_data(client_socket, file_json_1)
                    self.send_data(client_socket,worker_head_id_1)
                    self.send_data(client_socket,worker_head_id_2)


            except ConnectionResetError:
                # Handle the case when a client disconnects unexpectedly
                print("Client", idx + 1, "disconnected.")
                client_sockets.pop(idx)




            except Exception as e:
                print("Error sending data",e)

            try:
                for idx,client_socket in enumerate(client_sockets[2:]):
                    print("Sending json file to client for Cluster 2:",idx+1)
                    self.send_data(client_socket, file_json_2)
                    self.send_data(client_socket,worker_head_id_2)
                    self.send_data(client_socket,worker_head_id_1)



            except ConnectionResetError:
                # Handle the case when a client disconnects unexpectedly
                print("Client", idx + 1, "disconnected.")
                client_sockets.pop(idx)




            except Exception as e:
                print("Error sending data",e)

            print("Send file to all clusters")

            
            # Receive serialized data f1rom each client
            worker_weights = []
            for idx, client_socket in enumerate(client_sockets[:2]):
                unsorted_scores = self.receive_data(client_socket)
                unsorted_scores = [score[0][0].cpu().item() for score in unsorted_scores]
                
                # Ensure there is one score per worker (num_workers)
                while len(unsorted_scores) < self.num_workers:
                    unsorted_scores.append(-1)
                
                unsorted_scores = (idx, unsorted_scores)
                print("Unsorted scores",unsorted_scores)

                self.requester.push_scores(unsorted_scores, self.num_workers)
                print("score sent by idx:", idx)
        


            overall_scores = self.requester.calc_overall_scores(
                self.requester.get_score_matrix(), self.num_workers)
            
            print("get score matrix:", self.requester.get_score_matrix())
            round_top_k = self.requester.compute_top_k(
                list(self.worker_address.values())[:2], overall_scores)
            
            penalize = self.requester.find_bad_workers(
                list(self.worker_address.values())[:2], overall_scores)
            print("penalize :", penalize)
            self.requester.penalize_worker(penalize)
            self.requester.refund_worker(list(self.worker_address.values())[:2])

            
            
            self.requester.submit_top_k(round_top_k)
            
            self.requester.distribute_rewards()
            print("Distributed rewards for Cluster 1. ")

            for idx, client_socket in enumerate(client_sockets[2:]):
                unsorted_scores = self.receive_data(client_socket)
                unsorted_scores = [score[0][0].cpu().item() for score in unsorted_scores]
                
                # Ensure there is one score per worker (num_workers)
                while len(unsorted_scores) < self.num_workers:
                    unsorted_scores.append(-1)
                
                unsorted_scores = (idx, unsorted_scores)
                print("Unsorted scores",unsorted_scores)

                self.requester.push_scores(unsorted_scores, self.num_workers)
                print("score sent by idx:", idx)
        


            overall_scores = self.requester.calc_overall_scores(
                self.requester.get_score_matrix(), self.num_workers)
            
            print("get score matrix:", self.requester.get_score_matrix())
            round_top_k = self.requester.compute_top_k(
                list(self.worker_address.values())[3:], overall_scores)
            
            penalize = self.requester.find_bad_workers(
                list(self.worker_address.values())[3:], overall_scores)
            print("penalize :", penalize)
            self.requester.penalize_worker(penalize)
            self.requester.refund_worker(list(self.worker_address.values())[3:])

            
            
            self.requester.submit_top_k(round_top_k)
            
            self.requester.distribute_rewards()
            print("Distributed rewards for Cluster 2. Next round starting soon...")

            # self.requester.next_round()
                   
            print("Connection Closed")
            server_socket.close()
            return 0


 


