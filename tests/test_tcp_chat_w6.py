import socket
import threading
import time
import unittest
import uuid
from protocol import recv_json_frame, send_json_frame, pack_msg
from registry import UserRegistry
from server import CollaborationServer

class ChatTest(unittest.TestCase):
    def setUp(self):
        registry = UserRegistry(users_file=None)
        registry.user_db = {u: 'pw' for u in ('alice', 'bob', 'charlie')}
        self.server = CollaborationServer(host='127.0.0.1', port=0, udp_port=0, registry=registry, log_file=None)
        self.peers = []
        for i, name in enumerate(('alice', 'bob', 'charlie')):
            client, server = socket.socketpair()
            client.settimeout(0.25)
            threading.Thread(target=self.server.handle_client, args=(server, ('127.0.0.1', 1000+i)), daemon=True).start()
            send_json_frame(client, {'type':'AUTH', 'username':name, 'password':'pw'})
            self.assertEqual(recv_json_frame(client)['type'], 'AUTH_OK')
            self.peers.append(client)
    def tearDown(self):
        for peer in self.peers: peer.close()
        self.server.stop()
    def test_unicast_broadcast_and_identity(self):
        a,b,c = self.peers
        send_json_frame(a, {'type':'MSG','id':str(uuid.uuid4()),'to':'bob','from':'charlie','text':'hello'})
        self.assertEqual(recv_json_frame(b)['from'], 'alice')
        with self.assertRaises(socket.timeout): recv_json_frame(c)
        send_json_frame(a, {'type':'MSG','id':str(uuid.uuid4()),'to':'*','text':'all'})
        self.assertEqual(recv_json_frame(b)['text'], 'all')
        self.assertEqual(recv_json_frame(c)['text'], 'all')
        with self.assertRaises(socket.timeout): recv_json_frame(a)
    def test_unauthenticated_cannot_send(self):
        a,b = socket.socketpair(); a.settimeout(.2)
        try:
            threading.Thread(target=self.server.handle_client,args=(b,('127.0.0.1',999)),daemon=True).start()
            send_json_frame(a, {'type':'MSG','id':str(uuid.uuid4()),'to':'bob','text':'bad'})
            with self.assertRaises(socket.timeout): recv_json_frame(self.peers[1])
        finally: a.close()
    def test_concurrent_frames(self):
        a,b,c = self.peers
        def send(sock, label):
            for n in range(30): send_json_frame(sock, {'type':'MSG','id':str(uuid.uuid4()),'to':'bob','text':label+str(n)})
        threads = [threading.Thread(target=send,args=(a,'a')), threading.Thread(target=send,args=(c,'c'))]
        for t in threads:t.start()
        for _ in range(60): self.assertEqual(recv_json_frame(b)['type'],'MSG')
        for t in threads:t.join()

if __name__ == '__main__': unittest.main()
