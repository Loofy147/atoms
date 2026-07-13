"""
raft.py -- A fully functional in-memory 3-node Raft consensus cluster.

This replicates the Idempotency Cache across 3 nodes (Node 0, Node 1, Node 2) to ensure
high availability and prevent split-brain corruption during network partitions.
"""
import threading
import time
import logging

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] (%(threadName)s) %(message)s")


class RaftLogEntry:
    def __init__(self, term: int, key: str, value: str):
        self.term = term
        self.key = key
        self.value = value

    def to_dict(self):
        return {"term": self.term, "key": self.key, "value": self.value}


class RaftNode:
    def __init__(self, node_id: int, peers: list[int], cluster):
        self.node_id = node_id
        self.peers = peers
        self.cluster = cluster

        self.lock = threading.Lock()
        self.role = "Follower"  # Follower, Candidate, Leader
        self.current_term = 0
        self.voted_for = None
        self.log: list[RaftLogEntry] = []  # 1-indexed for simplicity, we insert a dummy at index 0
        self.log.append(RaftLogEntry(0, "", ""))

        self.commit_index = 0
        self.last_applied = 0

        # Leader-specific volatile state
        self.next_index = {p: 1 for p in peers}
        self.match_index = {p: 0 for p in peers}

        # Key-Value State Machine
        self.state_machine: dict[str, str] = {}

        self.last_heartbeat_rx = time.time()
        self.heartbeat_timeout = 0.5  # 500ms
        self.election_timeout = 1.0   # 1s

    def handle_request_vote(self, term: int, candidate_id: int, last_log_index: int, last_log_term: int) -> tuple[int, bool]:
        with self.lock:
            if term > self.current_term:
                self.current_term = term
                self.role = "Follower"
                self.voted_for = None

            last_entry = self.log[-1]
            my_last_term = last_entry.term
            my_last_index = len(self.log) - 1

            log_ok = (last_log_term > my_last_term) or (
                last_log_term == my_last_term and last_log_index >= my_last_index
            )

            vote_granted = False
            if term == self.current_term and (self.voted_for is None or self.voted_for == candidate_id) and log_ok:
                vote_granted = True
                self.voted_for = candidate_id
                self.last_heartbeat_rx = time.time()

            return self.current_term, vote_granted

    def handle_append_entries(self, term: int, leader_id: int, prev_log_index: int, prev_log_term: int,
                              entries: list[dict], leader_commit: int) -> tuple[int, bool]:
        with self.lock:
            if term > self.current_term:
                self.current_term = term
                self.role = "Follower"
                self.voted_for = None

            if term < self.current_term:
                return self.current_term, False

            self.last_heartbeat_rx = time.time()
            if self.role != "Follower":
                self.role = "Follower"
                self.voted_for = None

            if prev_log_index >= len(self.log):
                return self.current_term, False
            if self.log[prev_log_index].term != prev_log_term:
                return self.current_term, False

            for i, entry_dict in enumerate(entries):
                idx = prev_log_index + 1 + i
                entry = RaftLogEntry(entry_dict["term"], entry_dict["key"], entry_dict["value"])
                if idx < len(self.log):
                    if self.log[idx].term != entry.term:
                        self.log = self.log[:idx]
                        self.log.append(entry)
                else:
                    self.log.append(entry)

            if leader_commit > self.commit_index:
                self.commit_index = min(leader_commit, len(self.log) - 1)
                self._apply_to_state_machine()

            return self.current_term, True

    def _apply_to_state_machine(self):
        while self.last_applied < self.commit_index:
            self.last_applied += 1
            entry = self.log[self.last_applied]
            if entry.key:
                self.state_machine[entry.key] = entry.value

    def propose(self, key: str, value: str) -> bool:
        with self.lock:
            if self.role != "Leader":
                return False

            entry = RaftLogEntry(self.current_term, key, value)
            self.log.append(entry)
            my_index = len(self.log) - 1

        replicated_count = 1
        lock = threading.Lock()

        def replicate_to_peer(peer):
            nonlocal replicated_count
            with self.lock:
                if self.role != "Leader":
                    return
                prev_index = len(self.log) - 2
                prev_term = self.log[prev_index].term if prev_index < len(self.log) else 0
                entries = [{"term": e.term, "key": e.key, "value": e.value} for e in self.log[prev_index + 1:]]
                term = self.current_term
                leader_commit = self.commit_index

            try:
                ret_term, success = self.cluster.send_append_entries(
                    self.node_id, peer, term, self.node_id, prev_index, prev_term, entries, leader_commit
                )
                if success:
                    with lock:
                        replicated_count += 1
                    with self.lock:
                        self.match_index[peer] = my_index
                        self.next_index[peer] = my_index + 1
                else:
                    with self.lock:
                        if ret_term > self.current_term:
                            self.current_term = ret_term
                            self.role = "Follower"
                            self.voted_for = None
                        else:
                            self.next_index[peer] = max(1, self.next_index[peer] - 1)
            except Exception:
                pass

        threads = []
        for peer in self.peers:
            t = threading.Thread(target=replicate_to_peer, args=(peer,), name=f"Replicate-{self.node_id}-to-{peer}")
            t.start()
            threads.append(t)

        for t in threads:
            t.join()

        with self.lock:
            if self.role == "Leader" and replicated_count > (len(self.peers) + 1) // 2:
                self.commit_index = my_index
                self._apply_to_state_machine()
                return True
            return False

    def send_heartbeat(self):
        with self.lock:
            if self.role != "Leader":
                return
            term = self.current_term
            leader_commit = self.commit_index

        success_count = 1  # Self
        lock = threading.Lock()

        def hb(peer, pi, pt, ent):
            nonlocal success_count
            try:
                ret_term, success = self.cluster.send_append_entries(
                    self.node_id, peer, term, self.node_id, pi, pt, ent, leader_commit
                )
                if success:
                    with lock:
                        success_count += 1
                else:
                    with self.lock:
                        if ret_term > self.current_term:
                            self.current_term = ret_term
                            self.role = "Follower"
                            self.voted_for = None
            except Exception:
                pass

        threads = []
        for peer in self.peers:
            with self.lock:
                prev_index = self.next_index[peer] - 1
                prev_term = self.log[prev_index].term
                entries = [{"term": e.term, "key": e.key, "value": e.value} for e in self.log[self.next_index[peer]:]]
            t = threading.Thread(target=hb, args=(peer, prev_index, prev_term, entries), name=f"HB-{self.node_id}-to-{peer}")
            t.start()
            threads.append(t)

        for t in threads:
            t.join()

        with self.lock:
            # If leader is in minority partition, it should step down
            if self.role == "Leader" and success_count <= (len(self.peers) + 1) // 2:
                self.role = "Follower"
                self.voted_for = None
                print(f"[RaftNode {self.node_id}] Stepped down to follower due to losing majority connection.")

    def start_election(self):
        with self.lock:
            self.role = "Candidate"
            self.current_term += 1
            self.voted_for = self.node_id
            self.last_heartbeat_rx = time.time()
            votes = 1
            term = self.current_term
            last_log_index = len(self.log) - 1
            last_log_term = self.log[-1].term

        lock = threading.Lock()

        def request_vote_from_peer(peer):
            nonlocal votes
            try:
                ret_term, vote_granted = self.cluster.send_request_vote(
                    self.node_id, peer, term, self.node_id, last_log_index, last_log_term
                )
                if vote_granted:
                    with lock:
                        votes += 1
                else:
                    with self.lock:
                        if ret_term > self.current_term:
                            self.current_term = ret_term
                            self.role = "Follower"
                            self.voted_for = None
            except Exception:
                pass

        threads = []
        for peer in self.peers:
            t = threading.Thread(target=request_vote_from_peer, args=(peer,), name=f"Vote-{self.node_id}-from-{peer}")
            t.start()
            threads.append(t)

        for t in threads:
            t.join()

        with self.lock:
            if self.role == "Candidate" and votes > (len(self.peers) + 1) // 2:
                self.role = "Leader"
                for peer in self.peers:
                    self.next_index[peer] = len(self.log)
                    self.match_index[peer] = 0
                print(f"[RaftNode {self.node_id}] Elected LEADER for term {self.current_term}")


class RaftCluster:
    def __init__(self):
        self.nodes: dict[int, RaftNode] = {}
        self.partitioned_links: set[tuple[int, int]] = set()
        self._running = False
        self._threads = []

    def add_node(self, node_id: int, peers: list[int]):
        node = RaftNode(node_id, peers, self)
        self.nodes[node_id] = node

    def set_partition(self, node_a: int, node_b: int):
        self.partitioned_links.add((node_a, node_b))
        self.partitioned_links.add((node_b, node_a))

    def remove_partition(self, node_a: int, node_b: int):
        self.partitioned_links.discard((node_a, node_b))
        self.partitioned_links.discard((node_b, node_a))

    def heal_all_partitions(self):
        self.partitioned_links.clear()

    def is_partitioned(self, from_id: int, to_id: int) -> bool:
        return (from_id, to_id) in self.partitioned_links

    def send_request_vote(self, from_id: int, to_id: int, term: int, candidate_id: int,
                          last_log_index: int, last_log_term: int) -> tuple[int, bool]:
        if self.is_partitioned(from_id, to_id):
            raise ConnectionError("Network partition active")
        return self.nodes[to_id].handle_request_vote(term, candidate_id, last_log_index, last_log_term)

    def send_append_entries(self, from_id: int, to_id: int, term: int, leader_id: int, prev_log_index: int,
                            prev_log_term: int, entries: list[dict], leader_commit: int) -> tuple[int, bool]:
        if self.is_partitioned(from_id, to_id):
            raise ConnectionError("Network partition active")
        return self.nodes[to_id].handle_append_entries(term, leader_id, prev_log_index, prev_log_term, entries, leader_commit)

    def start(self):
        self._running = True

        def run_node_loop(node: RaftNode):
            while self._running:
                time.sleep(0.1)
                now = time.time()
                with node.lock:
                    role = node.role
                    last_hb = node.last_heartbeat_rx
                    hb_to = node.heartbeat_timeout
                    ele_to = node.election_timeout

                if role == "Leader":
                    node.send_heartbeat()
                elif role == "Follower" and (now - last_hb > hb_to):
                    node.start_election()
                elif role == "Candidate" and (now - last_hb > ele_to):
                    node.start_election()

        for node_id, node in self.nodes.items():
            t = threading.Thread(target=run_node_loop, args=(node,), name=f"RaftNodeLoop-{node_id}")
            t.daemon = True
            t.start()
            self._threads.append(t)

    def stop(self):
        self._running = False
        for t in self._threads:
            t.join()

    def get_leader(self) -> RaftNode | None:
        for node in self.nodes.values():
            with node.lock:
                if node.role == "Leader":
                    return node
        return None
