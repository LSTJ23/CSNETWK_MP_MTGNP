import sys
import json
import socket
import struct
import threading
import time
import random

from card_catalog import load_card_catalog

MAX_PDU_SIZE = 65535
DEFAULT_HOST = "127.0.0.1"
DEFAULT_PORT = 4444
DEFAULT_PRIORITY_TIMEOUT_MS = 60000

PHASE_SEQUENCES = [
    "UNTAP", "UPKEEP", "DRAW", "PRECOMBAT_MAIN", 
    "BEGIN_COMBAT", "DECLARE_ATTACKERS", "DECLARE_BLOCKERS", "ASSIGN_DAMAGE_ORDER", "COMBAT_DAMAGE", "END_OF_COMBAT", 
    "POSTCOMBAT_MAIN", "END_STEP", "CLEANUP"
]

def send_pdu(sock: socket.socket, payload: dict) -> None:
    """Encodes JSON payload and prefixes it with a 4-byte big-endian header."""
    json_bytes = json.dumps(payload).encode('utf-8')
    if len(json_bytes) > MAX_PDU_SIZE:
        raise ValueError(f"Payload size {len(json_bytes)} exceeds MAX_PDU_SIZE ({MAX_PDU_SIZE}).")
    header = struct.pack(">I", len(json_bytes))
    
    try:
        sock.sendall(header + json_bytes)
    except (ConnectionAbortedError, ConnectionResetError, BrokenPipeError, OSError) as e:
        raise ConnectionResetError(f"Connection aborted while sending: {e}")

def recv_exact(sock: socket.socket, length: int) -> bytes:
    """Reads exactly `length` bytes from a TCP socket."""
    buf = bytearray()
    while len(buf) < length:
        try:
            chunk = sock.recv(length - len(buf))
        except (ConnectionAbortedError, ConnectionResetError, BrokenPipeError, OSError) as e:
            raise ConnectionResetError(f"Connection aborted while receiving: {e}")
            
        if not chunk:
            raise ConnectionResetError("Connection closed while receiving data.")
        buf.extend(chunk)
    return bytes(buf)

def recv_pdu(sock: socket.socket) -> dict:
    header = recv_exact(sock, 4)
    length = struct.unpack(">I", header)[0]
    if length > MAX_PDU_SIZE:
        raise ValueError(f"Received PDU header specifies size {length} > MAX_PDU_SIZE.")
    payload_bytes = recv_exact(sock, length)
    return json.loads(payload_bytes.decode('utf-8'))


class MTGNPServer:
    def __init__(self, host=DEFAULT_HOST, port=DEFAULT_PORT):
        self.host = host
        self.port = port
        self.server_seq_num = 1
        self.seq_lock = threading.Lock()
        
        try:
            self.card_catalog, self.base_cards = load_card_catalog("mtgnp_master_card_list.xlsx")
            print(f"[SERVER] Loaded {len(self.card_catalog)} valid card instances and {len(self.base_cards)} base cards.")
        except Exception as e:
            print(f"[SERVER WARNING] Failed to load card catalog: {e}")
            self.card_catalog, self.base_cards = set(), {}  # Set to None or populate with allowed card IDs
        self.state = "LOBBY"       # LOBBY, IN_GAME, GAME_OVER
        self.players = []          # [{"id": "player_1", "socket": sock, "addr": addr}]
        self.ready_players = {}    # Maps conn_id -> {"player_id": str, "deck_list": list}
        self.lock = threading.RLock()
        
        self.current_priority_player = None
        self.current_priority_seq = 0
        self.consecutive_passes = 0
        self.priority_timer = None
        
        self.rematch_votes = {}
        self.reset_game_state()
        
        self.engine = GameEngine(self.game_state, self.base_cards)

    def validate_deck(self, deck_list):
        """Validates deck size and card legality."""
        deck_size = len(deck_list)
        
        if not(1 <= deck_size <= 50):
            return False, f"Invalid deck size: expected 1 to 50, got {len(deck_list) if isinstance(deck_list, list) else 'invalid format'}"
        
        for card_id in deck_list:
            if card_id not in self.card_catalog:
                return False, f"Invalid card_id in deck: '{card_id}' is not in the master catalog."
                
        return True, "Deck valid"
    
    def handle_player_ready(self, player_info: dict, pdu: dict):
        """Validates deck size and player ID uniqueness before marking player ready."""
        sock = player_info["socket"]
        conn_id = player_info["id"]
        
        player_id = str(pdu.get("player_id", "")).strip()
        deck_list = pdu.get("deck_list", [])

        # Call the validation method
        is_valid, reason = self.validate_deck(deck_list)
        if not is_valid:
            print(f"[SERVER] Deck validation failed for {player_id}: {reason}")
            send_pdu(sock, {
                "type": "ERROR",
                "seq_num": self.get_next_seq(),
                "code": "ILLEGAL_DECK",
                "message": f"PLAYER_READY rejected: {reason}"
            })
            return

        # If valid, proceed with saving the player state
        with self.lock:
            self.ready_players[conn_id] = {
                "player_id": player_id,
                "deck": list(deck_list),
                "hand": [],
                "battlefield": [],
                "graveyard": [],
                "life": 20
            }
            print(f"[SERVER] Player {player_id} submitted a valid 60-card deck and is ready.")
            
    def check_win_conditions(self):
        """
        Checks game-ending conditions according to RFC rules:
        1. Player life <= 0 (Loss)
        2. Player library/deck is empty when attempting to draw (Deckout Loss)
        """
        if len(self.ready_players) < 2:
            return False, None, ""

        players = list(self.ready_players.values())
        p1, p2 = players[0], players[1]

        # 1. Life total checks
        if p1["life"] <= 0 and p2["life"] <= 0:
            return True, "DRAW", "Both players hit 0 life simultaneously."
        elif p1["life"] <= 0:
            return True, p2["player_id"], f"Player {p1['player_id']} reached 0 life."
        elif p2["life"] <= 0:
            return True, p1["player_id"], f"Player {p2['player_id']} reached 0 life."

        # 2. Empty library checks (Deckout)
        if len(p1["deck"]) == 0 and len(p1["hand"]) == 0 and len(p1["battlefield"]) == 0:
            return True, p2["player_id"], f"Player {p1['player_id']} has no remaining cards to play or draw."
        if len(p2["deck"]) == 0 and len(p2["hand"]) == 0 and len(p2["battlefield"]) == 0:
            return True, p1["player_id"], f"Player {p2['player_id']} has no remaining cards to play or draw."

        return False, None, ""         
            
    def reset_to_lobby(self):
        """Resets game state back to LOBBY while retaining TCP connections."""
        if self.priority_timer:
            self.priority_timer.cancel()
        self.state = "LOBBY"
        self.ready_players.clear()
        self.game_state["stack"].clear()
        self.consecutive_passes = 0
        print("[SERVER] Server returned to LOBBY state. Awaiting PLAYER_READY PDUs.")

    def reset_game_state(self):
        """Initializes or resets authoritative game state for a new match."""
        self.game_state = {
            "phase": "LOBBY",
            "step": "NONE",
            "active_player": "player_1",
            "stack": [],
            "players": {}
        }
        self.consecutive_passes = 0
        self.rematch_votes.clear()
        if hasattr(self, 'engine'):
            self.engine.game_state = self.game_state

    def get_next_seq(self) -> int:
        with self.seq_lock:
            seq = self.server_seq_num
            self.server_seq_num += 1
            return seq

    def start(self):
        server_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        # Bind to 0.0.0.0 to accept IPv4, IPv6 localhost, and LAN connections
        server_sock.bind(("0.0.0.0", self.port))
        server_sock.listen(5)
        print(f"[SERVER] Listening on 0.0.0.0:{self.port}...", flush=True)

        while True:
            try:
                conn, addr = server_sock.accept()
            except OSError:
                break

            with self.lock:
                if len(self.players) >= 2:
                    print(f"[SERVER] Connection attempt from {addr} rejected: Server full.", flush=True)
                    try:
                        conn.close()
                    except Exception:
                        pass
                    continue

                player_id = f"player_{len(self.players) + 1}"
                player_info = {"id": player_id, "socket": conn, "addr": addr}
                self.players.append(player_info)
                print(f"[SERVER] {player_id} connected from {addr}. Total connected: {len(self.players)}", flush=True)

                t = threading.Thread(target=self.handle_client, args=(player_info,), daemon=True)
                t.start()

    def handle_player_ready(self, player_info: dict, pdu: dict):
        """Validates deck size and player ID uniqueness before marking player ready."""
        sock = player_info["socket"]
        conn_id = player_info["id"]
        
        player_id = str(pdu.get("player_id", "")).strip()
        deck_list = pdu.get("deck_list", [])

        if not player_id:
            send_pdu(sock, {
                "type": "ERROR",
                "seq_num": self.get_next_seq(),
                "code": "INVALID_ID",
                "message": "player_id must be a non-empty string."
            })
            return

        for existing_conn_id, ready_data in self.ready_players.items():
            if existing_conn_id != conn_id and ready_data["player_id"] == player_id:
                send_pdu(sock, {
                    "type": "ERROR",
                    "seq_num": self.get_next_seq(),
                    "code": "DUPLICATE_ID",
                    "message": f"Player ID '{player_id}' is already claimed by the other connected player."
                })
                return

        if len(deck_list) < 1 or len(deck_list) > 50:
            send_pdu(sock, {
                "type": "ERROR",
                "seq_num": self.get_next_seq(),
                "code": "ILLEGAL_DECK",
                "message": f"Deck must contain between 1 and 50 cards; got {len(deck_list)}."
            })
            return

        if self.card_catalog:
            invalid_cards = [card for card in deck_list if card not in self.card_catalog]
            if invalid_cards:
                send_pdu(sock, {
                    "type": "ERROR",
                    "seq_num": self.get_next_seq(),
                    "code": "ILLEGAL_DECK",
                    "message": f"Deck contains unrecognized cards: {invalid_cards}"
                })
                return

        self.ready_players[conn_id] = {
            "player_id": player_id,
            "deck_list": deck_list
        }

        ready_count = len(self.ready_players)
        waiting_for = []
        if ready_count < 2:
            waiting_for = ["player_2"] if conn_id == "player_1" else ["player_1"]

        send_pdu(sock, {
            "type": "GAME_STATE_UPDATE",
            "seq_num": self.get_next_seq(),
            "state": {
                "phase": "LOBBY",
                "players_ready": ready_count,
                "waiting_for": waiting_for
            }
        })

        if ready_count == 2:
            self.execute_game_setup()

    def execute_game_setup(self):
        """Executes deck shuffling, card drawing, life initialization, and coin flip."""
        self.state = "GAME_SETUP"
        conn_ids = list(self.ready_players.keys())
        p1_conn, p2_conn = conn_ids[0], conn_ids[1]
        
        p1_id = self.ready_players[p1_conn]["player_id"]
        p2_id = self.ready_players[p2_conn]["player_id"]

        p1_deck = list(self.ready_players[p1_conn]["deck_list"])
        p2_deck = list(self.ready_players[p2_conn]["deck_list"])
        random.shuffle(p1_deck)
        random.shuffle(p2_deck)

        p1_hand, p1_library = p1_deck[:7], p1_deck[7:]
        p2_hand, p2_library = p2_deck[:7], p2_deck[7:]

        starting_player = random.choice([p1_id, p2_id])

        self.game_state = {
            "turn": 1,
            "phase": "MULLIGAN",
            "step": "MAIN",
            "active_player": starting_player,

            "priority_holder": None,

            "life_totals": {p1_id: 20, p2_id: 20},
            "hands": {p1_id: p1_hand, p2_id: p2_hand},
            "libraries": {p1_id: p1_library, p2_id: p2_library},
            "battlefield": {p1_id: [], p2_id: []},
            "graveyard": {p1_id: [], p2_id: []},
            "stack": [],
            "land_cast_on_turn": False,
            "combat": {
                "attackers": [],
                "blockers": {},
                "damage_order": []
            }
        }
        self.engine.game_state = self.game_state

        self.mulligan_state = {
            p1_id: {"kept": False, "count": 0, "expected_seq": None},
            p2_id: {"kept": False, "count": 0, "expected_seq": None}
        }
        self.state = "MULLIGAN"

        for p in self.players:
            self.send_game_state_update(p)
    
    def handle_mulligan_choice(self, player_info: dict, pdu: dict):
        sock = player_info["socket"]
        conn_id = player_info["id"]
        pid = self.ready_players.get(conn_id, {}).get("player_id")

        if self.state != "MULLIGAN":
            send_pdu(sock, {
                "type": "ERROR",
                "seq_num": pdu.get("seq_num", self.get_next_seq()),
                "code": "WRONG_PHASE",
                "message": "MULLIGAN_CHOICE can only be sent during MULLIGAN phase."
            })
            return

        m_state = self.mulligan_state.get(pid)
        if not m_state or m_state["kept"]:
            # Player already kept, ignore further mulligan choices
            return

        client_seq = pdu.get("seq_num")
        if client_seq != m_state.get("expected_seq"):
            send_pdu(sock, {
                "type": "ERROR",
                "seq_num": self.get_next_seq(),
                "code": "STALE_ACTION",
                "message": f"Sequence mismatch. Expected {m_state.get('expected_seq')}, got {client_seq}.",
                "rejected_action": pdu
            })
            return
        
        keep = pdu.get("keep", True)
        cards_to_bottom = pdu.get("cards_to_bottom", [])

        if not keep:
            # Perform mulligan: shuffle hand, and redraw 7
            m_state["count"] += 1
            self.game_state["libraries"][pid].extend(self.game_state["hands"][pid])
            self.game_state["hands"][pid].clear()
            random.shuffle(self.game_state["libraries"][pid])
            
            for _ in range(7):
                if self.game_state["libraries"][pid]:
                    drawn = self.game_state["libraries"][pid].pop(0)
                    self.game_state["hands"][pid].append(drawn)
                    
            print(f"[SERVER] {pid} took a mulligan. Mulligan count: {m_state['count']}")
            self.send_game_state_update(player_info)
        else:
            # Keep hand: confirm bottomed cards
            if len(cards_to_bottom) != m_state["count"]:
                send_pdu(sock, {
                    "type": "ERROR",
                    "seq_num": pdu.get("seq_num", self.get_next_seq()),
                    "code": "ILLEGAL_ACTION",
                    "message": f"You must bottom exactly {m_state['count']} cards."
                })
                return
            
            hand = self.game_state["hands"][pid]
            for card in cards_to_bottom:
                if card not in hand:
                    send_pdu(sock, {
                        "type": "ERROR",
                        "seq_num": pdu.get("seq_num", self.get_next_seq()),
                        "code": "ILLEGAL_ACTION",
                        "message": f"Card '{card}' is not in your hand."
                    })
                    return
            
            # Remove cards and place said cards in bottom queue of library
            for card in cards_to_bottom:
                hand.remove(card)
                self.game_state["libraries"][pid].append(card)

            m_state["kept"] = True
            print(f"[SERVER] {pid} kept their hand.")
            
            self.send_game_state_update(player_info)

            # Confirm if both players have kept
            if all(s["kept"] for s in self.mulligan_state.values()):
                self.start_game_proper()
                
    def start_game_proper(self):
        """Transitions from MULLIGAN to IN_GAME and starts the first turn."""
        self.state = "IN_GAME"
        self.game_state["turn"] = 1
        
        print("[SERVER] Both players kept. Beginning Turn 1.")
        
        # Broadcast transition to Untap Step
        self.game_state["phase"] = "UNTAP"
        self.game_state["step"] = "NONE"
        
        for p in self.players:
            self.send_game_state_update(p)
        
        self.broadcast({
            "type": "PHASE_TRANSITION",
            "seq_num": self.get_next_seq(),
            "from_phase": "MULLIGAN",
            "to_phase": "UNTAP",
            "active_player": self.game_state["active_player"],
            "turn": self.game_state["turn"]
        })
        
        # Execute Untap Step logic
        self.engine.untap_step(self.game_state["active_player"])
        
        # Transition immediately to Upkeep Step
        self.transition_phase("UPKEEP", "NONE")

    def send_game_state_update(self, player_info):
        """Formats personalized GAME_STATE_UPDATE per Section 6.3 schema."""
        conn_id = player_info["id"]
        pid = self.ready_players.get(conn_id, {}).get("player_id", conn_id)
        all_pids = [r["player_id"] for r in self.ready_players.values()]
        opp_id = next((p for p in all_pids if p != pid), "opponent")

        # Captures seq before assigning it to the PDU
        current_seq = self.get_next_seq()
        
        # Tracks seq to enforce STALE_ACTION for mulligans
        if self.state == "MULLIGAN" and pid in self.mulligan_state:
            self.mulligan_state[pid]["expected_seq"] = current_seq
        
        state_pdu = {
            "type": "GAME_STATE_UPDATE",
            "seq_num": current_seq,
            "state": {
                "turn": self.game_state["turn"],
                "phase": self.game_state["phase"],
                "active_player": self.game_state["active_player"],
                "life_totals": self.game_state["life_totals"],
                "hand": self.game_state["hands"].get(pid, []),
                "hand_counts": {opp_id: len(self.game_state["hands"].get(opp_id, []))},
                "library_counts": {p: len(self.game_state["libraries"].get(p, [])) for p in all_pids},
                "battlefield": self.game_state["battlefield"],
                "graveyard": self.game_state["graveyard"],
                "stack": self.game_state["stack"]
            }
        }
        try:
            send_pdu(player_info["socket"], state_pdu)
        except ConnectionResetError:
            print(f"[SERVER] Failed to send state update to {pid}: Connection aborted.")

    def broadcast(self, pdu_template: dict):
        for p in self.players:
            pdu = dict(pdu_template)
            pdu["seq_num"] = self.get_next_seq()
            try:
                send_pdu(p["socket"], pdu)
            except Exception:
                pass

    def grant_priority(self, player_id: str, timeout_ms: int = DEFAULT_PRIORITY_TIMEOUT_MS):
        self.current_priority_player = player_id
        self.current_priority_seq = self.get_next_seq()

        pdu = {
            "type": "PRIORITY_GRANT",
            "seq_num": self.current_priority_seq,
            "player_id": player_id,
            "time_limit_ms": timeout_ms
        }
        
        target_conn = None
        for cid, rdata in self.ready_players.items():
            if rdata["player_id"] == player_id:
                target_conn = next((p for p in self.players if p["id"] == cid), None)
                break

        if target_conn:
            try:
                send_pdu(target_conn["socket"], pdu)
            except ConnectionResetError:
                print(f"[SERVER] Failed to grant priority to {player_id}: Connection aborted.")
                self.trigger_game_over(next((p for p in self.ready_players.values() if p["player_id"] != player_id), {}).get("player_id", "UNKNOWN"), "DISCONNECT")
                return

        if self.priority_timer:
            self.priority_timer.cancel()
        
        self.priority_timer = threading.Timer(
            timeout_ms / 1000.0, 
            self.handle_priority_timeout, 
            args=[player_id]
        )
        self.priority_timer.start()

    def handle_priority_timeout(self, timed_out_player_id: str):
        with self.lock:
            print(f"[SERVER] Priority timeout by {timed_out_player_id}.")
            winner = next(p["id"] for p in self.players if p["id"] != timed_out_player_id)
            self.trigger_game_over(winner, "DISCONNECT")

    def transition_phase(self, new_phase: str, new_step: str):
        self.game_state["phase"] = new_phase
        self.game_state["step"] = new_step
        self.consecutive_passes = 0
        
        for p in self.players:
            self.send_game_state_update(p)
        
        self.broadcast({
            "type": "PHASE_TRANSITION",
            "phase": new_phase,
            "step": new_step
        })
        self.grant_priority(self.game_state["active_player"])

    def trigger_game_over(self, winner_id: str, reason: str):
        with self.lock:
            if self.state == "GAME_OVER":
                return
            self.state = "GAME_OVER"
            if self.priority_timer:
                self.priority_timer.cancel()

            self.broadcast({
                "type": "GAME_OVER",
                "seq_num": self.get_next_seq(),
                "winner": winner_id,
                "reason": reason
            })

            self.rematch_votes.clear()
            self.broadcast({
                "type": "REMATCH_REQUEST",
                "seq_num": self.get_next_seq(),
                "message": "Game over! Would you like to play a rematch? (yes/no)"
            })

    def handle_rematch_response(self, player_id: str, accepted: bool):
        with self.lock:
            if self.state != "GAME_OVER":
                return

            self.rematch_votes[player_id] = accepted
            print(f"[SERVER] Rematch vote from {player_id}: {'Accepted' if accepted else 'Declined'}")

            if len(self.rematch_votes) == 2 or not accepted:
                both_agreed = len(self.rematch_votes) == 2 and all(self.rematch_votes.values())
                
                self.broadcast({
                    "type": "REMATCH_RESULT",
                    "accepted": both_agreed,
                    "message": "Both players agreed! Starting rematch..." if both_agreed else "Rematch declined. Shutting down."
                })

                if both_agreed:
                    self.reset_to_lobby()
                else:
                    self.close_server()

    def close_server(self):
        with self.lock:
            for p in self.players:
                try:
                    p["socket"].close()
                except Exception:
                    pass
            self.players.clear()
            sys.exit(0)

    def handle_client(self, player_info: dict):
        sock = player_info["socket"]
        pid = player_info["id"]

        try:
            while True:
                try:
                    pdu = recv_pdu(sock)
                except (ConnectionAbortedError, ConnectionResetError, BrokenPipeError, struct.error, ValueError, OSError) as e:
                    print(f"[SERVER] Connection lost/aborted for {pid}: {e}", flush=True)
                    if self.state == "GAME_OVER":
                        self.handle_rematch_response(pid, False)
                    elif self.state in ["IN_GAME", "GAME_SETUP"]:
                        all_ids = [r["player_id"] for r in self.ready_players.values()]
                        current_pid = self.ready_players.get(pid, {}).get("player_id", pid)
                        winner = next((p for p in all_ids if p != current_pid), "UNKNOWN")
                        self.trigger_game_over(winner, "DISCONNECT")
                    break

                pdu_type = pdu.get("type")
                client_seq = pdu.get("seq_num")

                if pdu_type == "PLAYER_READY":
                    if self.state != "LOBBY":
                        try:
                            send_pdu(sock, {
                                "type": "ERROR",
                                "seq_num": self.get_next_seq(),
                                "code": "INVALID_PHASE",
                                "message": "PLAYER_READY can only be sent during LOBBY phase."
                            })
                        except ConnectionResetError:
                            pass
                    else:
                        self.handle_player_ready(player_info, pdu)
                    continue
                
                if pdu_type == "MULLIGAN_CHOICE":
                    with self.lock:
                        self.handle_mulligan_choice(player_info, pdu)
                    continue
                
                if pdu_type == "PING":
                    try:
                        send_pdu(sock, {"type": "PONG", "seq_num": client_seq})
                    except ConnectionResetError:
                        pass
                    continue

                if pdu_type == "CONCEDE":
                    all_ids = [r["player_id"] for r in self.ready_players.values()]
                    current_pid = self.ready_players.get(pid, {}).get("player_id", pid)
                    winner = next((p for p in all_ids if p != current_pid), "UNKNOWN")
                    self.trigger_game_over(winner, "CONCEDE")
                    continue

                if pdu_type == "REMATCH_RESPONSE":
                    self.handle_rematch_response(pid, pdu.get("accepted", False))
                    continue

                with self.lock:
                    current_pid = self.ready_players.get(pid, {}).get("player_id")
                    if current_pid != self.current_priority_player or client_seq != self.current_priority_seq:
                        try:
                            send_pdu(sock, {
                                "type": "ERROR",
                                "seq_num": self.get_next_seq(),
                                "code": "STALE_ACTION",
                                "message": f"Priority token mismatch. Expected {self.current_priority_seq}, got {client_seq}.",
                                "rejected_action": pdu
                            })
                        except ConnectionResetError:
                            pass
                        if current_pid == self.current_priority_player:
                            self.grant_priority(current_pid)
                        continue

                    if self.priority_timer:
                        self.priority_timer.cancel()

                    if pdu_type == "PRIORITY_PASS":
                        self.consecutive_passes += 1
                        
                        if self.consecutive_passes >= 2:
                            self.consecutive_passes = 0
                            
                            # 1. If stack has spells, resolve the top item first
                            if self.game_state["stack"]:
                                resolved_item = self.engine.resolve_stack()
                                print(f"[SERVER] Resolved stack object: {resolved_item}")
                                
                                for p in self.players:
                                    self.send_game_state_update(p)
                                
                                self.grant_priority(self.game_state["active_player"])
                            else:
                                # 2. If stack is empty, transition to COMBAT
                                self.transition_phase("COMBAT", "DECLARE_ATTACKERS")
                        else:
                            all_pids = [r["player_id"] for r in self.ready_players.values()]
                            next_player = next(p for p in all_pids if p != current_pid)
                            self.grant_priority(next_player)

                    elif pdu_type == "CAST_SPELL":
                        card_id = pdu.get("card_id")
                        
                        # 1. Determine if card is a Land or Spell
                        base_card_name = "_".join(card_id.split("_")[:-1]) if card_id else ""
                        card_info = self.base_cards.get(base_card_name, {})
                        card_type = str(card_info.get("type", "")).upper()
                        
                        is_land = card_type == "LAND" or base_card_name in ["mountain", "forest", "island", "swamp", "plains"]

                        # 2. Process Land vs Spell using GameEngine
                        if is_land:
                            success = self.engine.play_land(current_pid, card_id)
                            if not success:
                                send_pdu(sock, {
                                    "type": "ERROR",
                                    "seq_num": self.get_next_seq(),
                                    "code": "ILLEGAL_ACTION",
                                    "message": f"Cannot play land '{card_id}' (not in hand or land already played this turn)."
                                })
                                self.grant_priority(current_pid)
                                continue
                        else:
                            success = self.engine.cast_spell(current_pid, card_id)
                            if not success:
                                send_pdu(sock, {
                                    "type": "ERROR",
                                    "seq_num": self.get_next_seq(),
                                    "code": "ILLEGAL_ACTION",
                                    "message": f"Cannot cast spell '{card_id}' (not in hand)."
                                })
                                self.grant_priority(current_pid)
                                continue

                        # 3. Broadcast updated game state and pass priority back
                        self.consecutive_passes = 0
                        for p in self.players:
                            self.send_game_state_update(p)
                        self.grant_priority(current_pid)

        finally:
            # Always clean up player state upon thread exit
            with self.lock:
                if player_info in self.players:
                    self.players.remove(player_info)
                if pid in self.ready_players:
                    del self.ready_players[pid]
                try:
                    sock.close()
                except Exception:
                    pass
                print(f"[SERVER] Cleaned up session for {pid}. Active connections: {len(self.players)}", flush=True)

class GameEngine:

    # initialize game engine with game state and card catalog
    def __init__(self, game_state, card_catalog):
        """
        game_state:
            The server's authoritative game state.

        card_catalog:
            Dictionary containing all card information.
        """

        self.game_state = game_state
        self.card_catalog = card_catalog

    # draw a card from the player's library to their hand
    def draw_card(self, player_id):

        # initialize the library for the player if it doesn't exist
        library = self.game_state["libraries"][player_id]

        # check if the library is empty
        if len(library) == 0:
            return False

        # draw the top card from the library and add it to the player's hand
        card = library.pop(0)

        # add the drawn card to the player's hand
        self.game_state["hands"][player_id].append(card)

        return True

    # play a land card from the player's hand to the battlefield
    def play_land(self, player_id, card_id):

        # player hand
        hand = self.game_state["hands"][player_id]

        # battlefield
        battlefield = self.game_state["battlefield"][player_id]

        # check if the card is in the player's hand in the first place
        if card_id not in hand:
            return False

        # Extract base card name (e.g. 'mountain_001' -> 'mountain')
        base_card_name = "_".join(card_id.split("_")[:-1]) if card_id else ""
        card = self.card_catalog.get(base_card_name, {})
        
        # check if the card is a land card
        if card.get("type", "").upper() != "LAND" and base_card_name not in ["mountain", "forest", "island", "swamp", "plains"]:
            return False

        # check if the player has already played a land this turn
        if self.game_state["land_cast_on_turn"]:
            return False

        # remove from hand and add to battlefield
        hand.remove(card_id)

        # add to battlefield with tapped status
        battlefield.append({

            "card": card_id,

            "tapped": False
        })

        # mark that the player has played a land this turn
        self.game_state["land_cast_on_turn"] = True

        return True

    # cast a spell from the player's hand to the stack
    def cast_spell(self, player_id, card_id):

        # player hand
        hand = self.game_state["hands"][player_id]

        # check if the card is in the player's hand
        if card_id not in hand:
            return False

        # remove from hand and add to stack
        hand.remove(card_id)

        self.game_state["stack"].append({

            "controller": player_id,

            "card": card_id
        })

        return True

    # Resolves spells on the stack, applies card effects, and moves cards to the graveyard
    def resolve_stack(self):
        """Resolves the top spell on the stack and applies its card effect."""
        if not self.game_state["stack"]:
            print("[GAME ENGINE] Stack is empty; nothing to resolve.")
            return None

        # Pop top spell off the FILO stack
        top_item = self.game_state["stack"].pop()
        
        # Determine controller and card_id
        if isinstance(top_item, dict):
            controller = top_item.get("controller")
            card_id = top_item.get("card")
        else:
            controller = self.game_state.get("active_player")
            card_id = top_item

        base_card_name = "_".join(card_id.split("_")[:-1]) if card_id else ""

        # Identify opponent ID
        all_pids = list(self.game_state["life_totals"].keys())
        opponent = next((p for p in all_pids if p != controller), None)

        print(f"[GAME ENGINE] Resolving '{card_id}' cast by {controller}...")

        # --- CARD RESOLUTION EFFECTS ---
        
        # 1. Direct Damage Spells
        if base_card_name in ["lightning_bolt", "shock"]:
            damage = 3 if base_card_name == "lightning_bolt" else 2
            if opponent:
                self.game_state["life_totals"][opponent] -= damage
                print(f"[GAME ENGINE] {base_card_name.upper()} dealt {damage} damage to {opponent}. Life remaining: {self.game_state['life_totals'][opponent]}")

        # 2. Counterspell (Target top spell beneath it on stack)
        elif base_card_name == "counterspell":
            if self.game_state["stack"]:
                countered = self.game_state["stack"].pop()
                
                if isinstance(countered, dict):
                    countered_card = countered.get("card")
                    countered_controller = countered.get("controller", opponent)
                else:
                    countered_card = countered
                    countered_controller = opponent
                
                # Move countered spell to its controller's graveyard
                if countered_controller and countered_card:
                    self.game_state["graveyard"][countered_controller].append(countered_card)
                print(f"[GAME ENGINE] COUNTERSPELL countered '{countered_card}'!")
            else:
                print(f"[GAME ENGINE] COUNTERSPELL fizzled (no target spell on stack).")

        # 3. Utility / Buff Spells
        elif base_card_name == "giant_growth":
            print(f"[GAME ENGINE] GIANT GROWTH resolved for {controller}. Target creature gains +3/+3 until end of turn.")

        elif base_card_name == "dark_ritual":
            print(f"[GAME ENGINE] DARK RITUAL resolved for {controller}. Added 3 Black Mana to mana pool.")

        # --- CLEANUP: Move the resolved spell (e.g. Counterspell/Lightning Bolt) to Graveyard ---
        if controller and card_id:
            self.game_state["graveyard"][controller].append(card_id)

        return top_item

    def untap_step(self, player_id):
        """Handles the untap step for the given player."""
        battlefield = self.game_state["battlefield"][player_id]
        for permanent in battlefield:
            if isinstance(permanent, dict):
                permanent["tapped"] = False
        print(f"[GAME ENGINE] Untap step completed for {player_id}. All permanents untapped.")

    def resolve_creature(self, stack_item):
        """Resolves a creature spell from the stack and places it onto the battlefield."""

        player_id = stack_item["controller"]
        card_id = stack_item["card"]

        card = self.card_catalog.get(card_id)

        if not card or card.get("type", "").upper() != "CREATURE":
            print(f"[GAME ENGINE] Cannot resolve '{card_id}': Not a creature.")
            return False

        self.game_state["battlefield"][player_id].append({
            "card": card_id,
            "tapped": False,
            "damage": 0,
            "power": card.get("power", 0), # .get("power", 0) to handle missing power. card["power"] would raise KeyError if missing
            "toughness": card.get("toughness", 0),
            "summoning_sickness": True
        })

        return True

    def declare_attackers(self, player_id, attackers):
        """Handles the declaration of attackers for the given player."""

        if self.game_state["phase"] != "DECLARE_ATTACKERS":
            return False
        
        if player_id != self.game_state["active_player"]:
            return False
        
        battlefield = self.game_state["battlefield"][player_id]

        valid_attackers = [perm for perm in battlefield if perm["card"] in attackers 
                           and not perm.get("tapped", False) 
                           and not perm.get("summoning_sickness", False)]

        # if the number of valid attackers does not match the number of declared attackers, reject the action
        # valid attackers are those that are untapped and do not have summoning sickness
        # valid attackers must not equal the number of declared attackers, 
        # otherwise it means some declared attackers are invalid because they are either tapped or have summoning sickness
        if len(valid_attackers) != len(attackers):
            print(f"[GAME ENGINE] Invalid attackers declared by {player_id}.")
            return False

        for perm in valid_attackers:
            perm["tapped"] = True

        self.game_state["combat"]["attackers"] = [perm["card"] for perm in valid_attackers]
        print(f"[GAME ENGINE] {player_id} declared attackers: {[perm['card'] for perm in valid_attackers]}")
        return True

    def declare_blockers(self, player_id, blockers):
        """Handles the declaration of blockers for the given player."""

        if self.game_state["phase"] != "DECLARE_BLOCKERS":
            return False
        
        all_pids = list(self.game_state["life_totals"].keys())
        opponent = next((p for p in all_pids if p != player_id), None)

        if not opponent:
            return False

        battlefield = self.game_state["battlefield"][player_id]
        attackers = self.game_state["combat"]["attackers"]

        # Keep track of which blockers have already been assigned to ensure no duplicates
        used_blockers = set()

        for declaration in blockers:
            blocker_card = declaration.get("blocker")
            attacker_card = declaration.get("attacker")

            # Validate that the blocker is on the battlefield and not tapped
            valid_blocker = next((perm for perm in battlefield if perm["card"] == blocker_card and not perm.get("tapped", False)), None)
            if not valid_blocker:
                print(f"[GAME ENGINE] Invalid blocker '{blocker_card}' declared by {player_id}. Not on battlefield or tapped.")
                return False

            # Validate that the attacker is among the declared attackers
            valid_attacker = next((perm for perm in attackers if perm["card"] == attacker_card), None)
            if not valid_attacker:
                print(f"[GAME ENGINE] Invalid attacker '{attacker_card}' declared by {player_id}. Not among declared attackers.")
                return False

            # Ensure each blocker is only assigned to one attacker
            if blocker_card in used_blockers:
                print(f"[GAME ENGINE] Invalid blockers declared by {player_id}. Duplicate blocker '{blocker_card}' detected.")
                return False

            used_blockers.add(blocker_card)

        self.game_state["combat"]["blockers"][player_id] = [{"blocker": decl["blocker"], "attacker": decl["attacker"]} for decl in blockers]
        print(f"[GAME ENGINE] {player_id} declared blockers: {blockers}")

        return True

    def assign_damage_order(self, player_id, damage_order):
        """Handles the assignment of damage order for the given player."""

        if self.game_state["phase"] != "ASSIGN_DAMAGE_ORDER":
            return False
        
        all_pids = list(self.game_state["life_totals"].keys())
        defending_player = next((p for p in all_pids if p != player_id), None)

        if not defending_player:
            return False

        if player_id not in self.game_state["combat"]["blockers"]:
            print(f"[GAME ENGINE] {player_id} has not declared blockers yet.")
            return False

        if player_id != self.game_state["active_player"]:
            print(f"[GAME ENGINE] {player_id} is not the active player and cannot assign damage order.")
            return False

        blockers = self.game_state["combat"]["blockers"].get(defending_player, [])

        if not blockers:
            print(f"[GAME ENGINE] No blockers declared by {defending_player}. Cannot assign damage order.")
            return True

        damage_order = {}

        for declaration in blockers:

            attacker_card = declaration.get("attacker")
            blocker_card = declaration.get("blocker")

            if attacker_card is None or blocker_card is None:
                print(f"[GAME ENGINE] Invalid blocker declaration: {declaration}")
                return False

            # Get the list of blockers assigned to this attacker
            actual_blockers = [b["blocker"] for b in blockers if b["attacker"] == attacker_card]

            # Make sure the submitted blockers are exactly the same as the actual blockers for this attacker
            if set(blocker_card.get(attacker_card, [])) != set(actual_blockers):
                print(f"[GAME ENGINE] Damage order mismatch for attacker '{attacker_card}'. Expected blockers: {actual_blockers}, got: {blocker_card.get(attacker_card, [])}")
                return False

            # blockers.get() returns a list of blockers for the given attacker, or an empty list if none exist
            damage_order[attacker_card] = blockers.get(attacker_card, []) + [blocker_card]

        # attackers_with_multiple_blockers = [attacker for attacker, assigned_blockers in damage_order.items() if len(assigned_blockers) > 1]

        # for block in blockers:
        #     attacker_card = block.get("attacker")

        #     if attacker_card not in attackers_with_multiple_blockers:
        #         damage_order[attacker_card] = [block.get("blocker")]

        #     attackers_with_multiple_blockers

        self.game_state["combat"]["damage_order"] = damage_order
        print(f"[GAME ENGINE] {player_id} assigned damage order: {damage_order}")

        return True

    def resolve_combat_damage(self):
        """Resolves combat damage based on declared attackers, blockers, and assigned damage order."""

        all_pids = list(self.game_state["life_totals"].keys())
        active_player = self.game_state["active_player"]
        defending_player = next((p for p in all_pids if p != active_player), None)

        if not defending_player:
            print("[GAME ENGINE] No defending player found. Cannot resolve combat.")
            return False
        
        attackers = self.game_state["combat"]["attackers"]
        blockers = self.game_state["combat"]["blockers"].get(defending_player, [])
        damage_order = self.game_state["combat"].get("damage_order", {})

        damage_events = []

        # 1) Resolve damage for each attacker based on assigned blockers and damage order
        for attacker in attackers:
            attacker_card = attacker["card"]

            power = attacker.get("power", 0)

            assigned_blockers = [b["blocker"] for b in blockers if b["attacker"] == attacker_card]

            # No blockers; damage goes to defending player
            if not assigned_blockers:
                self.game_state["life_totals"][defending_player] -= power
                damage_events.append({
                    "source": attacker_card,
                    "target": defending_player,
                    "controller": active_player,
                    "amount": power
                })

                continue
            # With Blockers; damage is assigned to blockers in the specified order
            ordered_blockers = damage_order.get(attacker_card, assigned_blockers)

            remaining_power = power

            for blocker_card in ordered_blockers:
                # Find the blocker permanent on the battlefield by matching the card name
                blocker = next((perm for perm in self.game_state["battlefield"][defending_player] if perm["card"] == blocker_card), None)

                if blocker is None:
                    print(f"[GAME ENGINE] Blocker '{blocker_card}' not found on battlefield for {defending_player}.")
                    continue

                blocker_toughness = blocker.get("toughness", 0)
                blocker_damage = blocker.get("damage", 0)

                lethal_damage = max(0, blocker_toughness - blocker_damage)

                damage_to_block = min(remaining_power, lethal_damage)

                blocker["damage"] = blocker_damage + damage_to_block
                remaining_power -= damage_to_block

                damage_events.append({
                    "source": attacker_card,
                    "target": blocker_card,
                    "controller": active_player,
                    "amount": damage_to_block
                })

                if remaining_power <= 0:
                    break

        # 2) Resolve damage for each blocker against their assigned attacker
        for block in blockers:
            blocker_card = block.get("blocker")
            attacker_card = block.get("attacker")

            # Find the attacker permanent on the battlefield by matching the card name
            attacker = next((perm for perm in self.game_state["battlefield"][active_player] if perm["card"] == attacker_card), None)

            if attacker is None:
                print(f"[GAME ENGINE] Attacker '{attacker_card}' not found on battlefield for {active_player}.")
                continue

            attacker_damage = attacker.get("damage", 0)

            # Find the blocker permanent on the battlefield by matching the card name
            blocker = next((perm for perm in self.game_state["battlefield"][defending_player] if perm["card"] == blocker_card), None)

            if blocker is None:
                print(f"[GAME ENGINE] Blocker '{blocker_card}' not found on battlefield for {defending_player}.")
                continue

            blocker_power = blocker.get("power", 0)

            # Blocker deals damage to the attacker
            attacker["damage"] = attacker_damage + blocker_power

            damage_events.append({
                "source": blocker_card,
                "target": attacker_card,
                "controller": defending_player,
                "amount": blocker_power
            })

        # 3) Check for destroyed creatures and move them to the graveyard
        creatures_died = []
        for player_id in [active_player, defending_player]:
            creatures_to_remove = []
            battlefield = self.game_state["battlefield"][player_id]
            graveyard = self.game_state["graveyard"][player_id]

            if "toughness" not in perm or "damage" not in perm:
                print(f"[GAME ENGINE] Warning: Creature permanent missing 'toughness' or 'damage' attributes for player {player_id}. Skipping damage check.")
                continue

            for perm in battlefield:
                card_name = perm.get("card")
                damage = perm.get("damage", 0)
                toughness = perm.get("toughness", 0)

                if damage >= toughness:
                    creatures_to_remove.append((player_id, card_name))
                    graveyard.append(card_name)
                    creatures_died.append(card_name)

            self.game_state["battlefield"][player_id] = [perm for perm in self.game_state["battlefield"][player_id] if (player_id, perm.get("card")) not in creatures_to_remove]

        print(f"[GAME ENGINE] Combat resolution completed. Damage events: {damage_events}")

        # 4) Check for a player's life total reaching 0 or below
        loser = None
        for player_id, life_total in self.game_state["life_totals"].items():
            if life_total <= 0:
                print(f"[GAME ENGINE] {player_id} has been reduced to {life_total} life. Game over.")
                loser = player_id

        # 5 Reset combat result and damage order for the next combat phase
        result = {
            "damage_events": damage_events,
            "life_totals": self.game_state["life_totals"].copy(),
            "creatures_died": creatures_died,
            "loser": loser
        }

        print(f"[GAME ENGINE] Combat resolution result: {result}")

        # Reset if needed for next combat phase, but keep the damage events and life totals for reporting
        # Comment the code block if wrong
        self.game_state["combat"]["attackers"] = []
        self.game_state["combat"]["blockers"] = {}
        self.game_state["combat"]["damage_order"] = {}

        return result
    

if __name__ == "__main__":
    server = MTGNPServer()
    server.start()
