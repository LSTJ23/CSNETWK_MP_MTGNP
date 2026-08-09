import socket
import threading
import json
import struct
import sys
import time

MAX_PDU_SIZE = 65535
DEFAULT_HOST = '127.0.0.1'
DEFAULT_PORT = 4444
PING_INTERVAL_SEC = 30.0
PONG_TIMEOUT_SEC = 10.0

def send_pdu(sock: socket.socket, payload: dict) -> None:
    json_bytes = json.dumps(payload).encode('utf-8')
    if len(json_bytes) > MAX_PDU_SIZE:
        raise ValueError(f"Payload size {len(json_bytes)} exceeds MAX_PDU_SIZE.")
    header = struct.pack(">I", len(json_bytes))
    sock.sendall(header + json_bytes)

def recv_exact(sock: socket.socket, length: int) -> bytes:
    buf = bytearray()
    while len(buf) < length:
        chunk = sock.recv(length - len(buf))
        if not chunk:
            raise ConnectionResetError("Server connection closed.")
        buf.extend(chunk)
    return bytes(buf)

def recv_pdu(sock: socket.socket) -> dict:
    header = recv_exact(sock, 4)
    length = struct.unpack(">I", header)[0]
    if length > MAX_PDU_SIZE:
        raise ValueError(f"Received PDU header size {length} > MAX_PDU_SIZE.")
    payload_bytes = recv_exact(sock, length)
    return json.loads(payload_bytes.decode('utf-8'))


class MTGNPClient:
    def __init__(self, host=DEFAULT_HOST, port=DEFAULT_PORT):
        self.host = host
        self.port = port
        self.sock = None
        self.running = False
        
        self.visible_state = {}
        self.player_ready_seq = 1
        self.current_priority_seq = 0
        self.last_server_seq = 0
        self.has_priority = False
        self.awaiting_rematch_decision = False
        
        self.ping_counter = 0
        self.awaiting_pong = False
        self.lock = threading.Lock()

    def start(self):
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        try:
            self.sock.connect((self.host, self.port))
            print(f"[CLIENT] Connected to server at {self.host}:{self.port}")
            print("[LOBBY] Type 'ready <player_id> [card1,card2,...]' to signal ready state.")
        except Exception as e:
            print(f"[CLIENT] Connection failed: {e}")
            return

        self.running = True

        receiver_thread = threading.Thread(target=self.listen_for_messages, daemon=True)
        receiver_thread.start()

        ping_thread = threading.Thread(target=self.ping_heartbeat_loop, daemon=True)
        ping_thread.start()

        self.user_input_loop()

    def ping_heartbeat_loop(self):
        while self.running:
            time.sleep(PING_INTERVAL_SEC)
            if not self.running:
                break

            with self.lock:
                self.ping_counter += 1
                ping_pdu = {"type": "PING", "seq_num": self.ping_counter}
                self.awaiting_pong = True
                try:
                    send_pdu(self.sock, ping_pdu)
                except Exception:
                    break

            send_time = time.time()
            while self.running and self.awaiting_pong:
                if time.time() - send_time > PONG_TIMEOUT_SEC:
                    print("\n[CLIENT ERROR] Heartbeat timeout. Disconnecting.")
                    self.shutdown()
                    return
                time.sleep(0.5)

    def listen_for_messages(self):
        while self.running:
            try:
                pdu = recv_pdu(self.sock)
            except (ConnectionResetError, struct.error, ValueError):
                if self.running:
                    print("\n[CLIENT] Server connection lost.")
                self.shutdown()
                break

            msg_type = pdu.get("type")
            seq_num = pdu.get("seq_num", 0)

            with self.lock:
                # Prevent heartbeat PDUs from overwriting the expected action seq_num
                if msg_type != "PONG":
                    self.last_server_seq = seq_num

            if msg_type == "PONG":
                with self.lock:
                    self.awaiting_pong = False

            elif msg_type == "GAME_STATE_UPDATE":
                with self.lock:
                    state_data = pdu.get("state", {})
                    if state_data.get("phase") == "LOBBY":
                        print(f"\n[LOBBY UPDATE] Ready: {state_data.get('players_ready')}/2. Waiting for: {state_data.get('waiting_for')}")
                    else:
                        self.visible_state = state_data
                        # If mulligan, update current_priority_seq to echo seq_num
                        if state_data.get("phase") == "MULLIGAN":
                            self.current_priority_seq = seq_num
                            self.render_visible_state()
                            print("\n[MULLIGAN] Hand redrawn. Type 'mulligan' to draw again, or 'keep' to keep.")

            elif msg_type == "PRIORITY_GRANT":
                with self.lock:
                    self.current_priority_seq = seq_num
                    self.has_priority = True
                print(f"\n>>> Priority Granted! [seq_num: {seq_num}] Commands: 'pass', 'cast <card_id>', 'concede'")

            elif msg_type == "PHASE_TRANSITION":
                with self.lock:
                    self.current_priority_seq = seq_num
                    new_phase = pdu.get("phase")
                    new_step = pdu.get("step")
                    if new_phase:
                        self.visible_state["phase"] = new_phase
                    if new_step:
                        self.visible_state["step"] = new_step
                print(f"\n[PHASE] Transited to {pdu.get('phase')} - {pdu.get('step')}")

            elif msg_type == "ERROR":
                print(f"\n[SERVER REJECTION] {pdu.get('code')}: {pdu.get('message')}")

            elif msg_type == "GAME_OVER":
                print(f"\n[GAME OVER] Winner: {pdu.get('winner')} | Reason: {pdu.get('reason')}")

            elif msg_type == "REMATCH_REQUEST":
                self.awaiting_rematch_decision = True
                print(f"\n[REMATCH] {pdu.get('message')}")
                print("Type 'yes' or 'no': ", end="", flush=True)

            elif msg_type == "REMATCH_RESULT":
                accepted = pdu.get("accepted", False)
                print(f"\n[REMATCH RESULT] {pdu.get('message')}")
                if accepted:
                    with self.lock:
                        self.visible_state = {}
                        self.has_priority = False
                        self.awaiting_rematch_decision = False
                    print("[LOBBY] Send 'ready <player_id> [deck]' to signal ready state.")
                else:
                    self.shutdown()
                    break

    def render_visible_state(self):
        print("\n" + "=" * 50)
        print("--- AUTHORITATIVE VISIBLE GAME STATE ---")
        print(f"Turn: {self.visible_state.get('turn')} | Phase: {self.visible_state.get('phase')} | Active Player: {self.visible_state.get('active_player')}")
        print(f"Life Totals: {self.visible_state.get('life_totals', {})}")
        print(f"Your Hand: {self.visible_state.get('hand', [])}")
        print(f"Opponent Hand Counts: {self.visible_state.get('hand_counts', {})}")
        print(f"Library Counts: {self.visible_state.get('library_counts', {})}")
        print(f"Battlefield: {self.visible_state.get('battlefield', {})}")
        print(f"Graveyard: {self.visible_state.get('graveyard', {})}")
        print(f"Stack: {self.visible_state.get('stack', [])}")
        print("=" * 50)

    def send_player_ready(self, player_id: str, deck_list: list):
        pdu = {
            "type": "PLAYER_READY",
            "seq_num": self.player_ready_seq,
            "player_id": player_id,
            "deck_list": deck_list
        }
        self.player_ready_seq += 1
        send_pdu(self.sock, pdu)

    def send_action(self, action_type: str, extra_fields: dict = None):
        pdu = {
            "type": action_type,
            "seq_num": self.current_priority_seq if action_type not in ["CONCEDE", "REMATCH_RESPONSE", "MULLIGAN_CHOICE"] else self.last_server_seq
        }
        if extra_fields:
            pdu.update(extra_fields)
        
        send_pdu(self.sock, pdu)
        with self.lock:
            self.has_priority = False

    def user_input_loop(self):
        while self.running:
            try:
                cmd = input().strip()
                if not cmd:
                    continue

                if self.awaiting_rematch_decision:
                    if cmd.lower() in ["yes", "y"]:
                        self.send_action("REMATCH_RESPONSE", {"accepted": True})
                        self.awaiting_rematch_decision = False
                        print("[CLIENT] Rematch accepted. Waiting for opponent...")
                    elif cmd.lower() in ["no", "n"]:
                        self.send_action("REMATCH_RESPONSE", {"accepted": False})
                        self.awaiting_rematch_decision = False
                        print("[CLIENT] Rematch declined. Exiting...")
                        self.shutdown()
                        break
                    else:
                        print("Please type 'yes' or 'no': ", end="", flush=True)
                    continue
                
                if self.visible_state.get("phase") == "MULLIGAN": 
                    if cmd.lower().startswith("keep"):
                        parts = cmd.split(maxsplit = 1)
                        cards_to_bottom = []
                        if len(parts) > 1:
                            clean_input = parts[1].translate(str.maketrans("", "", "[]\"'"))
                            cards_to_bottom = [c.strip() for c in clean_input.split(",") if c.strip()]
                        
                        pdu = {
                            "type": "MULLIGAN_CHOICE",
                            "seq_num": self.last_server_seq,  # Echoes the updated sequence number received from redraw/update
                            "keep": True,
                            "cards_to_bottom": cards_to_bottom
                        }
                        send_pdu(self.sock, pdu)
                        print("[CLIENT] Kept hand. Waiting for opponent's response...")
                    elif cmd.lower() == "mulligan": 
                        pdu = {
                            "type": "MULLIGAN_CHOICE",
                            "seq_num": self.last_server_seq,  # Echoes the updated sequence number received from redraw
                            "keep": False,
                            "cards_to_bottom": []
                        }
                        send_pdu(self.sock, pdu)
                        print("[CLIENT] Taking mulligan...")
                    else:
                        print("Mulligan phase. Type either 'keep' or 'mulligan': ", end="", flush=True)
                    continue

                if cmd.lower().startswith("ready"):
                    parts = cmd.split(maxsplit=2)
                    if len(parts) < 2:
                        print("Usage: ready <player_id> [card1,card2,...]")
                        continue
                    
                    p_id = parts[1]
                    cards = parts[2].split(",") if len(parts) > 2 else ["lightning_bolt_001", "shock_001"]
                    cards = [c.strip() for c in cards if c.strip()]
                    
                    self.send_player_ready(p_id, cards)
                    print(f"[CLIENT] Sent PLAYER_READY for '{p_id}' with {len(cards)} cards.")
                    continue

                if cmd.lower() in ["/exit", "concede"]:
                    self.send_action("CONCEDE")
                    continue

                if not self.has_priority:
                    print("You do not hold priority.")
                    continue

                parts = cmd.split(maxsplit=1)
                action = parts[0].lower()

                if action == "pass":
                    self.send_action("PRIORITY_PASS")
                elif action == "cast" and len(parts) > 1:
                    self.send_action("CAST_SPELL", {"card_id": parts[1]})
                elif action == "play" and len(parts) > 1:
                    self.send_action("PLAY_LAND", {"card_id": parts[1]})
                else:
                    print("Invalid command. Options: 'pass', 'cast <card_id>', 'concede'")

            except (KeyboardInterrupt, EOFError):
                break

        self.shutdown()

    def shutdown(self):
        self.running = False
        if self.sock:
            try:
                self.sock.close()
            except Exception:
                pass
        sys.exit(0)


if __name__ == "__main__":
    client = MTGNPClient()
    client.start()
