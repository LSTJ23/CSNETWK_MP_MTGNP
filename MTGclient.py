import socket
import threading
import json
import struct
import sys
import time
import os
import argparse

# Global constants for packet framing, server defaults, and heartbeat timing
MAX_PDU_SIZE = 65535
DEFAULT_HOST = '127.0.0.1'
DEFAULT_PORT = 4444
PING_INTERVAL_SEC = 30.0
PONG_TIMEOUT_SEC = 10.0

# Encodes a Python dictionary into JSON and sends it over TCP with a 4-byte big-endian length header
def send_pdu(sock: socket.socket, payload: dict) -> None:
    # Serialize dictionary payload to UTF-8 encoded bytes
    json_bytes = json.dumps(payload).encode('utf-8')
    # Validate payload size against the maximum PDU protocol limit
    if len(json_bytes) > MAX_PDU_SIZE:
        raise ValueError(f"Payload size {len(json_bytes)} exceeds MAX_PDU_SIZE.")
    # Pack payload length into a 4-byte unsigned integer (Big-Endian network byte order)
    header = struct.pack(">I", len(json_bytes))
    # Send both header and body atomically over the socket stream
    sock.sendall(header + json_bytes)

# Reads exact byte length from TCP socket to prevent incomplete packet reads
def recv_exact(sock: socket.socket, length: int) -> bytes:
    buf = bytearray()
    # Loop continuously until the entire requested byte count has been assembled
    while len(buf) < length:
        chunk = sock.recv(length - len(buf))
        if not chunk:
            raise ConnectionResetError("Server connection closed.")
        buf.extend(chunk)
    return bytes(buf)

# Receives and parses a full PDU packet from the socket
def recv_pdu(sock: socket.socket) -> dict:
    # Read fixed 4-byte big-endian length header
    header = recv_exact(sock, 4)
    length = struct.unpack(">I", header)[0]
    # Enforce packet size ceiling safety check
    if length > MAX_PDU_SIZE:
        raise ValueError(f"Received PDU header size {length} > MAX_PDU_SIZE.")
    # Read the full payload based on parsed header length
    payload_bytes = recv_exact(sock, length)
    # Decode raw UTF-8 bytes into JSON dict
    return json.loads(payload_bytes.decode('utf-8'))


class MTGNPClient:
    # Initializes client state, socket references, sequence numbers, and thread locks
    def __init__(self, host=DEFAULT_HOST, port=DEFAULT_PORT, verbose: bool = False):
        self.host = host
        self.port = port
        self.verbose = verbose
        self.sock = None
        self.running = False
        
        # Local view of authoritative game state
        self.visible_state = {}
        self.player_ready_seq = 1
        self.current_priority_seq = 0
        self.last_server_seq = 0
        self.has_priority = False
        self.awaiting_rematch_decision = False
        
        # Heartbeat tracking parameters
        self.ping_counter = 0
        self.awaiting_pong = False
        self.lock = threading.Lock()

    # Outputs debug information if verbose flag is enabled
    def log_verbose(self, message: str):
        if self.verbose:
            print(f"\n\033[36m[VERBOSE CLIENT]\033[0m {message}", flush=True)

    # Establishes TCP connection and launches receiver, heartbeat, and input threads
    def start(self):
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        try:
            self.sock.connect((self.host, self.port))
            verbose_status = "ENABLED" if self.verbose else "DISABLED"
            self.cleanprint(f"[CLIENT] Connected to server at {self.host}:{self.port} | Verbose Mode: [{verbose_status}]")
        except Exception as e:
            print(f"[CLIENT] Connection failed: {e}")
            return

        self.running = True

        # Spawn background listener thread to handle incoming server PDUs asynchronously
        receiver_thread = threading.Thread(target=self.listen_for_messages, daemon=True)
        receiver_thread.start()

        # Spawn background heartbeat thread to send periodic PING messages
        ping_thread = threading.Thread(target=self.ping_heartbeat_loop, daemon=True)
        ping_thread.start()

        # Hand over main thread to interactive command input loop
        self.user_input_loop()

    # Thread worker that periodically sends PING PDUs and checks for PONG timeout
    def ping_heartbeat_loop(self):
        while self.running:
            time.sleep(PING_INTERVAL_SEC)
            if not self.running:
                break

            # Safely assemble and send ping payload under lock
            with self.lock:
                self.ping_counter += 1
                ping_pdu = {"type": "PING", "seq_num": self.ping_counter}
                self.awaiting_pong = True
                try:
                    self.log_verbose(f"OUTGOING PDU (PING): {ping_pdu}")
                    send_pdu(self.sock, ping_pdu)
                except Exception:
                    break

            # Wait for PONG reply and monitor for timeout threshold
            send_time = time.time()
            while self.running and self.awaiting_pong:
                if time.time() - send_time > PONG_TIMEOUT_SEC:
                    print("\n[CLIENT ERROR] Heartbeat timeout. Disconnecting.")
                    self.shutdown()
                    return
                time.sleep(0.5)

    # Helper function to clear active prompt line and display incoming server messages cleanly
    def cleanprint(self, message: str):
        # Clears the active prompt line, prints the server message, and restores '> '.
        print(f"\r\033[K{message}\n"
              f"[Valid Commands: {self.get_available_commands()}]\n\n> ", end="", flush=True)

    # Worker thread listening for incoming network packets and dispatching based on PDU type
    def listen_for_messages(self):
        while self.running:
            try:
                pdu = recv_pdu(self.sock)
                self.log_verbose(f"INCOMING PDU: {json.dumps(pdu)}")
            except (ConnectionResetError, struct.error, ValueError):
                if self.running:
                    print("[CLIENT] Server connection lost.")
                self.shutdown()
                break

            msg_type = pdu.get("type")
            seq_num = pdu.get("seq_num", 0)

            with self.lock:
                # Prevent heartbeat PDUs from overwriting the expected action seq_num
                if msg_type != "PONG":
                    self.last_server_seq = seq_num

            # Handle heartbeats
            if msg_type == "PONG":
                with self.lock:
                    self.awaiting_pong = False

            # Update local game state mirror
            elif msg_type == "GAME_STATE_UPDATE":
                with self.lock:
                    state_data = pdu.get("state", {})
                    if state_data.get("phase") == "LOBBY":
                        print(f"[LOBBY UPDATE] Ready: {state_data.get('players_ready')}/2. Waiting for: {state_data.get('waiting_for')}")
                    else:
                        self.visible_state = state_data
                        # If mulligan, update current_priority_seq to echo seq_num
                        if state_data.get("phase") == "MULLIGAN":
                            self.current_priority_seq = seq_num
                        self.render_visible_state()

            # Handle explicit priority grants from server
            elif msg_type == "PRIORITY_GRANT":
                with self.lock:
                    self.current_priority_seq = seq_num
                    self.has_priority = True
                self.cleanprint(f">>> Priority Granted! [{seq_num}]")

            # Update phase/step metadata upon transition notification
            elif msg_type == "PHASE_TRANSITION":
                with self.lock:
                    # PHASE_TRANSITION is informational. The following
                    # PRIORITY_GRANT contains the action sequence number.
                    self.has_priority = False
                    new_phase = pdu.get("phase")
                    new_step = pdu.get("step")
                    if new_phase:
                        self.visible_state["phase"] = new_phase
                    if new_step:
                        self.visible_state["step"] = new_step
                    if "active_player" in pdu:
                        self.visible_state["active_player"] = pdu["active_player"]
                    if "turn" in pdu:
                        self.visible_state["turn"] = pdu["turn"]
                self.cleanprint(f"[PHASE] Transited to {pdu.get('phase')} - {pdu.get('step')}")

            # Display combat results
            elif msg_type == "COMBAT_DAMAGE_RESULT":
                self.cleanprint(
                    f"[COMBAT DAMAGE] Events: {pdu.get('damage_events', [])} | "
                    f"Life: {pdu.get('life_totals', {})} | "
                    f"Died: {pdu.get('creatures_died', [])}"
                )

            # Display rejection errors from server
            elif msg_type == "ERROR":
                self.cleanprint(f"[SERVER REJECTION] {pdu.get('code')}: {pdu.get('message')}")

            # Display game over banner
            elif msg_type == "GAME_OVER":
                print(f"[GAME OVER] Winner: {pdu.get('winner')} | Reason: {pdu.get('reason')}")

            # Handle post-game rematch requests
            elif msg_type == "REMATCH_REQUEST":
                self.awaiting_rematch_decision = True
                self.cleanprint(f"[REMATCH] {pdu.get('message')}")

            # Handle rematch outcome voting
            elif msg_type == "REMATCH_RESULT":
                accepted = pdu.get("accepted", False)
                self.cleanprint(f"[REMATCH RESULT] {pdu.get('message')}")
                if accepted:
                    with self.lock:
                        self.visible_state = {}
                        self.has_priority = False
                        self.awaiting_rematch_decision = False
                    self.cleanprint("[LOBBY] Send 'ready <player_id> [deck]' to signal ready state.")
                else:
                    self.shutdown()
                    break

    # Renders structured display of current authoritative game state to stdout
    def render_visible_state(self):
        self.cleanprint(
            f"\n{'=' * 50}\n"
            f"--- AUTHORITATIVE VISIBLE GAME STATE ---\n"
            f"Turn: {self.visible_state.get('turn')} | Phase: {self.visible_state.get('phase')} | Step: {self.visible_state.get('step')} | Active Player: {self.visible_state.get('active_player')}\n"
            f"Life Totals: {self.visible_state.get('life_totals', {})}\n"
            f"Your Hand: {self.visible_state.get('hand', [])}\n"
            f"Opponent Hand Counts: {self.visible_state.get('hand_counts', {})}\n"
            f"Library Counts: {self.visible_state.get('library_counts', {})}\n"
            f"Battlefield: {self.visible_state.get('battlefield', {})}\n"
            f"Graveyard: {self.visible_state.get('graveyard', {})}\n"
            f"Stack: {self.visible_state.get('stack', [])}\n"
            f"{'=' * 50}"
        )

    # Returns contextual list of commands available based on current phase and priority state
    def get_available_commands(self) -> str:
        """Returns valid commands based on current state and phase."""
        if self.awaiting_rematch_decision:
            return "yes | no | /exit"

        phase = self.visible_state.get("phase", "LOBBY")
        step = self.visible_state.get("step", "NONE")

        if phase == "LOBBY":
            return "ready <player_id> [card1,card2,...] | /exit"

        if phase == "MULLIGAN":
            return "keep | mulligan | concede | /exit"

        if not self.has_priority:
            return "concede | /exit (Waiting for priority...)"

        if phase in ["PRECOMBAT_MAIN", "POSTCOMBAT_MAIN"]:
            return "pass | cast <card_id> | play <card_id> | concede | /exit"
            
        elif step == "DECLARE_ATTACKERS":
            return "attack <card1,card2,...> | pass | concede | /exit"
            
        elif step == "DECLARE_BLOCKERS":
            return "block <blocker:attacker,...> | pass | concede | /exit"

        elif step == "ASSIGN_DAMAGE_ORDER":
            return "order <attacker>:<blocker1,blocker2,...> | pass | concede | /exit"

        return "pass | cast <card_id> | play <card_id> | concede | /exit"

    # Constructs and transmits PLAYER_READY PDU containing player ID and chosen deck
    def send_player_ready(self, player_id: str, deck_list: list):
        pdu = {
            "type": "PLAYER_READY",
            "seq_num": self.player_ready_seq,
            "player_id": player_id,
            "deck_list": deck_list
        }
        self.player_ready_seq += 1
        self.log_verbose(f"OUTGOING PDU: {json.dumps(pdu)}")
        send_pdu(self.sock, pdu)

    # Transmits general action PDU and releases local priority token
    def send_action(self, action_type: str, extra_fields: dict = None):
        pdu = {
            "type": action_type,
            "seq_num": self.current_priority_seq if action_type not in ["CONCEDE", "REMATCH_RESPONSE", "MULLIGAN_CHOICE"] else self.last_server_seq
        }
        if extra_fields:
            pdu.update(extra_fields)

        self.log_verbose(f"OUTGOING PDU: {json.dumps(pdu)}")
        send_pdu(self.sock, pdu)
        with self.lock:
            self.has_priority = False

    # Main user interactive loop parsing text commands from standard input
    def user_input_loop(self):
        while self.running:
            try:
                cmd = input().strip()
                if not cmd:
                    self.cleanprint("")
                    continue

                # Global exit shortcut
                if cmd.lower() == "/exit":
                    print("[CLIENT] Exiting immediately...")
                    self.running = False
                    self.shutdown()
                    os._exit(0)

                # 1. Rematch phase logic
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
                        print(f"Invalid command.")
                    continue
                
                current_phase = self.visible_state.get("phase", "LOBBY")

                # 2. Lobby phase logic
                if current_phase == "LOBBY":
                    if cmd.lower().startswith("ready"):
                        parts = cmd.split(maxsplit=2)
                        if len(parts) < 2:
                            print("Usage: ready <player_id> <card1,card2,...>")
                            continue
                        p_id = parts[1]
                        raw_cards = parts[2] if len(parts) > 2 else ""
                        # Strip standard array delimiters and formatting chars from input string
                        clean_cards = raw_cards.translate(str.maketrans("", "", "[]\"'"))
                        cards = [c.strip() for c in clean_cards.split(",") if c.strip()]
                        self.send_player_ready(p_id, cards)
                        continue
                    elif cmd.lower() in ["/exit", "concede"]:
                        self.send_action("CONCEDE")
                        continue
                    else:
                        print(f"Invalid command.")
                        continue

                # 3. Mulligan phase logic
                if current_phase == "MULLIGAN":
                    if cmd.lower().startswith("keep"):
                        parts = cmd.split(maxsplit=1)
                        cards_to_bottom = []
                        if len(parts) > 1:
                            clean_input = parts[1].translate(str.maketrans("", "", "[]\"'"))
                            cards_to_bottom = [c.strip() for c in clean_input.split(",") if c.strip()]
                        
                        pdu = {
                            "type": "MULLIGAN_CHOICE",
                            "seq_num": self.last_server_seq,
                            "keep": True,
                            "cards_to_bottom": cards_to_bottom
                        }
                        self.log_verbose(f"OUTGOING PDU: {json.dumps(pdu)}")
                        send_pdu(self.sock, pdu)
                        print("[CLIENT] Kept hand. Waiting for opponent's response...")
                    elif cmd.lower() == "mulligan": 
                        pdu = {
                            "type": "MULLIGAN_CHOICE",
                            "seq_num": self.last_server_seq,
                            "keep": False,
                            "cards_to_bottom": []
                        }
                        self.log_verbose(f"OUTGOING PDU: {json.dumps(pdu)}")
                        send_pdu(self.sock, pdu)
                        print("[CLIENT] Taking mulligan...")
                    elif cmd.lower() == "concede":
                        self.send_action("CONCEDE")
                    else:
                        print(f"Invalid command in MULLIGAN phase.")
                    continue

                # 4. In-game actions (UNTAP, UPKEEP, MAIN, COMBAT, etc.)
                if not self.has_priority:
                    print("You do not hold priority.")
                    continue

                parts = cmd.split(maxsplit=1)
                action = parts[0].lower()

                # Parse specific combat or main phase user commands
                if action == "pass":
                    self.send_action("PRIORITY_PASS")
                elif action == "cast" and len(parts) > 1:
                    self.send_action("CAST_SPELL", {"card_id": parts[1]})
                elif action == "play" and len(parts) > 1:
                    self.send_action("PLAY_LAND", {"card_id": parts[1]})
                elif action == "attack" and len(parts) > 1:
                    raw_cards = parts[1].translate(str.maketrans("", "", "[]\"'"))
                    attacker_ids = [c.strip() for c in raw_cards.split(",") if c.strip()]

                    # Infer opponent ID for default targeting in attacker declaration payload
                    opponent = next(
                        (p for p in self.visible_state.get("life_totals", {})
                         if p != self.visible_state.get("active_player")),
                        None
                    )

                    attackers = [
                        {
                            "card_id": card_id,
                            "creature_id": card_id,
                            "target": opponent
                        }
                        for card_id in attacker_ids
                    ]
                    self.send_action("DECLARE_ATTACKERS", {"attackers": attackers})
                elif action == "block" and len(parts) > 1:
                    declarations = []
                    # Parse blocker:attacker pairs
                    for pair in parts[1].split(","):
                        if ":" not in pair:
                            print("Usage: block <blocker>:<attacker>,<blocker>:<attacker>")
                            declarations = []
                            break
                        blocker, attacker = pair.split(":", 1)
                        declarations.append({
                            "blocker": blocker.strip(),
                            "attacker": attacker.strip()
                        })
                    self.send_action("DECLARE_BLOCKERS", {"blockers": declarations})
                elif action == "order" and len(parts) > 1:
                    damage_order = {}
                    # Parse attacker:blocker1,blocker2 damage assignment sequences
                    for group in parts[1].split(";"):
                        if ":" not in group:
                            continue
                        attacker, blocker_list = group.split(":", 1)
                        damage_order[attacker.strip()] = [
                            b.strip() for b in blocker_list.split(",") if b.strip()
                        ]
                    self.send_action("ASSIGN_DAMAGE_ORDER", {"damage_order": damage_order})
                elif action == "concede":
                    self.send_action("CONCEDE")
                else:
                    print(f"Invalid command.")
                    
            except (KeyboardInterrupt, EOFError):
                break

        self.shutdown()

    # Stops client process, releases socket connection, and exits system thread
    def shutdown(self):
        self.running = False
        if self.sock:
            try:
                self.sock.close()
            except Exception:
                pass
        sys.exit(0)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="MTGNP Game Client")
    parser.add_argument("-v", "--verbose", action="store_true", help="Enable verbose debug logging")
    args = parser.parse_args()

    client = MTGNPClient(host=DEFAULT_HOST, port=DEFAULT_PORT, verbose=args.verbose)
    client.start()