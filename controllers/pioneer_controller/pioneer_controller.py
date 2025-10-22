import json
import math
import random
import redis
from controller import Robot
from flask import Flask, request
import threading, time
from collections import deque
from opcua import Server, ua
from datetime import datetime


# ==== Cấu hình ====
# Tốc độ các bánh xe (rad/s)
FORWARD_FAST_SPEED = 3
FORWARD_NORMAL_SPEED = 2.5
FORWARD_SLOW_SPEED = 1
BACKWARD_SPEED = 4.0
TURN_SPEED = 0.5
TURN_DURATION = 3  # giây

# ==== Khởi tạo robot ====
robot = Robot()
timestep = int(robot.getBasicTimeStep())

# ==== Khởi tạo motor ====
front_left_motor = robot.getDevice('front left wheel')
rear_left_motor = robot.getDevice('back left wheel')
front_right_motor = robot.getDevice('front right wheel')
rear_right_motor = robot.getDevice('back right wheel')

motors = [front_left_motor, rear_left_motor, front_right_motor, rear_right_motor]
for m in motors:
    m.setPosition(float('inf'))
    m.setVelocity(0.0)

# ==== Khởi tạo GPS ====
gps = robot.getDevice('gps')
gps.enable(timestep)

imu = robot.getDevice('inertial unit')
imu.enable(timestep)


class RobotCommandServer:
    def __init__(self, endpoint="opc.tcp://0.0.0.0:4840/freeopcua/server/"):
        self.server = Server()
        self.endpoint = endpoint
        self.message_var = None
        self.namespace_idx = None
        self.is_monitoring = False
        self.monitor_thread = None
        self.last_processed_command = None  # Để tránh xử lý command trùng lặp

    def setup_server(self):
        """Initialize server configuration"""
        self.server.set_endpoint(self.endpoint)
        self.server.set_security_policy([ua.SecurityPolicyType.NoSecurity])

        uri = "http://example.org"
        self.namespace_idx = self.server.register_namespace(uri)
        print(f"Registered namespace: {uri} at index {self.namespace_idx}")

        objects = self.server.get_objects_node()
        robot_device = objects.add_object(self.namespace_idx, "RobotController")

        self.message_var = robot_device.add_variable(
            self.namespace_idx,
            "CommandMessage",
            "Ready"
        )
        self.message_var.set_writable(True)

        node_id = self.message_var.nodeid
        print(f"Message Variable NodeId: ns={node_id.NamespaceIndex};i={node_id.Identifier}")

        return node_id

    def start(self):
        """Start the server"""
        try:
            node_id = self.setup_server()
            self.server.start()
            print(f"🚀 Robot Command Server started at {self.endpoint}")
            print(f"📍 Message NodeId: ns={node_id.NamespaceIndex};i={node_id.Identifier}")

            self.start_monitoring()
            return True
        except Exception as e:
            print(f"Failed to start server: {e}")
            return False

    def start_monitoring(self):
        """Start monitoring in separate thread"""
        self.is_monitoring = True
        self.monitor_thread = threading.Thread(target=self._monitor_messages, daemon=True)
        self.monitor_thread.start()
        print("📡 OPC UA monitoring started in background thread")

    def stop_monitoring(self):
        """Stop monitoring"""
        self.is_monitoring = False
        if self.monitor_thread:
            self.monitor_thread.join(timeout=1.0)

    def stop(self):
        """Stop the server"""
        try:
            self.stop_monitoring()
            self.server.stop()
            print("🛑 OPC UA Server stopped")
        except Exception as e:
            print(f"Error stopping server: {e}")

    def _monitor_messages(self):
        """Internal monitoring method - OPTIMIZED"""
        print("📡 Monitoring for robot commands...")

        try:
            while self.is_monitoring:
                if self.message_var is None:
                    time.sleep(0.01)
                    continue

                try:
                    current_value = self.message_var.get_value()

                    # Chỉ xử lý khi có command mới và khác "Ready"
                    if (current_value != self.last_processed_command and
                            current_value != "Ready" and
                            current_value is not None):
                        # if "||" in current_value:
                        #     tm = time.time_ns()
                        #     print(f"Timestamp now (ns): {tm}")
                        #     print(f"Current OPC UA value: {current_value}")
                        timestamp = datetime.now().strftime('%H:%M:%S')
                        print(f"📨 [{timestamp}] Received OPC UA command: {current_value}")

                        self.process_command(current_value)
                        self.last_processed_command = current_value

                except Exception as e:
                    print(f"Error reading OPC UA value: {e}")

                time.sleep(0.1)

        except Exception as e:
            print(f"❌ Error in OPC UA monitoring: {e}")

    def process_command(self, command):
        """Process received robot command"""
        global current_command, is_turning

        print(f"🤖 Processing OPC UA command: {command}")
        a = extract_direction(command)
        if "forward-slow" in command:
            if not is_turning:
                current_command = 'forward-slow'
            else:
                command_queue.append('forward-slow')
            print("▶️  Robot is running slow...")
        elif "forward-fast" in command:
            if not is_turning:
                current_command = 'forward-fast'
            else:
                command_queue.append('forward-fast')
            print("▶️  Robot is running fast...")
        elif "forward" in command:
            if not is_turning:
                current_command = 'forward'
            else:
                command_queue.append('forward')
            print("▶️  Robot moving forward...")
        elif "stop" in command:
            current_command = 'stop'
            print("⏹️  Robot stopped...")
        elif ("left" in a) & ("-" not in a):
            if not is_turning:
                current_command = 'left'
            else:
                command_queue.append('left')
            print("🔄 Robot turning left...")
        elif ("right" in a) & ("-" not in a):
            if not is_turning:
                current_command = 'right'
            else:
                command_queue.append('right')
            print("🔄 Robot turning right...")
        elif ("right" in a or "left" in a) & ('-' in a):
            if not is_turning:
                current_command = a
            else:
                command_queue.append(a)
            print(f"🔄 Robot turning {a}...")
        elif "back" in command:
            if not is_turning:
                current_command = 'back'
            else:
                command_queue.append('back')
            print("⬅️ Robot moving backward...")
        else:
            print(f"❓ Unknown OPC UA command: {command}")


def extract_direction(s: str):
    """
    Return the first match like 'left', 'right', 'left-3', 'right-29' from the input string.
    """
    import re
    return s.split(": ")[-1]
    # m = re.search(r'\b(?:left|right)(?:-\d+)?\b', s, re.IGNORECASE)
    # return m.group(0) if m else None


# ==== Hàm set tốc độ ====
def set_velocity(left_speed, right_speed):
    front_left_motor.setVelocity(left_speed)
    rear_left_motor.setVelocity(left_speed)
    front_right_motor.setVelocity(right_speed)
    rear_right_motor.setVelocity(right_speed)


# ==== Các hàm điều khiển ====
def move_forward_fast():
    set_velocity(FORWARD_FAST_SPEED, FORWARD_FAST_SPEED)


def move_forward_normal():
    set_velocity(FORWARD_NORMAL_SPEED, FORWARD_NORMAL_SPEED)


def move_forward_slow():
    set_velocity(FORWARD_SLOW_SPEED, FORWARD_SLOW_SPEED)


def move_backward():
    set_velocity(-BACKWARD_SPEED, -BACKWARD_SPEED)


def turn_left():
    set_velocity(-TURN_SPEED, TURN_SPEED)


def turn_left_stabilizing():
    if before_command is None or before_command == 'stop':
        set_velocity(-TURN_SPEED, TURN_SPEED)
        return
    if before_command == 'forward-fast':
        speed = FORWARD_FAST_SPEED
    elif before_command == 'forward-slow':
        speed = FORWARD_SLOW_SPEED
    else:
        speed = FORWARD_NORMAL_SPEED
    set_velocity(speed/2, speed + speed/2)

def turn_right():
    set_velocity(TURN_SPEED, -TURN_SPEED)

def turn_right_stabilizing():
    if before_command is None or before_command == 'stop':
        set_velocity(TURN_SPEED, -TURN_SPEED)
        return
    if before_command == 'forward-fast':
        speed = FORWARD_FAST_SPEED
    elif before_command == 'forward-slow':
        speed = FORWARD_SLOW_SPEED
    else:
        speed = FORWARD_NORMAL_SPEED
    set_velocity(speed + speed/2, speed/2)


def stop():
    set_velocity(0, 0)


# ==== Hàm tính độ lệch góc an toàn ====
def angle_diff(a, b):
    """Trả về độ lệch nhỏ nhất giữa 2 góc (rad) trong [-π, π]"""
    d = b - a
    while d > math.pi:
        d -= 2 * math.pi
    while d < -math.pi:
        d += 2 * math.pi
    return d


# ==== HTTP API ====
current_command = 'stop'
is_turning = False
current_yaw = None
command_queue = deque()
stabilizing = None
num = None
before_command = None


# Cache cho GPS và IMU data
last_gps_update = 0
last_imu_update = 0
cached_gps_data = (0, 0)
cached_heading = 0
GPS_UPDATE_INTERVAL = 0.1
IMU_UPDATE_INTERVAL = 0.05


def get_current_robot_pose():
    global last_gps_update, cached_gps_data
    current_time = time.time()

    if current_time - last_gps_update > GPS_UPDATE_INTERVAL:
        gps_values = gps.getValues()
        cached_gps_data = (gps_values[0], gps_values[1])
        last_gps_update = current_time

    return cached_gps_data


def get_current_robot_heading():
    global last_imu_update, cached_heading
    current_time = time.time()

    if current_time - last_imu_update > IMU_UPDATE_INTERVAL:
        rpy = imu.getRollPitchYaw()
        yaw = rpy[2]
        yaw = round(yaw, 6) + 1.570798
        cached_heading = math.atan2(math.sin(yaw), math.cos(yaw))
        last_imu_update = current_time

    return cached_heading


def set_motion(command):
    global is_turning, current_yaw, current_command, stabilizing, num, before_command

    if is_turning and current_yaw is not None:
        current = get_current_robot_heading()
        diff = angle_diff(current_yaw, current)
        if abs(diff) >= (math.pi / 2 - 0.01):
            stop()
            current_command = 'stop'
            is_turning = False
            current_yaw = None
            if command_queue:
                current_command = command_queue.popleft()
                set_motion(current_command)
            time.sleep(0.001)
        return

    if stabilizing and current_yaw is not None and num is not None:
        current = get_current_robot_heading()
        diff = angle_diff(current_yaw, current)
        if (num is None) or (num <= 0):
            return
        rate = 180/num
        condition = (math.pi / rate - 0.01)
        print("Rate: ", rate)
        print("Condition: ", condition)
        print("Diff: ", abs(diff))

        if abs(diff) >= condition:
            stop()
            if before_command is None:
                current_command = 'stop'
            else:
                current_command = before_command
            stabilizing = False
            current_yaw = None
            num = None
            if command_queue:
                current_command = command_queue.popleft()
                set_motion(current_command)
            time.sleep(0.001)
        return

    if command == 'forward-slow':
        before_command = command
        move_forward_slow()
    elif command == 'forward':
        before_command = command
        move_forward_normal()
    elif command == 'forward-fast':
        before_command = command
        move_forward_fast()
    elif command == 'left':
        before_command = command
        turn_left()
        is_turning = True
        current_yaw = get_current_robot_heading()
        return
    elif command == 'right':
        before_command = command
        turn_right()
        is_turning = True
        current_yaw = get_current_robot_heading()
        return
    elif command == 'back':
        before_command = command
        move_backward()
    elif command == 'stop':
        before_command = command
        stop()
    import re
    left_stabilization = re.match(r'^\s*(left)(?:-(\d+))?\s*$', command, re.IGNORECASE)
    if left_stabilization is not None:
        turn_left_stabilizing()
        stabilizing = True
        num = int(left_stabilization.group(2)) if left_stabilization.group(2) is not None else None
        current_yaw = get_current_robot_heading()
    else:
        right_stabilization = re.match(r'^\s*(right)(?:-(\d+))?\s*$', command, re.IGNORECASE)
        if right_stabilization is not None:
            turn_right_stabilizing()
            stabilizing = True
            num = int(right_stabilization.group(2)) if right_stabilization.group(2) is not None else None
            current_yaw = get_current_robot_heading()


app = Flask(__name__)


@app.route('/control', methods=['POST'])
def control():
    global current_command, is_turning
    data = request.json
    command = data.get('command', '')
    import re

    if command in ['forward-slow', 'forward', 'forward-fast', 'left', 'right', 'back', 'stop']:
        if is_turning:
            command_queue.append(command)
            return {'status': 'queued', 'queue_length': len(command_queue)}
        else:
            current_command = command
            return {'status': 'ok', 'command': current_command}
    elif re.match(r'^\s*(right|left)(?:-(\d+))?\s*$', command, re.IGNORECASE):
        if not is_turning:
            current_command = command
            return {'status': 'ok', 'command': current_command}
    else:
        return {'status': 'error', 'message': 'Invalid command'}, 400


@app.route('/current-robot-pose', methods=['GET'])
def current_robot_pose():
    x, y = get_current_robot_pose()
    return {"status": "ok", "x": x, "y": y}


@app.route('/current-robot-heading', methods=['GET'])
def current_robot_heading():
    z = get_current_robot_heading()
    return {"status": "ok", "heading": z}


def run_flask():
    print("🌐 Starting Flask server on http://0.0.0.0:6000")
    app.run(host='0.0.0.0', port=6000, debug=False, use_reloader=False, threaded=True)


# ==== Redis client với connection pooling ====
redis_pool = redis.ConnectionPool(host='127.0.0.1', port=6379, db=0, max_connections=2)
redis_client = redis.Redis(connection_pool=redis_pool)

last_redis_publish = 0
REDIS_PUBLISH_INTERVAL = 0.1

# map_supervisor = Supervisor()

obstacle_topic = redis_client.pubsub()
obstacle_topic.subscribe('obstacles')


def parse_obstacles_message(message):
    import time, json
    data = json.loads(message.decode())
    obstacles = data.get("obstacles", [])
    result = []
    timestamp_str = str(int(time.time() * 1000))
    for obj in obstacles:
        bbox = obj.get("bbox", {})
        center = obj.get("center", {})
        converted = {
            "timestamp": timestamp_str,
            "camera_id": 0,
            "class": "static",
            "object_id": int(obj.get("id", 0)),
            "conf": 0.95,
            "center": center,
            "corners": bbox
        }
        result.append(converted)
    return result

def publish_robot_and_obstacle_pose():
    global last_redis_publish
    current_time = time.time()

    if current_time - last_redis_publish < REDIS_PUBLISH_INTERVAL:
        return

    obstacles = []
    while True:
        msg = obstacle_topic.get_message(ignore_subscribe_messages=True)
        if msg is None:
            break  # Không có message mới -> dừng đọc
        if msg['type'] == 'message':
            obstacles = parse_obstacles_message(msg['data'])
    try:
        x, y = get_current_robot_pose()
        heading = get_current_robot_heading()

        message = {
            "april_tags": [
                {
                    "timestamp": time.time_ns(),
                    "camera_id": random.randint(1, 10),
                    "object_id": random.randint(1, 10),
                    "yaw": heading,
                    "center": [x, y],
                    "corners": []
                }
            ],
            "objects": obstacles
        }
        redis_client.xadd("robot_obstacle_pose_stream", {"data": json.dumps(message)})
        last_redis_publish = current_time

    except Exception as e:
        print(f"Redis publish error: {e}")


# ==== Khởi tạo servers ====
print('🤖 Starting Robot Controller...')

flask_thread = threading.Thread(target=run_flask, daemon=True)
flask_thread.start()

server = RobotCommandServer()
opc_ua_started = server.start()

if not opc_ua_started:
    print("⚠️  Warning: OPC UA server failed to start, continuing with Flask only")

print('✅ Controller started with servers:')
print('   - Flask API server at http://localhost:6000/control')
if opc_ua_started:
    print('   - OPC UA server at opc.tcp://0.0.0.0:4840/freeopcua/server/')

print('🔄 Starting main simulation loop...')

try:
    step_count = 0
    while robot.step(timestep) != -1:
        step_count += 1
        publish_robot_and_obstacle_pose()
        set_motion(current_command)

except KeyboardInterrupt:
    print('\n🛑 Received interrupt signal, shutting down...')
finally:
    if opc_ua_started:
        server.stop()
    print('👋 Robot controller stopped')
