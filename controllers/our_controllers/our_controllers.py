import datetime
import json
import math
import random

import redis
from controller import Robot
from flask import Flask, request
import threading, time
from collections import deque

from opcua import Server, ua

# ==== Cấu hình ====
FORWARD_FAST_SPEED = 1.1
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

def turn_right():
    set_velocity(TURN_SPEED, -TURN_SPEED)

def stop():
    set_velocity(0, 0)

class RobotCommandServer:
    def __init__(self, endpoint="opc.tcp://0.0.0.0:4840/freeopcua/server/"):
        self.server = Server()
        self.endpoint = endpoint
        self.message_var = None
        self.namespace_idx = None

    def setup_server(self):
        """Initialize server configuration"""
        # Basic server setup
        self.server.set_endpoint(self.endpoint)
        self.server.set_security_policy([ua.SecurityPolicyType.NoSecurity])

        # Register namespace
        uri = "http://example.org"
        self.namespace_idx = self.server.register_namespace(uri)
        print(f"Registered namespace: {uri} at index {self.namespace_idx}")

        # Create object structure
        objects = self.server.get_objects_node()
        robot_device = objects.add_object(self.namespace_idx, "RobotController")

        # Create message variable
        self.message_var = robot_device.add_variable(
            self.namespace_idx,
            "CommandMessage",
            "Ready"
        )
        self.message_var.set_writable(True)

        # Log NodeId info
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
            return True
        except Exception as e:
            print(f"Failed to start server: {e}")
            return False

    def stop(self):
        """Stop the server"""
        try:
            self.server.stop()
            print("🛑 Server stopped")
        except Exception as e:
            print(f"Error stopping server: {e}")

    def monitor_messages(self):
        """Monitor incoming messages"""
        last_value = ""
        print("📡 Monitoring for robot commands...")

        try:
            while True:
                current_value = self.message_var.get_value()

                if current_value != last_value and current_value != "Ready":
                    timestamp = datetime.datetime.now().strftime('%H:%M:%S')
                    print(f"📨 [{timestamp}] Received command: {current_value}")

                    # Process command here
                    self.process_command(current_value)

                    last_value = current_value

                time.sleep(0.1)  # Fast polling

        except KeyboardInterrupt:
            print("🛑 Monitoring stopped by user")
        except Exception as e:
            print(f"❌ Error in monitoring: {e}")

    def process_command(self, command):
        """Process received robot command"""
        print(f"🤖 Processing command: {command}")

        # Add your robot command processing logic here
        if "forward-slow" in command:
            print("▶️  Robot is running slow...")
        elif "forward-fast" in command:
            print("▶️  Robot is running fast...")
        elif "stop" in command:
            print("⏹️  Robot stopped...")
        elif "left" in command:
            print("🔄 Robot turning left...")
        elif "right" in command:
            print("🔄 Robot turning right...")
        else:
            print(f"❓ Unknown command: {command}")

def main():
    server = RobotCommandServer()

    if server.start():
        try:
            server.monitor_messages()
        finally:
            server.stop()
    else:
        print("Failed to start server")

if __name__ == "__main__":
    main()

# ==== HTTP API ====
current_command = 'stop'
is_turning = False
turn_end_time = 0
current_yaw = None
command_queue = deque()

def set_motion(command):
    global is_turning, turn_end_time, current_command

    # Nếu đang quay, kiểm tra xem có kết thúc chưa
    if is_turning:
        if time.time() >= turn_end_time:
            current_command = 'stop'
            is_turning = False
            stop()
            # Sau khi quay xong, lấy lệnh tiếp theo từ hàng chờ (nếu có)
            if command_queue:
                current_command = command_queue.popleft()
                set_motion(current_command)
            time.sleep(0.1)
        return

    if command == 'forward-slow':
        move_forward_slow()
    elif command == 'forward':
        move_forward_normal()
    elif command == 'forward-fast':
        move_forward_fast()
    elif command == 'left':
        turn_left()
        is_turning = True
        current_yaw = get_current_robot_pose()
        turn_end_time = get_current_robot_heading() + TURN_DURATION
    elif command == 'right':
        turn_right()
        is_turning = True
        current_yaw = get_current_robot_pose()
        turn_end_time = time.time() + TURN_DURATION
    elif command == 'back':
        move_backward()
    elif command == 'stop':
        stop()

app = Flask(__name__)

@app.route('/control', methods=['POST'])
def control():
    global current_command, is_turning
    data = request.json
    command = data.get('command', '')

    if command in ['forward-slow', 'forward', 'forward-fast', 'left', 'right', 'back', 'stop']:
        if is_turning:
            command_queue.append(command)
            return {'status': 'queued', 'queue_length': len(command_queue)}
        else:
            current_command = command
            return {'status': 'ok', 'command': current_command}
    else:
        return {'status': 'error', 'message': 'Invalid command'}, 400

def get_current_robot_pose():
    gps_values = gps.getValues()
    ax, ay = gps_values[0], gps_values[1]
    return ax, ay

def get_current_robot_heading():
    rpy = imu.getRollPitchYaw()  # [roll, pitch, yaw]
    yaw = rpy[2]  # Góc quay quanh trục Z
    yaw = round(yaw, 6) + 1.570798
    # Normalize yaw về [-π, π]
    normalized_yaw = math.atan2(math.sin(yaw), math.cos(yaw))

    return normalized_yaw

redis_client = redis.Redis(host='127.0.0.1', port=6379, db=0)

def get_robot_and_obstacle_pose():
    gps_values = gps.getValues()
    ax, ay = gps_values[0], gps_values[1]
    rpy = imu.getRollPitchYaw()  # [roll, pitch, yaw]
    yaw = rpy[2]  # Góc quay quanh trục Z
    yaw = round(yaw, 6) + 1.570798
    normalized_yaw = math.atan2(math.sin(yaw), math.cos(yaw))
    message = {
        "aprila_tags": [
            {
                "timestame": time.time_ns(),
                "camera_id": random.randint(1, 10),
                "object_id": random.randint(1, 10),
                "yaw": normalized_yaw,
                "center": [ax, ay],
                "corners": []
            }
        ],
        "objects": [
            {
                "timestame": time.time_ns(),
                "camera_id": random.randint(1, 10),
                "class": "static/dynamic",
                "conf": 0.95,
                "center": [
                    random.randint(1, 10),
                    random.randint(1, 10),
                ],
                "corners": [
                    [
                        "x1",
                        "y1"
                    ],
                    [
                        "x2",
                        "y2"
                    ],
                    [
                        "x3",
                        "y3"
                    ],
                    [
                        "x4",
                        "y4"
                    ]
                ]
        }
        ]
    }
    redis_client.xadd("robot_obstacle_pose_stream", {"data": json.dumps(message)})

get_robot_and_obstacle_pose()
@app.route('/current-robot-pose', methods=['GET'])
def current_robot_pose():
    x, y = get_current_robot_pose()
    return {"status": "ok", "x": x, "y": y}


@app.route('/current-robot-heading', methods=['GET'])
def current_robot_heading():
    z = get_current_robot_heading()
    return {"status": "ok", "heading": z}

def run_flask():
    app.run(host='0.0.0.0', port=6000, debug=False, use_reloader=False)

threading.Thread(target=run_flask, daemon=True).start()

print('Controller started with API server at http://localhost:6000/control')

# ==== Vòng lặp chính ====
while robot.step(timestep) != -1:
    x, y = get_current_robot_pose()
    z = get_current_robot_heading()
    # print(f"[GPS] Position: x={x:.4f}, y={y:.4f}, z={z:.6f}")
    set_motion(current_command)
