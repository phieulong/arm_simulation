# Copyright 1996-2024 Cyberbotics Ltd.
# Licensed under the Apache License, Version 2.0
import asyncio
import math
from controller import Robot
from asyncua import Server, ua
import threading, time
from datetime import datetime
from flask import Flask

# ===== Constants (giữ nguyên như C) =====

TURN_WHEEL_SPEED=-0.1
FORWARD_FAST_SPEED = 3
FORWARD_NORMAL_SPEED = 4.0
FORWARD_SLOW_SPEED = 10
BACKWARD_SPEED = 4.0
TURN_SPEED = 0.5

# ===== Robot init =====
robot = Robot()
time_step = int(robot.getBasicTimeStep())

# ===== Motors =====
motor_back_left_joint = robot.getDevice("back_left_wheel_joint")
motor_back_right_joint = robot.getDevice("back_right_wheel_joint")
motor_front_joint = robot.getDevice("front_wheel_joint")

motor_back_left_joint.setPosition(float("inf"))
motor_back_right_joint.setPosition(float("inf"))
motor_front_joint.setPosition(float("inf"))

motor_back_left_joint.setVelocity(0.0)
motor_back_right_joint.setVelocity(0.0)
motor_front_joint.setVelocity(0.0)

motor_back_left = robot.getDevice("back_left_wheel")
motor_back_right = robot.getDevice("back_right_wheel")
motor_front = robot.getDevice("front_wheel")

motor_back_left.setPosition(float("inf"))
motor_back_right.setPosition(float("inf"))
motor_front.setPosition(float("inf"))

motor_back_left.setVelocity(0.0)
motor_back_right.setVelocity(0.0)
motor_front.setVelocity(0.0)

# ===== Sensors =====
depth_camera = robot.getDevice("depth_camera")
depth_camera.enable(time_step)

camera = robot.getDevice("camera")
camera.enable(time_step)

gps = robot.getDevice("gps")
gps.enable(time_step)

imu = robot.getDevice("inertial unit")
imu.enable(time_step)

robot_front_wheel_radian_sensor = robot.getDevice("front_caster_joint_sensor")
robot_front_wheel_radian_sensor.enable(time_step)

robot_back_left_wheel_radian_sensor = robot.getDevice("back_left_caster_joint_sensor")
robot_back_left_wheel_radian_sensor.enable(time_step)

robot_back_right_wheel_radian_sensor = robot.getDevice("back_right_caster_joint_sensor")
robot_back_right_wheel_radian_sensor.enable(time_step)



# ==== Hàm set tốc độ ====
def set_velocity(left_speed, right_speed, front_seed):
    motor_back_left.setVelocity(left_speed)
    motor_back_right.setVelocity(right_speed)
    motor_front.setVelocity(front_seed)

def move_forward_fast():
    set_velocity(FORWARD_NORMAL_SPEED, FORWARD_NORMAL_SPEED, FORWARD_NORMAL_SPEED)

def move_forward_normal():
    set_velocity(FORWARD_NORMAL_SPEED, FORWARD_NORMAL_SPEED, FORWARD_NORMAL_SPEED)

def move_forward_slow():
    set_velocity(FORWARD_NORMAL_SPEED, FORWARD_NORMAL_SPEED, FORWARD_NORMAL_SPEED)

def move_backward():
    set_velocity(-BACKWARD_SPEED, -BACKWARD_SPEED, -BACKWARD_SPEED)

def stop():
    set_velocity(0, 0, 0)

def set_motion(command):
    if command == "forward-slow":
        move_forward_slow()
    elif command == "forward":
        move_forward_normal()
    elif command == "forward-fast":
        move_forward_fast()
    elif command == "stop":
        stop()
        return

# ====== Hàm đọc sensors ======

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

def get_current_robot_front_wheel_in_radiant():
    value = robot_front_wheel_radian_sensor.getValue()
    value  = (round((value - math.pi) % (2 * math.pi) - math.pi, 2)) * -1
    # print(f"Getting front wheel radian: {value}")
    return value

def get_current_robot_back_left_wheel_in_radiant():
    value = robot_back_left_wheel_radian_sensor.getValue()
    value  = (round((value  - math.pi) % (2 * math.pi) - math.pi, 2)) * -1
    # print(f"Getting back left wheel radian: {value}")
    return value

def get_current_robot_back_right_wheel_in_radiant():
    value = robot_back_right_wheel_radian_sensor.getValue()
    value  = (round((value  - math.pi) % (2 * math.pi) - math.pi, 2)) * -1
    # print(f"Getting back right wheel radian: {value}")
    return value

# ==== OPC UA Server ====
class RobotCommandServer:
    def __init__(self, endpoint="opc.tcp://0.0.0.0:4840/freeopcua/server/"):
        self.sub = None
        self.server = Server()
        self.endpoint = endpoint
        self.forward_var = None
        self.namespace_idx = None
        self.all_wheel_speed_var = None
        self.turn_front_var = None
        self.is_monitoring = False
        self.monitor_thread = None
        self.last_processed_command = None  # Để tránh xử lý command trùng lặp

    async def setup_server(self):
        """Initialize server configuration"""
        await self.server.init()
        self.server.set_endpoint(self.endpoint)
        self.server.set_security_policy([ua.SecurityPolicyType.NoSecurity])

        uri = "http://example.org"
        self.namespace_idx = await self.server.register_namespace(uri)

        objects = self.server.get_objects_node()
        robot_device = await objects.add_object(self.namespace_idx, "RobotController")

        # Biến tốc độ đi thẳng
        self.all_wheel_speed_var = await robot_device.add_variable(
            self.namespace_idx,
            "AllWheelSpeed",
            ua.Variant(0.0, ua.VariantType.Float)
        )
        await self.all_wheel_speed_var.set_writable(True)


        print(f"AllWheelSpeed NodeId: NamespaceId { self.all_wheel_speed_var.nodeid.NamespaceIndex}, NodeId {self.all_wheel_speed_var.nodeid.Identifier}", )

    async def setup_internal_subscription(self):
        class Handler:
            @staticmethod
            def datachange_notification(node, val, data):
                print("🚨 VALUE CHANGED")
                print("NodeId:", node.nodeid)
                print("Value:", val)

        handler = Handler()
        self.sub = await self.server.create_subscription(0, handler)

        await self.sub.subscribe_data_change(self.all_wheel_speed_var)

    async def run(self):
        await self.setup_server()
        await self.setup_internal_subscription()

        print("🤖 OPC UA Robot Server running...")
        async with self.server:
            while True:
                await asyncio.sleep(100)


# ==== API Server ====
app = Flask(__name__)
@app.route("/current-robot", methods=["GET"])
def get_current_robot():
    try:
        x, y = get_current_robot_pose()
        heading = get_current_robot_heading()

        current_robot = {
            "timestamp": time.time_ns(),
            "camera_id": 0,
            "object_id": 0,
            "yaw": heading,
            "center": [x, y],
            "corners": [],
        }
        return {"status": "ok", "robot": current_robot}
    except Exception as e:
        print(f"Redis publish error: {e}")


def run_flask():
    print("🌐 Starting Flask server on http://0.0.0.0:6000")
    app.run(host="0.0.0.0", port=6000, debug=False, use_reloader=False, threaded=True)


# ==== Khởi tạo servers ====
print("🤖 Starting Robot Controller...")

# Flask
flask_thread = threading.Thread(target=run_flask, daemon=True)
flask_thread.start()

# OPC UA
server = RobotCommandServer()
opcua_thread = threading.Thread(
    target=lambda: asyncio.run(server.run()),
    daemon=True
)
opcua_thread.start()

print("🔄 Starting Webots simulation loop...")

# 🔥 BẮT BUỘC
while robot.step(time_step) != -1:
    pass



