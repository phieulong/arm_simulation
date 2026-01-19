# Copyright 1996-2024 Cyberbotics Ltd.
# Licensed under the Apache License, Version 2.0
import asyncio
import math
from controller import Robot
from asyncua import Server, ua
import threading, time
from flask import Flask
import cv2
import numpy as np
from pupil_apriltags import Detector
import redis
import json

# ==== Redis Client ====
redis_client = redis.Redis(host="192.168.0.71", port=26379, db=0, decode_responses=True)
REDIS_APRILTAG_CHANNEL = "apriltag_detection_194"
REDIS_APRILTAG_KEY = "apriltag_latest"

# ==== Imports và khởi tạo apriltag dectector ====

# Khởi tạo detector (nên tạo 1 lần, không tạo lại mỗi frame)
_apriltag_detector = Detector(
    families="tag36h11",
    nthreads=4,
    quad_decimate=1.0,
    quad_sigma=0.0,
    refine_edges=True,
    decode_sharpening=0.25,
    debug=False
)


# Camera intrinsic parameters
# fieldOfView trong robot = 2.05949 radian = 117.99 độ
# Camera resolution: 1280 x 720
CAMERA_FOV_RAD = 2.05949
CAMERA_WIDTH = 1280
CAMERA_HEIGHT = 720

# Tính focal length (pixels) từ FOV
# focal_length = (width / 2) / tan(fov / 2)
CAMERA_FOCAL_LENGTH = (CAMERA_WIDTH / 2.0) / np.tan(CAMERA_FOV_RAD / 2.0)

# Kích thước thực của AprilTag (cm) - điều chỉnh theo tag thực tế
APRILTAG_SIZE_CM = 5  # Kích thước cạnh của AprilTag (cm)

def analyze_apriltag_offset(image):
    """
    Phân tích vị trí AprilTag và trả về khoảng cách lệch trái/phải thực tế (cm) và chiều sâu

    Parameters
    ----------
    image : np.ndarray
        Ảnh đầu vào (BGR hoặc grayscale)
    depth_image : np.ndarray | None
        Ảnh depth từ depth_camera (float32, đơn vị mét)
        Nếu None, sẽ ước lượng khoảng cách từ kích thước tag

    Returns
    -------
    lateral_offset_cm : float | None
        Khoảng cách lệch trái/phải so với tâm camera (cm)
        (+) AprilTag ở bên phải tâm camera -> robot cần di chuyển sang phải
        (-) AprilTag ở bên trái tâm camera -> robot cần di chuyển sang trái
        Giá trị này KHÔNG THAY ĐỔI khi robot di chuyển tịnh tiến (nếu không lệch góc)

    yaw_deg : float | None
        Góc xoay của AprilTag so với camera (độ)
        0° = AprilTag vuông góc với camera
        (+) xoay ngược chiều kim đồng hồ

    distance_cm : float | None
        Khoảng cách (chiều sâu) từ camera đến AprilTag (cm)
        Đọc từ depth_camera nếu có, nếu không sẽ ước lượng từ kích thước tag
    """

    # ---- 1. Chuyển sang grayscale nếu cần ----
    if len(image.shape) == 3:
        gray = cv2.cvtColor(image, cv2.COLOR_BGR2GRAY)
    else:
        gray = image
    h, w = gray.shape

    # ---- 2. Detect AprilTag ----
    detections = _apriltag_detector.detect(gray)
    if len(detections) == 0:
        return None, None, None

    # ---- 3. Tính khoảng cách (chiều sâu) từ depth_camera ----
    width = range_finder.getWidth()
    height = range_finder.getHeight()
    range_image = range_finder.getRangeImage()
    depth_2d = np.array(range_image).reshape((height, width))
    cx = width // 2
    cy = height // 2
    distance_m = depth_2d[cy, cx]
    distance_cm = distance_m * 100

    # ---- 4. Lấy tag đầu tiên (hoặc chọn theo ID nếu cần) ----
    tag = detections[0]
    corners = tag.corners
    cx, cy = tag.center
    offset_px = cx - (w / 2.0)

    # ---- 5. Chuyển offset từ pixel sang cm ----
    # Sử dụng similar triangles: offset_cm / distance_cm = offset_px / focal_length
    lateral_offset_cm = (offset_px * distance_cm) / CAMERA_FOCAL_LENGTH

    # ---- 6. Lệch góc (yaw) của AprilTag ----
    # vector cạnh trên của tag: corner 0 -> corner 1
    v = corners[1] - corners[0]
    yaw_rad = np.arctan2(v[1], v[0])
    yaw_deg = np.degrees(yaw_rad)
    print(lateral_offset_cm, yaw_deg, distance_cm)

    return lateral_offset_cm, yaw_deg, distance_cm

# ===== Constants (giữ nguyên như C) =====

TURN_WHEEL_SPEED=-0.3
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


range_finder = robot.getDevice("depth_camera")
range_finder.enable(time_step)


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

# ===== Task Queue cho Main Loop =====
# Thay thế time.sleep bằng hệ thống task-based
class MonitorTask:
    """Base class cho các monitoring task"""
    def __init__(self):
        self.completed = False
        self.callback = None

    def check_and_execute(self):
        """Kiểm tra điều kiện và thực thi - gọi trong main loop"""
        raise NotImplementedError

    def is_completed(self):
        return self.completed

class MonitorWheelAngleTask(MonitorTask):
    """Task để monitor góc bánh xe"""
    def __init__(self, target_radian, callback=None):
        super().__init__()
        self.target_radian = target_radian
        self.callback = callback

    def check_and_execute(self):
        current_angle = get_current_robot_front_wheel_in_radiant()
        # print(f"Current front wheel angle: {current_angle} with target {self.target_radian}")

        if abs(current_angle - self.target_radian) <= 0.006:
            # Đạt mục tiêu - dừng motor
            motor_front_joint.setVelocity(0)
            motor_back_left_joint.setVelocity(0)
            motor_back_right_joint.setVelocity(0)
            self.completed = True
            if self.callback is not None:
                self.callback()
            return True
        return False

class MonitorBodyHeadingTask(MonitorTask):
    """Task để monitor heading của robot"""
    def __init__(self, initial_heading, target_radian, callback=None):
        super().__init__()
        self.initial_heading = initial_heading
        self.target_radian = target_radian
        self.callback = callback

    def check_and_execute(self):
        current_heading = get_current_robot_heading()
        heading_diff = abs(abs(current_heading) - abs(self.initial_heading))

        if abs(heading_diff - self.target_radian) <= 0.006:
            set_velocity(0, 0, 0)
            self.completed = True
            # Quay bánh xe về vị trí ban đầu
            add_turn_all_wheels_task(0, -1, self.callback)
            return True
        return False

# Danh sách các task đang chạy
active_tasks = []
task_lock = threading.Lock()

def add_task(task):
    """Thêm task vào queue - thread-safe"""
    with task_lock:
        active_tasks.append(task)

def process_tasks():
    """Xử lý tất cả các task trong main loop"""
    with task_lock:
        # Lọc bỏ các task đã hoàn thành
        tasks_to_process = active_tasks.copy()

    completed_tasks = []
    for task in tasks_to_process:
        if task.check_and_execute():
            completed_tasks.append(task)

    # Xóa các task đã hoàn thành
    with task_lock:
        for task in completed_tasks:
            if task in active_tasks:
                active_tasks.remove(task)

# ==== Hàm set tốc độ ====
def set_velocity(left_speed, right_speed, front_seed):
    motor_back_left.setVelocity(left_speed)
    motor_back_right.setVelocity(right_speed)
    motor_front.setVelocity(front_seed)

def add_turn_all_wheels_task(turn_radian, turn_direction = 1, callback=None):
    """Thêm task để quay tất cả bánh xe"""
    print(f"Turning all wheels... {turn_radian}")
    speed = (TURN_WHEEL_SPEED / 2 * 1.5 ) * turn_direction

    motor_front_joint.setVelocity(speed)
    motor_back_left_joint.setVelocity(speed)
    motor_back_right_joint.setVelocity(speed)

    # Thêm task để monitor
    task = MonitorWheelAngleTask(turn_radian, callback)
    add_task(task)

def turn_body(turn_radian):
    def start_turn_body():
        set_velocity(-FORWARD_SLOW_SPEED/10, -FORWARD_SLOW_SPEED/10, FORWARD_SLOW_SPEED/5)
        current_heading = get_current_robot_heading()
        print(f"Current heading: {current_heading}")
        print(f"Target heading: {turn_radian}")

        # Thêm task để monitor body heading
        task = MonitorBodyHeadingTask(current_heading, turn_radian, None)
        add_task(task)

    add_turn_all_wheels_task(turn_radian, 1,  start_turn_body)


# ====== Hàm đọc sensors ======

# Cache cho GPS và IMU data
last_gps_update = 0
last_imu_update = 0
cached_gps_data = (0, 0)
cached_heading = 0
GPS_UPDATE_INTERVAL = 0.0
IMU_UPDATE_INTERVAL = 0.3

def get_current_robot_pose():
    global last_gps_update, cached_gps_data
    current_time = time.time()
    gps_values = gps.getValues()
    return gps_values[0], gps_values[1]

def get_current_robot_heading():
    global last_imu_update, cached_heading
    current_time = time.time()
    rpy = imu.getRollPitchYaw()
    yaw = rpy[2]
    yaw = round(yaw, 6) + 1.570798
    return math.atan2(math.sin(yaw), math.cos(yaw))

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
        self.allWheelSpeedSub = None
        self.bodyTurnSub = None
        self.frontWheelSpeedSub = None
        self.backWheelsSpeedSub = None
        self.turnAllWheelsSub = None
        self.server = Server()
        self.endpoint = endpoint
        self.forward_var = None
        self.namespace_idx = None
        self.all_wheel_speed_var = None
        self.body_turn_var = None
        self.front_wheel_speed_var = None
        self.back_wheels_speed_var = None
        self.turn_all_wheels_var = None
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

        # Biến điều khiển quay 3 bánh
        self.body_turn_var = await robot_device.add_variable(
            self.namespace_idx,
            "BodyTurn",
            ua.Variant(0.0, ua.VariantType.Float)
        )
        await self.body_turn_var.set_writable(True)
        print(f"BodyTurn NodeId: NamespaceId { self.body_turn_var.nodeid.NamespaceIndex}, NodeId {self.body_turn_var.nodeid.Identifier}", )

        self.front_wheel_speed_var = await robot_device.add_variable(
            self.namespace_idx,
            "FrontWheelSpeed",
            ua.Variant(0.0, ua.VariantType.Float)
        )
        await self.front_wheel_speed_var.set_writable(True)
        print(f"FrontWheelSpeed NodeId: NamespaceId { self.front_wheel_speed_var.nodeid.NamespaceIndex}, NodeId {self.front_wheel_speed_var.nodeid.Identifier}", )

        self.back_wheels_speed_var = await robot_device.add_variable(
            self.namespace_idx,
            "FrontWheelSpeed",
            ua.Variant(0.0, ua.VariantType.Float)
        )
        await self.back_wheels_speed_var.set_writable(True)
        print(f"FrontWheelSpeed NodeId: NamespaceId { self.back_wheels_speed_var.nodeid.NamespaceIndex}, NodeId {self.back_wheels_speed_var.nodeid.Identifier}", )

        self.turn_all_wheels_var = await robot_device.add_variable(
            self.namespace_idx,
            "TurnAllWheels",
            ua.Variant(0.0, ua.VariantType.Float)
        )
        await self.turn_all_wheels_var.set_writable(True)
        print(f"TurnAllWheels NodeId: NamespaceId { self.turn_all_wheels_var.nodeid.NamespaceIndex}, NodeId {self.turn_all_wheels_var.nodeid.Identifier}", )


    async def setup_internal_subscription(self):
        class AllWheelSpeedHandler:
            @staticmethod
            def datachange_notification(node, val, data):
                print("🚨 ALL WHEEL SPEEDS CHANGED")
                print("NodeId:", node.nodeid)
                print("Value:", val)
                set_velocity(val*0.93, val*0.93, val)

        class BodyTurnHandler:
            @staticmethod
            def datachange_notification(node, val, data):
                print("🚨 BODY TURN CHANGED")
                print("NodeId:", node.nodeid)
                print("Value:", val)
                if val is not None and val != 0.0:
                    radiant = val / 180 * math.pi
                    turn_body(radiant)

        class FrontWheelSpeedHandler:
            @staticmethod
            def datachange_notification(node, val, data):
                print("🚨 FRONT WHEEL SPEED CHANGED")
                print("NodeId:", node.nodeid)
                print("Value:", val)
                motor_front.setVelocity(val)

        class BackWheelsSpeedHandler:
            @staticmethod
            def datachange_notification(node, val, data):
                print("🚨 BACK WHEELS SPEED CHANGED")
                print("NodeId:", node.nodeid)
                print("Value:", val)
                motor_back_right.setVelocity(val)
                motor_back_left.setVelocity(val)


        class TurnAllWheelsHandler:
            @staticmethod
            def datachange_notification(node, val, data):
                print("🚨 TURN ALL WHEELS CHANGED")
                print("NodeId:", node.nodeid)
                print("Value:", val)
                if val is not None and val != 0.0:
                    radiant = val / 180 * math.pi
                    add_turn_all_wheels_task(radiant, 1, None)

        all_wheel_speed_handler = AllWheelSpeedHandler()
        body_turn_handler = BodyTurnHandler()
        front_wheel_speed_handler = FrontWheelSpeedHandler()
        back_wheels_speed_handler = BackWheelsSpeedHandler()
        turn_all_wheels_handler = TurnAllWheelsHandler()

        self.allWheelSpeedSub = await self.server.create_subscription(0, all_wheel_speed_handler)
        self.bodyTurnSub = await self.server.create_subscription(0, body_turn_handler)
        self.frontWheelSpeedSub = await self.server.create_subscription(0, front_wheel_speed_handler)
        self.backWheelsSpeedSub = await self.server.create_subscription(0, back_wheels_speed_handler)
        self.turnAllWheelsSub = await self.server.create_subscription(0, turn_all_wheels_handler)

        await self.allWheelSpeedSub.subscribe_data_change(self.all_wheel_speed_var)
        await self.bodyTurnSub.subscribe_data_change(self.body_turn_var)
        await self.frontWheelSpeedSub.subscribe_data_change(self.front_wheel_speed_var)
        await self.backWheelsSpeedSub.subscribe_data_change(self.back_wheels_speed_var)
        await self.turnAllWheelsSub.subscribe_data_change(self.turn_all_wheels_var)

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

# Lấy kích thước camera
camera_width = camera.getWidth()
camera_height = camera.getHeight()

# Counter để giảm tần suất xử lý apriltag (không cần mỗi frame)
apriltag_check_counter = 0
APRILTAG_CHECK_INTERVAL = 10  # Kiểm tra mỗi 10 timesteps

# 🔥 Main loop - Xử lý tất cả tasks với robot.step thay vì time.sleep
while robot.step(time_step) != -1:
    # Xử lý các monitoring tasks trong mỗi timestep
    process_tasks()

    # Xử lý AprilTag detection từ camera
    apriltag_check_counter += 1
    if apriltag_check_counter >= APRILTAG_CHECK_INTERVAL:
        apriltag_check_counter = 0

        # Lấy ảnh từ camera
        camera_data = camera.getImage()


        if camera_data:
            # Chuyển đổi từ raw bytes sang numpy array (BGRA format)
            image = np.frombuffer(camera_data, np.uint8).reshape((camera_height, camera_width, 4))
            # Chuyển từ BGRA sang BGR
            image_bgr = cv2.cvtColor(image, cv2.COLOR_BGRA2BGR)

            # Phân tích AprilTag với depth image
            lateral_offset_cm, yaw_deg, distance_cm = analyze_apriltag_offset(image_bgr)

            if lateral_offset_cm is not None:
                # Publish kết quả vào Redis
                try:
                    apriltag_data = {
                        "timestamp": time.time_ns(),
                        "lateral_offset_cm": round(float(lateral_offset_cm), 2),
                        "yaw_deg": round(float(yaw_deg), 2),
                        "distance_cm": round(float(distance_cm), 2),
                        "detected": True
                    }
                    apriltag_json = json.dumps(apriltag_data)

                    # Lưu vào key để có thể đọc bất cứ lúc nào
                    redis_client.set(REDIS_APRILTAG_KEY, apriltag_json)

                    # Publish vào channel để các subscriber nhận được realtime
                    redis_client.publish(REDIS_APRILTAG_CHANNEL, apriltag_json)
                except Exception as e:
                    print(f"❌ Redis publish error: {e}")
