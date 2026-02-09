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
        Góc giữa mặt phẳng AprilTag và mặt phẳng camera (quay quanh trục Z - vertical)
        0° = AprilTag song song với camera
        ±90° = AprilTag vuông góc với camera
        Được tính từ rotation matrix của pose estimation

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

    # ---- 2. Camera parameters cho pose estimation ----
    camera_params = [
        CAMERA_FOCAL_LENGTH,  # fx
        CAMERA_FOCAL_LENGTH,  # fy
        CAMERA_WIDTH / 2.0,   # cx
        CAMERA_HEIGHT / 2.0   # cy
    ]

    # ---- 3. Detect AprilTag với pose estimation ----
    detections = _apriltag_detector.detect(
        gray,
        estimate_tag_pose=True,
        camera_params=camera_params,
        tag_size=APRILTAG_SIZE_CM / 100.0  # Chuyển sang mét
    )

    if len(detections) == 0:
        return None, None, None

    # ---- 4. Tính khoảng cách (chiều sâu) từ depth_camera ----
    width = range_finder.getWidth()
    height = range_finder.getHeight()
    range_image = range_finder.getRangeImage()
    depth_2d = np.array(range_image).reshape((height, width))
    cx = width // 2
    cy = height // 2
    distance_m = depth_2d[cy, cx]
    distance_cm = distance_m * 100

    # ---- 5. Lấy tag đầu tiên (hoặc chọn theo ID nếu cần) ----
    tag = detections[0]
    center_x, center_y = tag.center
    offset_px = center_x - (w / 2.0)

    # ---- 6. Chuyển offset từ pixel sang cm ----
    # Sử dụng similar triangles: offset_cm / distance_cm = offset_px / focal_length
    lateral_offset_cm = (offset_px * distance_cm) / CAMERA_FOCAL_LENGTH

    # ---- 7. Tính góc giữa mặt phẳng AprilTag và mặt phẳng camera ----
    if tag.pose_R is not None:
        # pose_R là rotation matrix 3x3 (tag frame -> camera frame)
        R = np.array(tag.pose_R)

        # Normal vector của AprilTag trong tag frame là [0, 0, 1]
        # Sau transform sang camera frame: normal_cam = R @ [0, 0, 1] = R[:, 2]
        normal_in_cam = R[:, 2]

        # Góc giữa normal vector và trục Z của camera (trục nhìn thẳng)
        # cos(angle) = normal_z / |normal| = normal_z (vì |normal| = 1 trong rotation matrix)
        # Góc = arccos(normal_z)
        normal_z = normal_in_cam[2]

        # Clamp để tránh lỗi numerical
        normal_z = np.clip(normal_z, -1.0, 1.0)

        # Góc giữa 2 mặt phẳng (0° = song song, 90° = vuông góc)
        angle_rad = np.arccos(normal_z)
        yaw_deg = np.degrees(angle_rad)

        # Để biết AprilTag nghiêng sang trái hay phải, dùng normal_x
        # Nếu muốn giá trị có dấu:
        # - Dương: nghiêng sang phải
        # - Âm: nghiêng sang trái
        if normal_in_cam[0] < 0:
            yaw_deg = -yaw_deg

        print(f"DEBUG Normal in camera frame: [{normal_in_cam[0]:.4f}, {normal_in_cam[1]:.4f}, {normal_in_cam[2]:.4f}]")
        print(f"DEBUG Angle between planes: {yaw_deg:.2f}°")
    else:
        # Fallback nếu pose estimation không thành công
        yaw_deg = 0.0

    print(f"RESULT: lateral={lateral_offset_cm:.2f}cm, yaw={yaw_deg:.2f}°, distance={distance_cm:.2f}cm")

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
motor_steering_front = robot.getDevice("front_wheel_joint")
motor_steering_back = robot.getDevice("back_wheel_joint")
motor_steering_middle = robot.getDevice("middle_wheel_joint")

motor_steering_front.setPosition(float("inf"))
motor_steering_back.setPosition(float("inf"))
motor_steering_middle.setPosition(float("inf"))

motor_steering_front.setVelocity(0.0)
motor_steering_back.setVelocity(0.0)
motor_steering_middle.setVelocity(0.0)

motor_front = robot.getDevice("front_wheel")
motor_back = robot.getDevice("back_wheel")
motor_middle = robot.getDevice("middle_wheel")

motor_front.setPosition(float("inf"))
motor_back.setPosition(float("inf"))
motor_middle.setPosition(float("inf"))

motor_front.setVelocity(0.0)
motor_back.setVelocity(0.0)
motor_middle.setVelocity(0.0)

# ===== Sensors =====


range_finder = robot.getDevice("depth_camera")
range_finder.enable(time_step)


camera = robot.getDevice("camera")
camera.enable(time_step)

gps = robot.getDevice("gps")
gps.enable(time_step)

imu = robot.getDevice("inertial unit")
imu.enable(time_step)

front_steering_angle = robot.getDevice("front_caster_joint_sensor")
front_steering_angle.enable(time_step)

back_steering_angle = robot.getDevice("back_caster_joint_sensor")
back_steering_angle.enable(time_step)

middle_steering_angle = robot.getDevice("middle_caster_joint_sensor")
middle_steering_angle.enable(time_step)

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

class FrontMonitorWheelAngleTask(MonitorTask):
    """Task để monitor góc bánh xe"""
    def __init__(self, target_radian, callback=None):
        super().__init__()
        self.target_radian = target_radian
        self.callback = callback

    def check_and_execute(self):
        current_angle = get_current_front_wheel_angel()

        if abs(current_angle - self.target_radian) <= 0.006:
            # Đạt mục tiêu - dừng motor
            motor_steering_front.setVelocity(0)
            self.completed = True
            if self.callback is not None:
                self.callback()
            return True
        return False

class BackMonitorWheelAngleTask(MonitorTask):
    """Task để monitor góc bánh xe"""
    def __init__(self, target_radian, callback=None):
        super().__init__()
        self.target_radian = target_radian
        self.callback = callback

    def check_and_execute(self):
        current_angle = get_current_back_wheel_angel()

        if abs(current_angle - self.target_radian) <= 0.006:
            # Đạt mục tiêu - dừng motor
            motor_steering_back.setVelocity(0)
            self.completed = True
            if self.callback is not None:
                self.callback()
            return True
        return False

class MiddleMonitorWheelAngleTask(MonitorTask):
    """Task để monitor góc bánh xe"""
    def __init__(self, target_radian, callback=None):
        super().__init__()
        self.target_radian = target_radian
        self.callback = callback

    def check_and_execute(self):
        current_angle = get_current_middle_wheel_angel()

        if abs(current_angle - self.target_radian) <= 0.006:
            # Đạt mục tiêu - dừng motor
            motor_steering_middle.setVelocity(0)
            self.completed = True
            if self.callback is not None:
                self.callback()
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
    motor_back.setVelocity(left_speed)
    motor_middle.setVelocity(right_speed)
    motor_front.setVelocity(front_seed)



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

def get_current_front_wheel_angel():
    value = front_steering_angle.getValue()
    value  = (round((value - math.pi) % (2 * math.pi) - math.pi, 2)) * -1
    return value

def get_current_back_wheel_angel():
    value = back_steering_angle.getValue()
    value  = (round((value  - math.pi) % (2 * math.pi) - math.pi, 2)) * -1
    return value

def get_current_middle_wheel_angel():
    value = middle_steering_angle.getValue()
    value  = (round((value  - math.pi) % (2 * math.pi) - math.pi, 2)) * -1
    return value

# ==== OPC UA Server ====
class RobotCommandServer:
    def __init__(self, endpoint="opc.tcp://0.0.0.0:4840/freeopcua/server/"):
        self.frontSpeedSub = None
        self.middleSpeedSub = None
        self.backSpeedSub = None

        self.frontSteeringSub = None
        self.middleSteeringSub = None
        self.backSteeringSub = None

        self.front_speed_var = None
        self.middle_speed_var = None
        self.back_speed_var = None

        self.front_steering_var = None
        self.middle_steering_var = None
        self.back_steering_var = None

        # Biến để đọc góc hiện tại của bánh xe (read-only)
        self.front_angle_sensor_var = None
        self.middle_angle_sensor_var = None
        self.back_angle_sensor_var = None

        self.server = Server()
        self.endpoint = endpoint
        self.forward_var = None
        self.namespace_idx = None

        self.is_monitoring = False
        self.monitor_thread = None
        self.last_processed_command = None  # Để tránh xử lý command trùng lặp
        self.loop = None  # Lưu event loop để gọi async functions từ thread khác

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
        self.front_speed_var = await robot_device.add_variable(
            self.namespace_idx,
            "FrontMotorSpeed",
            ua.Variant(0.0, ua.VariantType.Float)
        )
        await self.front_speed_var.set_writable(True)
        print(f"FrontMotorSpeed NodeId: NamespaceId { self.front_speed_var.nodeid.NamespaceIndex}, NodeId {self.front_speed_var.nodeid.Identifier}", )

        # Biến tốc độ đi thẳng
        self.middle_speed_var = await robot_device.add_variable(
            self.namespace_idx,
            "MiddleMotorSpeed",
            ua.Variant(0.0, ua.VariantType.Float)
        )
        await self.middle_speed_var.set_writable(True)
        print(f"MiddleMotorSpeed NodeId: NamespaceId { self.middle_speed_var.nodeid.NamespaceIndex}, NodeId {self.middle_speed_var.nodeid.Identifier}", )

        # Biến tốc độ đi thẳng
        self.back_speed_var = await robot_device.add_variable(
            self.namespace_idx,
            "BackMotorSpeed",
            ua.Variant(0.0, ua.VariantType.Float)
        )
        await self.back_speed_var.set_writable(True)
        print(f"BackMotorSpeed NodeId: NamespaceId {self.back_speed_var.nodeid.NamespaceIndex}, NodeId {self.back_speed_var.nodeid.Identifier}", )

        # Biến điều khiển quay bánh
        self.front_steering_var = await robot_device.add_variable(
            self.namespace_idx,
            "FrontSteering",
            ua.Variant(0.0, ua.VariantType.Float)
        )
        await self.front_steering_var.set_writable(True)
        print(f"FrontSteering NodeId: NamespaceId { self.front_steering_var.nodeid.NamespaceIndex}, NodeId {self.front_steering_var.nodeid.Identifier}", )

        # Biến điều khiển quay bánh
        self.middle_steering_var = await robot_device.add_variable(
            self.namespace_idx,
            "MiddleSteering",
            ua.Variant(0.0, ua.VariantType.Float)
        )
        await self.middle_steering_var.set_writable(True)
        print(f"MiddleSteering NodeId: NamespaceId { self.middle_steering_var.nodeid.NamespaceIndex}, NodeId {self.middle_steering_var.nodeid.Identifier}", )

        # Biến điều khiển quay bánh
        self.back_steering_var = await robot_device.add_variable(
            self.namespace_idx,
            "BackSteering",
            ua.Variant(0.0, ua.VariantType.Float)
        )
        await self.back_steering_var.set_writable(True)
        print(
            f"BackSteering NodeId: NamespaceId {self.back_steering_var.nodeid.NamespaceIndex}, NodeId {self.back_steering_var.nodeid.Identifier}", )

        # Biến đọc góc hiện tại của bánh xe (read-only cho client)
        self.front_angle_sensor_var = await robot_device.add_variable(
            self.namespace_idx,
            "FrontAngleSensor",
            ua.Variant(0.0, ua.VariantType.Float)
        )
        await self.front_angle_sensor_var.set_writable(False)
        print(f"FrontAngleSensor NodeId: NamespaceId {self.front_angle_sensor_var.nodeid.NamespaceIndex}, NodeId {self.front_angle_sensor_var.nodeid.Identifier}")

        self.middle_angle_sensor_var = await robot_device.add_variable(
            self.namespace_idx,
            "MiddleAngleSensor",
            ua.Variant(0.0, ua.VariantType.Float)
        )
        await self.middle_angle_sensor_var.set_writable(False)
        print(f"MiddleAngleSensor NodeId: NamespaceId {self.middle_angle_sensor_var.nodeid.NamespaceIndex}, NodeId {self.middle_angle_sensor_var.nodeid.Identifier}")

        self.back_angle_sensor_var = await robot_device.add_variable(
            self.namespace_idx,
            "BackAngleSensor",
            ua.Variant(0.0, ua.VariantType.Float)
        )
        await self.back_angle_sensor_var.set_writable(False)
        print(
            f"BackAngleSensor NodeId: NamespaceId {self.back_angle_sensor_var.nodeid.NamespaceIndex}, NodeId {self.back_angle_sensor_var.nodeid.Identifier}")

    async def setup_internal_subscription(self):
        class FrontSpeedHandler:
            @staticmethod
            def datachange_notification(node, val, data):
                print("🚨 FRONT SPEEDS CHANGED")
                print("NodeId:", node.nodeid)
                print("Value:", val)
                motor_front.setVelocity(val)

        class BackSpeedHandler:
            @staticmethod
            def datachange_notification(node, val, data):
                print("🚨BACK SPEEDS CHANGED")
                print("NodeId:", node.nodeid)
                print("Value:", val)
                motor_back.setVelocity(val)

        class MiddleSpeedHandler:
            @staticmethod
            def datachange_notification(node, val, data):
                print("🚨 MIDDLE SPEEDS CHANGED")
                print("NodeId:", node.nodeid)
                print("Value:", val)
                motor_middle.setVelocity(val)

        class FrontSteeringHandler:
            @staticmethod
            def datachange_notification(node, val, data):
                print("🚨 FRONT STEERING CHANGED")
                print("NodeId:", node.nodeid)
                print("Value:", val)
                # current = get_current_front_wheel_angel()
                # target = val/180 * math.pi
                motor_steering_front.setVelocity(val)

        class BackSteeringHandler:
            @staticmethod
            def datachange_notification(node, val, data):
                print("🚨 BACK STEERING CHANGED")
                print("NodeId:", node.nodeid)
                print("Value:", val)
                # current = get_current_back_wheel_angel()
                # target = val/180 * math.pi
                # speed = shortest_rotation_direction_rad(current, target) * TURN_WHEEL_SPEED
                motor_steering_back.setVelocity(val)


        class MiddleSteeringHandler:
            @staticmethod
            def datachange_notification(node, val, data):
                print("🚨 MIDDLE STEERING CHANGED")
                print("NodeId:", node.nodeid)
                print("Value:", val)
                # current = get_current_middle_wheel_angel()
                # target = val/180 * math.pi
                # speed = shortest_rotation_direction_rad(current, target) * TURN_WHEEL_SPEED
                motor_steering_middle.setVelocity(val)

        front_speed_handler = FrontSpeedHandler()
        middle_speed_handler = MiddleSpeedHandler()
        back_speed_handler = BackSpeedHandler()
        front_steering_handler = FrontSteeringHandler()
        middle_steering_handler = MiddleSteeringHandler()
        back_steering_handler = BackSteeringHandler()

        self.frontSpeedSub = await self.server.create_subscription(0, front_speed_handler)
        self.middleSpeedSub = await self.server.create_subscription(0, middle_speed_handler)
        self.backSpeedSub = await self.server.create_subscription(0, back_speed_handler)
        self.frontSteeringSub = await self.server.create_subscription(0, front_steering_handler)
        self.middleSteeringSub = await self.server.create_subscription(0, middle_steering_handler)
        self.backSteeringSub = await self.server.create_subscription(0, back_steering_handler)

        await self.frontSpeedSub.subscribe_data_change(self.front_speed_var)
        await self.backSpeedSub.subscribe_data_change(self.back_speed_var)
        await self.middleSpeedSub.subscribe_data_change(self.middle_speed_var)
        await self.frontSteeringSub.subscribe_data_change(self.front_steering_var)
        await self.backSteeringSub.subscribe_data_change(self.back_steering_var)
        await self.middleSteeringSub.subscribe_data_change(self.middle_steering_var)


    async def update_wheel_angles(self):
        """Cập nhật giá trị góc bánh xe từ sensors vào OPC UA variables"""
        try:
            front_angle = get_current_front_wheel_angel()
            back_angle = get_current_back_wheel_angel()
            middle_angle = get_current_middle_wheel_angel()

            # Chuyển từ radian sang độ để dễ đọc
            front_angle_deg = front_angle * 180 / math.pi
            back_angle_deg = back_angle * 180 / math.pi
            middle_angle_deg = middle_angle * 180 / math.pi

            await self.front_angle_sensor_var.write_value(ua.Variant(front_angle_deg, ua.VariantType.Float))
            await self.back_angle_sensor_var.write_value(ua.Variant(back_angle_deg, ua.VariantType.Float))
            await self.middle_angle_sensor_var.write_value(ua.Variant(middle_angle_deg, ua.VariantType.Float))
        except Exception as e:
            print(f"Error updating wheel angles: {e}")

    def update_wheel_angles_sync(self):
        """Wrapper để gọi update_wheel_angles từ thread khác (main loop)"""
        if self.loop and self.loop.is_running():
            asyncio.run_coroutine_threadsafe(self.update_wheel_angles(), self.loop)

    async def run(self):
        self.loop = asyncio.get_event_loop()  # Lưu event loop
        await self.setup_server()
        await self.setup_internal_subscription()

        print("🤖 OPC UA Robot Server running...")
        async with self.server:
            while True:
                await asyncio.sleep(100)

def normalize_angle_rad(angle):
    """
    Chuẩn hoá góc về [-pi, pi)
    """
    return (angle + math.pi) % (2 * math.pi) - math.pi


def shortest_rotation_direction_rad(current, target):
    current = normalize_angle_rad(current)
    target = normalize_angle_rad(target)
    delta = normalize_angle_rad(target - current)
    if delta == 0:
        return 0
    return -1 if delta < 0 else 1


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
APRILTAG_CHECK_INTERVAL = 0  # Kiểm tra mỗi 10 timesteps

# 🔥 Main loop - Xử lý tất cả tasks với robot.step thay vì time.sleep
while robot.step(time_step) != -1:
    # Xử lý các monitoring tasks trong mỗi timestep

    # Cập nhật giá trị góc bánh xe vào OPC UA variables
    server.update_wheel_angles_sync()

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
