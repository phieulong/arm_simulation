import json
from time import sleep

from controller import Supervisor
import redis
import threading
import time
from flask import Flask
import random

# --- Cấu hình bản đồ ---
API_URL = "http://localhost:6060/map"

last_redis_publish = 0
REDIS_PUBLISH_INTERVAL = (0.1, 0.3)
start, end = REDIS_PUBLISH_INTERVAL
random_delay = random.uniform(start, end)
# Khởi tạo Supervisor
root_supervisor = Supervisor()

# Cache cho map data
cached_map = None
cache_timestamp = 0
CACHE_DURATION = 100000  # Cache trong 1 giây
cache_lock = threading.Lock()

# Redis client
redis_client = redis.Redis(host='127.0.0.1', port=6379, db=0)

# Flask app
app = Flask(__name__)


def get_node_size(node):
    """Lấy kích thước của node dựa trên type"""
    node_type = '<unknown>'
    try:
        node_type = node.getTypeName()
        if node_type in ["ConveyorBelt", "WoodenPallet"]:
            return node.getField("size").getSFVec3f()
        elif node_type == 'WoodenPalletStack':
            return node.getField("palletSize").getSFVec3f()
        elif node_type == 'CardboardBox':
            # Giả sử CardboardBox có field "size"
            size_field = node.getField("size")
            if size_field:
                return size_field.getSFVec3f()
    except Exception as e:
        print(f"Error getting node size for {node_type}: {e}")
    return [0, 0, 0]


def calculate_bbox(translation, size):
    """Tính toán bounding box từ translation và size"""
    bx, by = translation[0], translation[1]
    bl, bw = size[0], size[1]

    return [
        [bx - bl / 2, by - bw / 2],  # bottom-left
        [bx - bl / 2, by + bw / 2],  # top-left
        [bx + bl / 2, by - bw / 2],  # bottom-right
        [bx + bl / 2, by + bw / 2],  # top-right
    ]


def parse_robot_message(message):
    import json
    data = json.loads(message.decode())
    return data.get("robot", {})

def fetch_current_robot_api(robot_id=0, timeout=2):
    import urllib.request
    import urllib.error
    """
    Fetch the current robot from the local API `GET /current-robot`.
    Returns the robot dict on success or an empty dict on failure.
    Uses urllib to avoid needing the `requests` package.
    The function expects the target service to be on port 6060 (the map/HTTP server).
    """
    url = f"http://localhost:600{robot_id}/current-robot"
    try:
        req = urllib.request.Request(url, headers={"Accept": "application/json"})
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            body = resp.read().decode("utf-8")
            try:
                payload = json.loads(body)
            except Exception as e:
                print(f"Error parsing JSON from {url}: {e} -- body={body}")
                return {}

            # The API returns an object with key "robot" according to your description.
            if isinstance(payload, dict):
                return payload.get("robot", {})
            return {}
    except urllib.error.URLError as e:
        print(f"Network error fetching robot data from {url}: {e}")
    except Exception as e:
        print(f"Unexpected error fetching robot data from {url}: {e}")

    return {}

def publish_robot_and_obstacles(supervisor = root_supervisor):
    global last_redis_publish, random_delay

    now = time.time()
    """Lấy thông tin arena và obstacles"""
    try:
        # supervisor.simulationResetPhysics()
        # supervisor.step(0)  # Đảm bảo supervisor cập nhật trạng thái
        # --- Arena ---
        # if now - last_redis_publish < random_delay:
        #     return None
        arena = supervisor.getFromDef("rectangle_arena")
        if not arena:
            print("Warning: rectangle_arena not found")
            return None

        robot_0 = fetch_current_robot_api(0)
        robot_1 = fetch_current_robot_api(1)
        robot_2 = fetch_current_robot_api(2)
        robot_3  = fetch_current_robot_api(3)

        arena_size = arena.getField("floorSize").getSFVec2f()
        w, h = arena_size[0], arena_size[1]

        # --- Lấy tất cả obstacles ---
        target_types = {"CardboardBox"}
        obstacles = []

        root = supervisor.getRoot()
        children_field = root.getField("children")
        n = children_field.getCount()

        obstacle_id = 1
        for i in range(n):
            try:
                node = children_field.getMFNode(i)
                if node and node.getTypeName() in target_types:
                    # Lấy size và translation
                    belt_size = get_node_size(node)
                    belt_translation = node.getField("translation").getSFVec3f()
                    # Tạo obstacle object
                    timestamp_str = str(int(time.time() * 1000))
                    obstacles.append({
                        "timestamp": timestamp_str,
                        "camera_id": 0,
                        "class": "static",
                        "object_id": obstacle_id,
                        "conf": 0.95,
                        "center": [belt_translation[0], belt_translation[1]],
                        "corners": calculate_bbox(belt_translation, belt_size),
                    })
                    obstacle_id += 1
            except Exception as e:
                print(f"Error processing node {i}: {e}")
                continue
        message = {"april_tags": [robot_0, robot_1, robot_2, robot_3], "objects": obstacles}
        redis_client.xadd("robot_obstacle_pose_stream", {"data": json.dumps(message)})
        last_redis_publish = now
        sleep(0.05)
    except Exception as e:
        print(f"Error publishing apriltags and robots: {e}")
        return None


def get_map(supervisor = root_supervisor):
    """Lấy thông tin arena và obstacles"""
    try:
        # --- Arena ---
        arena = supervisor.getFromDef("rectangle_arena")
        if not arena:
            print("Warning: rectangle_arena not found")
            return None

        arena_size = arena.getField("floorSize").getSFVec2f()
        w, h = arena_size[0], arena_size[1]

        # --- Lấy tất cả obstacles ---
        target_types = {"ConveyorBelt", "WoodenPallet", "WoodenPalletStack"}
        obstacles = []

        root = supervisor.getRoot()
        children_field = root.getField("children")
        n = children_field.getCount()

        obstacle_id = 1
        for i in range(n):
            try:
                node = children_field.getMFNode(i)
                if node and node.getTypeName() in target_types:
                    # Lấy size và translation
                    belt_size = get_node_size(node)
                    belt_translation = node.getField("translation").getSFVec3f()

                    # Tạo obstacle object
                    obstacles.append({
                        "id": str(obstacle_id),
                        "type": node.getTypeName(),
                        "bbox": calculate_bbox(belt_translation, belt_size)
                    })
                    obstacle_id += 1

            except Exception as e:
                print(f"Error processing node {i}: {e}")
                continue

        factory_map = {
            "width": w,
            "height": h,
            "name": "Map_20250905_1158",
            "objects": obstacles,
            "points": []
        }

        return factory_map

    except Exception as e:
        print(f"Error getting arena and obstacles: {e}")
        return None


def get_cached_map():
    """Lấy map từ cache hoặc tạo mới nếu cần"""
    global cached_map, cache_timestamp

    with cache_lock:
        current_time = time.time()

        # Kiểm tra cache có hết hạn không
        if cached_map is None or (current_time - cache_timestamp) > CACHE_DURATION:
            print("Refreshing map cache...")
            start_time = time.time()

            new_map = get_map(root_supervisor)

            if new_map is not None:
                cached_map = new_map
                cache_timestamp = current_time

            end_time = time.time()
            print(f"Map cache refreshed in {end_time - start_time:.3f}s")

        return cached_map


def force_refresh_cache():
    """Buộc refresh cache"""
    global cached_map, cache_timestamp
    with cache_lock:
        cached_map = None
        cache_timestamp = 0


# --- Flask Routes ---
@app.route('/map', methods=['GET'])
def get_factory_map():
    """API endpoint để lấy map"""
    try:
        start_time = time.time()
        map_data = get_cached_map()
        end_time = time.time()

        if map_data is None:
            return {"status": "error", "message": "Failed to get map data"}, 500

        print(f"API request processed in {end_time - start_time:.3f}s")
        return {"status": "ok", "map": map_data}

    except Exception as e:
        print(f"Error in API endpoint: {e}")
        return {"status": "error", "message": str(e)}, 500


@app.route('/map/refresh', methods=['POST'])
def refresh_map():
    """API endpoint để force refresh map cache"""
    try:
        force_refresh_cache()
        map_data = get_cached_map()

        if map_data is None:
            return {"status": "error", "message": "Failed to refresh map"}, 500

        return {"status": "ok", "message": "Map cache refreshed", "map": map_data}

    except Exception as e:
        print(f"Error refreshing map: {e}")
        return {"status": "error", "message": str(e)}, 500


@app.route('/health', methods=['GET'])
def health_check():
    """Health check endpoint"""
    return {"status": "ok", "timestamp": time.time()}


def run_flask():
    """Chạy Flask server"""
    print("Starting Flask server on http://0.0.0.0:6060")
    app.run(host='0.0.0.0', port=6060, debug=False, use_reloader=False)

PUBLISH_INTERVAL = 0.05  # 50ms

has_rotated = False

import math
rotated_manhole_ids = set()
def rotate_robot_near_point(supervisor, robot_def, target_point, threshold=0.2, direction="right", angle_deg:float =90):
    """Quay robot khi đến gần target_point một lần duy nhất."""
    node = supervisor.getFromDef(robot_def)
    if node is None:
        print(f"[WARN] Không tìm thấy robot DEF='{robot_def}'")
        return False

    translation = node.getField("translation").getSFVec3f()
    pos_x, pos_y = translation[0], translation[1]
    dx = target_point[0] - pos_x
    dy = target_point[1] - pos_y
    distance = math.sqrt(dx * dx + dy * dy)

    if distance <= threshold:
        rotation_field = node.getField("rotation")
        current_rot = rotation_field.getSFRotation()
        current_yaw = current_rot[3]

        delta_angle = math.radians(angle_deg)
        if direction == "left":
            new_yaw = current_yaw + delta_angle
        else:
            new_yaw = current_yaw - delta_angle

        rotation_field.setSFRotation([0, 0, 1, new_yaw])
        print(f"[INFO] Robot quay {direction} {angle_deg:.1f}° tại vị trí gần {target_point}")
        return True

    return False


import random
def rotate_robot_random(supervisor, robot_def, target_point, threshold=0.2, angle_range=(45, 135)):
    """Wrapper random hướng (left/right) và góc quay trong khoảng cho trước."""
    direction = random.choice(["left", "right"])
    angle_deg = random.uniform(angle_range[0], angle_range[1])

    return rotate_robot_near_point(
        supervisor=supervisor,
        robot_def=robot_def,
        target_point=target_point,
        threshold=threshold,
        direction=direction,
        angle_deg=angle_deg
    )

def rotate_near_manhole_objects(supervisor, robot_def, threshold=0.25, angle_range=(60, 120)):
    """
    Hàm wrapper cao nhất:
    - Tự động tìm tất cả object PROTO SquareManhole trong world.
    - Với mỗi object, nếu robot đến gần, quay random 1 lần.
    - Đảm bảo mỗi manhole chỉ kích hoạt 1 lần duy nhất.
    """
    global rotated_manhole_ids

    root = supervisor.getRoot()
    children_field = root.getField("children")
    n = children_field.getCount()

    for i in range(n):
        try:
            node = children_field.getMFNode(i)
            if "SquareManhole" in node.getTypeName():
                # Lấy id duy nhất (DEF name hoặc index)
                def_name = node.getDef() or f"SquareManhole_{i}"
                if def_name in rotated_manhole_ids:
                    continue  # Đã quay rồi => bỏ qua

                # Lấy vị trí manhole
                pos = node.getField("translation").getSFVec3f()
                manhole_point = [pos[0], pos[1]]

                # Gọi hàm random quay
                rotated = rotate_robot_random(
                    supervisor=supervisor,
                    robot_def=robot_def,
                    target_point=manhole_point,
                    threshold=threshold,
                    angle_range=angle_range
                )

                if rotated:
                    rotated_manhole_ids.add(def_name)

        except Exception as e:
            print(f"[ERROR] Lỗi xử lý node {i}: {e}")
            continue

# --- Main Logic ---
if __name__ == "__main__":
    # Khởi tạo cache ban đầu
    print("Initializing map cache...")
    initial_map = get_cached_map()
    if initial_map:
        print(f"Map initialized with {len(initial_map['objects'])} obstacles")
    else:
        print("Warning: Failed to initialize map")

    # Chạy Flask server trong thread riêng
    flask_thread = threading.Thread(target=run_flask, daemon=True)
    flask_thread.start()

    print("Flask server started, beginning Webots simulation loop...")

    # Webots simulation loop được tối ưu
    timestep = int(root_supervisor.getBasicTimeStep())
    last_cache_refresh = 0
    loop_count = 0
    last_publish = 0

    # track time for periodic actions
    last_publish_time = time.time()
    publisher_thread = threading.Thread(
        target=publish_robot_and_obstacles,
        args=(root_supervisor,),
        daemon=True,
    )
    publisher_thread.start()
    while root_supervisor.step(timestep) != -1:
        # compute current time at start of iteration
        now = time.time()

        rotate_near_manhole_objects(
            supervisor=root_supervisor,
            robot_def="Pioneer_3-AT",
            threshold=0.5,
            angle_range=(5, 10)
        )

        loop_count += 1


    print("Webots simulation ended")