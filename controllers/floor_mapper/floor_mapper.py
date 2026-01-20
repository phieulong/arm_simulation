import json
import socket
from time import sleep

from controller import Supervisor
import redis
import threading
import time
from flask import Flask
import random
import psycopg2
# from psycopg2 import sql  # Unused import commented out
from redis import Connection

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

class IPv4Connection(Connection):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.socket_family = socket.AF_INET

# Redis client
pool = redis.ConnectionPool(
    connection_class=IPv4Connection,
    host='localhost',
    port=6379,
    db=0
)

redis_client = redis.Redis(host='localhost', port=6379, db=0, connection_pool=pool)

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
        [bx - bl / 2, by + bw / 2],  # top-left
        [bx + bl / 2, by + bw / 2],  # top-right
        [bx + bl / 2, by - bw / 2],  # bottom-right
        [bx - bl / 2, by - bw / 2],  # bottom-left
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

def publish_robot_and_obstacles(map_robot_ids, supervisor = root_supervisor):
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
        robot_0['object_id'] = map_robot_ids["0"]
        # robot_1 = fetch_current_robot_api(1)
        # robot_1['object_id'] = map_robot_ids["1"]
        # robot_2 = fetch_current_robot_api(2)
        # robot_2['object_id'] = map_robot_ids["2"]
        # robot_3  = fetch_current_robot_api(3)
        # robot_3['object_id'] = map_robot_ids["3"]

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
        message = {"april_tags": [robot_0,
                                  # robot_1, robot_2, robot_3
                                  ], "objects": obstacles}
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
                        "x": belt_translation[0],
                        "y": belt_translation[1],
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


def create_postgres_connection():
    """
    Creates a connection to the PostgreSQL database.
    Returns the connection object or None if the connection fails.
    """
    try:
        connection = psycopg2.connect(
            dbname="arm",
            user="postgres",
            password="postgres",  # Replace with the actual password or use environment variables
            host="localhost",
            port=5432
        )
        print("Connection to PostgreSQL database established successfully.")
        return connection
    except Exception as e:
        print(f"Error connecting to PostgreSQL database: {e}")
        return None

def fetch_all_maps():
    """
    Fetch all rows from the "maps" table in the PostgreSQL database.
    Returns a list of dictionaries representing the rows, or an empty list if an error occurs.
    """
    try:
        connection = create_postgres_connection()
        if connection is None:
            print("Failed to establish database connection.")
            return []

        cursor = connection.cursor()
        query = "SELECT id, created_at, created_by, updated_at, updated_by, name, width, height, description FROM public.maps"
        cursor.execute(query)

        # Fetch all rows and convert to a list of dictionaries
        columns = [desc[0] for desc in cursor.description]
        rows = cursor.fetchall()
        result = [dict(zip(columns, row)) for row in rows]

        cursor.close()
        connection.close()

        return result

    except Exception as e:
        print(f"Error fetching maps: {e}")
        return []

def delete_all_allowed_zone(connection):
    """
    Deletes all records from the "maps" table in the PostgreSQL database.

    Args:
        connection: The database cursor to execute the query.

    Returns:
        bool: True if the deletion was successful, False otherwise.
    """
    try:
        cursor = connection.cursor()
        delete_query = "DELETE FROM public.robot_allowed_zones;"
        cursor.execute(delete_query)
        connection.commit()
        print("All robot allowed zone s deleted successfully.")
        return True
    except Exception as e:
        print(f"Error deleting robot allowed zone: {e}")
        return False


def delete_all_map_objects(connection):
    """
    Deletes all records from the "maps" table in the PostgreSQL database.

    Args:
        connection: The database cursor to execute the query.

    Returns:
        bool: True if the deletion was successful, False otherwise.
    """
    try:
        cursor = connection.cursor()
        delete_query = "DELETE FROM public.map_objects;"
        cursor.execute(delete_query)
        connection.commit()
        print("All map object s deleted successfully.")
        return True
    except Exception as e:
        print(f"Error deleting map objects: {e}")
        return False

def delete_all_maps(connection):
    """
    Deletes all records from the "maps" table in the PostgreSQL database.

    Args:
        connection: The database connection to commit the transaction.

    Returns:
        bool: True if the deletion was successful, False otherwise.
    """
    try:
        cursor = connection.cursor()
        delete_query = "DELETE FROM public.maps;"
        cursor.execute(delete_query)
        connection.commit()  # Commit the transaction
        print("All maps deleted successfully.")
        return True
    except Exception as e:
        print(f"Error deleting maps: {e}")
        return False

def delete_all_object_types(connection):
    """
    Deletes all records from the "maps" table in the PostgreSQL database.

    Args:
        connection: The database connection to commit the transaction.

    Returns:
        bool: True if the deletion was successful, False otherwise.
    """
    try:
        cursor = connection.cursor()
        delete_query = "DELETE FROM public.object_types;"
        cursor.execute(delete_query)
        connection.commit()  # Commit the transaction
        print("All object types deleted successfully.")
        return True
    except Exception as e:
        print(f"Error deleting object types: {e}")
        return False

def insert_map(connection, created_by, name, width, height, description=None):
    """
    Inserts a new map record into the "maps" table in the PostgreSQL database.

    Args:
        connection: The database connection to commit the transaction.
        created_by (str): The user who created the map.
        name (str): The name of the map.
        width (int): The width of the map.
        height (int): The height of the map.
        description (str, optional): A description of the map. Defaults to None.

    Returns:
        bool: True if the insertion was successful, False otherwise.
    """
    try:
        cursor = connection.cursor()
        insert_query = """
        INSERT INTO public.maps (created_at, created_by, updated_at, updated_by, name, width, height, description)
        VALUES (NOW(), %s, NOW(), %s, %s, %s, %s, %s)  RETURNING id;
        """
        cursor.execute(insert_query, (created_by, created_by, name, width, height, description))
        map_id = cursor.fetchone()[0]
        connection.commit()  # Commit the transaction
        print("Map inserted successfully.")
        return map_id

    except Exception as e:
        print(f"Error inserting map: {e}")
        return None

def insert_object_type(connection, created_by, name, settings=None):
    from psycopg2.extras import Json
    """
    Inserts a new object type into the "object_types" table in the PostgreSQL database.

    Args:
        connection: The database connection to commit the transaction.
        created_by (str): The user who created the object type.
        name (str): The name of the object type.
        settings (dict, optional): Additional settings for the object type. Defaults to None.

    Returns:
        int: The ID of the inserted object type, or None if the insertion failed.
    """
    try:
        cursor = connection.cursor()
        if not settings:
            settings = Json({
                    "icon": "",
                    "color": "#0d738c",
                    "shape": "polygon",
                    "zIndex": 1,
                    "movable": False,
                    "iconType": "library",
                    "isActive": True,
                    "robotIds": [],
                    "objectName": "Obstacle",
                    "objectType": "obstacle",
                    "safeRadius": 1,
                    "description": "",
                    "safetyLevel": None,
                    "robotTypeIds": [],
                    "stopActionThreshold": 1,
                    "slowDownActionThreshold": 1
                })

        insert_query = """
        INSERT INTO public.object_types (created_at, created_by, updated_at, updated_by, name, settings)
        VALUES (NOW(), %s, NOW(), %s, %s, %s) RETURNING id;
        """
        cursor.execute(insert_query, (created_by, created_by, name, settings if settings else None))
        object_type_id = cursor.fetchone()[0]
        connection.commit()  # Commit the transaction
        print(f"Object type '{name}' inserted successfully with ID {object_type_id}.")
        return object_type_id
    except Exception as e:
        print(f"Error inserting object type: {e}")
        return None

def insert_map_objects_from_cache(connection, created_by, map_id, object_type_id, cached_map):
    """
    Inserts objects from the cached map into the "map_objects" table in the PostgreSQL database.

    Args:
        cursor: The database cursor to execute the query.
        connection: The database connection to commit the transaction.
        created_by (str): The user who created the map objects.
        map_id (int): The ID of the map these objects belong to.
        object_type_id (int): The ID of the object type for these objects.

    Returns:
        int: The number of objects inserted, or None if the insertion failed.
    """
    try:
        # Fetch the cached map
        cursor = connection.cursor()
        objects = cached_map['objects']
        insert_query = """
        INSERT INTO public.map_objects (created_at, created_by, updated_at, updated_by, name, shape, x, y, map_id, object_type_id)
        VALUES (NOW(), %s, NOW(), %s, %s, %s, %s, %s, %s, %s);
        """

        for obj in objects:
            name = obj.get('type', 'Unknown Object')
            shape = json.dumps(obj.get('bbox', {}))
            x = obj.get('x', 0)
            y = obj.get('y', 0)

            cursor.execute(insert_query, (created_by, created_by, name, shape, x, y, map_id, object_type_id))

        connection.commit()  # Commit the transaction
        print(f"Inserted {len(objects)} objects into the map_objects table.")
        return len(objects)

    except Exception as e:
        print(f"Error inserting map objects: {e}")
        return None


def insert_robot_type(connection, created_by, name, capacity=None, max_speed=None,
                     safe_distance=None, min_battery_capacity=None, robot_type_id=None,
                     model=None, description=None, width=None, height=None, length=None):
    """
    Inserts a new robot type into the "robot_types" table in the PostgreSQL database.

    Args:
        connection: The database connection to commit the transaction.
        created_by (str): The user who created the robot type.
        name (str): The name of the robot type.
        capacity (int, optional): Robot capacity.
        max_speed (numeric, optional): Maximum speed of the robot.
        safe_distance (int, optional): Safe distance for the robot.
        min_battery_capacity (int, optional): Minimum battery capacity.
        robot_type_id (str, optional): Robot type identifier.
        model (str, optional): Robot model.
        description (str, optional): Description of the robot type.
        width (int, optional): Robot width.
        height (int, optional): Robot height.
        length (int, optional): Robot length.

    Returns:
        int: The ID of the inserted robot type, or None if the insertion failed.
    """
    try:
        cursor = connection.cursor()
        insert_query = """
        INSERT INTO public.robot_types (
            created_at, created_by, updated_at, updated_by, name, capacity,
            max_speed, safe_distance, min_battery_capacity, robot_type_id,
            model, description, width, height, length
        )
        VALUES (NOW(), %s, NOW(), %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        RETURNING id;
        """

        cursor.execute(insert_query, (
            created_by, created_by, name, capacity, max_speed, safe_distance,
            min_battery_capacity, robot_type_id, model, description,
            width, height, length
        ))

        robot_type_id_result = cursor.fetchone()[0]
        connection.commit()
        print(f"Robot type '{name}' inserted successfully with ID {robot_type_id_result}.")
        return robot_type_id_result

    except Exception as e:
        print(f"Error inserting robot type: {e}")
        return None

def delete_all_robot_types(connection):
    """
    Deletes all records from the "robot_types" table in the PostgreSQL database.

    Args:
        connection: The database connection to commit the transaction.

    Returns:
        bool: True if the deletion was successful, False otherwise.
    """
    try:
        cursor = connection.cursor()
        delete_query = "DELETE FROM public.robot_types;"
        cursor.execute(delete_query)
        connection.commit()
        print("All robot types deleted successfully.")
        return True
    except Exception as e:
        print(f"Error deleting robot types: {e}")
        return False

def insert_robot(connection, created_by, serial_number, ua_opc_endpoint, name=None,
                capacity=None, max_speed=None, robot_type_id=None, firmware_version=None,
                username=None, password=None, min_battery_capacity=None, safe_distance=None,
                last_maintenance=None, installation_date=None, status="active"):
    """
    Inserts a new robot into the "robots" table in the PostgreSQL database.

    Args:
        connection: The database connection to commit the transaction.
        created_by (str): The user who created the robot.
        serial_number (str): Unique serial number for the robot.
        ua_opc_endpoint (str): OPC UA endpoint for the robot.
        name (str, optional): The name of the robot.
        capacity (int, optional): Robot capacity.
        max_speed (numeric, optional): Maximum speed of the robot.
        robot_type_id (int, optional): Foreign key to robot_types table.
        firmware_version (str, optional): Firmware version of the robot.
        username (str, optional): Username for robot authentication.
        password (str, optional): Password for robot authentication.
        min_battery_capacity (int, optional): Minimum battery capacity.
        safe_distance (int, optional): Safe distance for the robot.
        last_maintenance (datetime, optional): Last maintenance timestamp.
        installation_date (datetime, optional): Installation date.
        status (str, optional): Robot status (default: "active").

    Returns:
        int: The ID of the inserted robot, or None if the insertion failed.
    """
    try:
        cursor = connection.cursor()
        insert_query = """
        INSERT INTO public.robots (
            created_at, created_by, updated_at, updated_by, capacity, max_speed,
            name, robot_type_id, serial_number, ua_opc_endpoint, firmware_version,
            username, password, min_battery_capacity, safe_distance,
            last_maintenance, installation_date, status
        )
        VALUES (NOW(), %s, NOW(), %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        RETURNING id;
        """

        cursor.execute(insert_query, (
            created_by, created_by, capacity, max_speed, name, robot_type_id,
            serial_number, ua_opc_endpoint, firmware_version, username, password,
            min_battery_capacity, safe_distance, last_maintenance, installation_date, status
        ))

        robot_id = cursor.fetchone()[0]
        connection.commit()
        print(f"Robot '{name or serial_number}' inserted successfully with ID {robot_id}.")
        return robot_id

    except Exception as e:
        print(f"Error inserting robot: {e}")
        return None

def delete_all_robots(connection):
    """
    Deletes all records from the "robots" table in the PostgreSQL database.

    Args:
        connection: The database connection to commit the transaction.

    Returns:
        bool: True if the deletion was successful, False otherwise.
    """
    try:
        cursor = connection.cursor()
        delete_query = "DELETE FROM public.robots;"
        cursor.execute(delete_query)
        connection.commit()
        print("All robots deleted successfully.")
        return True
    except Exception as e:
        print(f"Error deleting robots: {e}")
        return False

def fetch_all_robots(connection):
    """
    Fetch all rows from the "robots" table in the PostgreSQL database.
    Returns a list of dictionaries representing the rows, or an empty list if an error occurs.
    """
    try:
        cursor = connection.cursor()
        query = """
        SELECT id, created_at, created_by, updated_at, updated_by, capacity, max_speed,
               name, robot_type_id, serial_number, ua_opc_endpoint, firmware_version,
               username, min_battery_capacity, safe_distance, last_maintenance,
               installation_date, status
        FROM public.robots WHERE deleted_at IS NULL
        """
        cursor.execute(query)

        # Fetch all rows and convert to a list of dictionaries
        columns = [desc[0] for desc in cursor.description]
        rows = cursor.fetchall()
        result = [dict(zip(columns, row)) for row in rows]

        cursor.close()
        return result

    except Exception as e:
        print(f"Error fetching robots: {e}")
        return []

def insert_camera(connection, created_by, camera_id, name, calibration_preview_image=None,
                 calibration_setting=None, calibration_status=None, connection_status=None,
                 fov=None, frame_rate=None, ip=None, is_active=None, live_source=None,
                 password=None, physical_location=None, port=None, position_mapping=None,
                 protocol=None, snapshot=None, stitching_id=None, user_name=None,
                 webrtc_live_url=None):
    """
    Inserts a new camera into the "cameras" table in the PostgreSQL database.

    Args:
        connection: The database connection to commit the transaction.
        created_by (str): The user who created the camera record.
        camera_id (str): Unique camera identifier (required).
        name (str): The name of the camera (required).
        calibration_preview_image (str, optional): Preview image path for calibration.
        calibration_setting (dict, optional): Calibration settings as JSON.
        calibration_status (bool, optional): Calibration status.
        connection_status (bool, optional): Connection status.
        fov (float, optional): Field of view (double precision).
        frame_rate (int, optional): Frame rate.
        ip (str, optional): Camera IP address (max 16 chars).
        is_active (bool, optional): Whether camera is active.
        live_source (str, optional): Live source URL.
        password (str, optional): Camera password.
        physical_location (dict, optional): Physical location as JSON.
        port (int, optional): Camera port number.
        position_mapping (dict, optional): Position mapping as JSON.
        protocol (str, optional): Connection protocol (max 20 chars).
        snapshot (str, optional): Snapshot URL.
        stitching_id (int, optional): Stitching ID reference.
        user_name (str, optional): Camera username.
        webrtc_live_url (str, optional): WebRTC live URL.

    Returns:
        int: The ID of the inserted camera, or None if the insertion failed.
    """
    try:
        cursor = connection.cursor()
        insert_query = """
        INSERT INTO public.cameras (
            created_at, created_by, updated_at, updated_by, calibration_preview_image,
            calibration_setting, calibration_status, camera_id, connection_status, fov,
            frame_rate, ip, is_active, live_source, name, password, physical_location,
            port, position_mapping, protocol, snapshot, stitching_id, user_name,
            webrtc_live_url
        )
        VALUES (NOW(), %s, NOW(), %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        RETURNING id;
        """

        cursor.execute(insert_query, (
            created_by, created_by, calibration_preview_image,
            json.dumps(calibration_setting) if calibration_setting else None,
            calibration_status, camera_id, connection_status, fov, frame_rate, ip,
            is_active, live_source, name, password,
            json.dumps(physical_location) if physical_location else None,
            port, json.dumps(position_mapping) if position_mapping else None,
            protocol, snapshot, stitching_id, user_name, webrtc_live_url
        ))

        camera_db_id = cursor.fetchone()[0]
        connection.commit()
        print(f"Camera '{name}' (ID: {camera_id}) inserted successfully with database ID {camera_db_id}.")
        return camera_db_id

    except Exception as e:
        print(f"Error inserting camera: {e}")
        return None

def delete_all_cameras(connection):
    """
    Deletes all records from the "cameras" table in the PostgreSQL database.

    Args:
        connection: The database connection to commit the transaction.

    Returns:
        bool: True if the deletion was successful, False otherwise.
    """
    try:
        cursor = connection.cursor()
        delete_query = "DELETE FROM public.cameras;"
        cursor.execute(delete_query)
        connection.commit()
        print("All cameras deleted successfully.")
        return True
    except Exception as e:
        print(f"Error deleting cameras: {e}")
        return False

def fetch_all_cameras(connection):
    """
    Fetch all rows from the "cameras" table in the PostgreSQL database.
    Returns a list of dictionaries representing the rows, or an empty list if an error occurs.
    """
    try:
        cursor = connection.cursor()
        query = """
        SELECT id, created_at, created_by, updated_at, updated_by, calibration_preview_image,
               calibration_setting, calibration_status, camera_id, connection_status, fov,
               frame_rate, ip, is_active, live_source, name, password, physical_location,
               port, position_mapping, protocol, snapshot, stitching_id, user_name,
               webrtc_live_url
        FROM public.cameras
        """
        cursor.execute(query)

        # Fetch all rows and convert to a list of dictionaries
        columns = [desc[0] for desc in cursor.description]
        rows = cursor.fetchall()
        result = [dict(zip(columns, row)) for row in rows]

        cursor.close()
        return result

    except Exception as e:
        print(f"Error fetching cameras: {e}")
        return []

def initialize_sample_cameras(connection):
    """
    Initialize sample cameras into the database after map insertion.
    This function inserts predefined camera configurations for the factory setup.

    Args:
        connection: The database connection to commit the transaction.

    Returns:
        list: List of camera IDs that were successfully inserted
    """
    try:
        # Delete all existing cameras first
        delete_all_cameras(connection)

        # Sample camera configurations based on provided data
        camera_configs = [
            {
                "camera_id": "Camera01",
                "name": "Camera01",
                "ip": "14.224.216.11",
                "port": 8559,
                "protocol": "RTSP",
                "user_name": "admin",
                "password": "bnk@13032018",
                "is_active": True,
                "connection_status": True,
                "physical_location": {
                    "x": 0.0,
                    "y": 0.0,
                    "z": 0.0,
                    "width": 3840,
                    "height": 2160,
                    "rotationAngle": 0.0
                },
                "position_mapping": []
            },
            {
                "camera_id": "CameraID03",
                "name": "CameraName03",
                "ip": "14.224.216.11",
                "port": 8558,
                "protocol": "RTSP",
                "user_name": "admin",
                "password": "bnk@13032018",
                "is_active": True,
                "connection_status": True,
                "physical_location": {
                    "x": 0.0,
                    "y": 0.0,
                    "z": 0.0,
                    "width": 3840,
                    "height": 2160,
                    "rotationAngle": 0.0
                },
                "position_mapping": []
            },
            {
                "camera_id": "Camera02",
                "name": "Camera02",
                "ip": "14.224.216.11",
                "port": 8558,
                "protocol": "RTSP",
                "user_name": "admin",
                "password": "bnk@13032018",
                "is_active": True,
                "connection_status": True,
                "physical_location": {
                    "x": 0.0,
                    "y": 0.0,
                    "z": 0.0,
                    "width": 3840,
                    "height": 2160,
                    "rotationAngle": 0.0
                },
                "position_mapping": [
                    {
                        "x": 2438.6789918300083,
                        "y": 150.03061965682622,
                        "id": 1,
                        "type": 1,
                        "realX": 1.0,
                        "realY": 1.0
                    },
                    {
                        "x": 0.0,
                        "y": 0.0,
                        "id": 0,
                        "type": 0,
                        "realX": 1.0,
                        "realY": 1.0
                    }
                ],
                "snapshot": "096c0060d18411f0950f12c8dcd0ebc3.jpg"
            }
        ]

        inserted_camera_ids = []

        for config in camera_configs:
            camera_id = insert_camera(
                connection=connection,
                created_by="admin",
                camera_id=config["camera_id"],
                name=config["name"],
                ip=config["ip"],
                port=config["port"],
                protocol=config["protocol"],
                user_name=config["user_name"],
                password=config["password"],
                is_active=config["is_active"],
                connection_status=config["connection_status"],
                physical_location=config["physical_location"],
                position_mapping=config["position_mapping"],
                snapshot=config.get("snapshot")
            )

            if camera_id:
                inserted_camera_ids.append(camera_id)
                print(f"Successfully inserted camera '{config['name']}' with database ID {camera_id}")
            else:
                print(f"Failed to insert camera '{config['name']}'")

        print(f"Successfully initialized {len(inserted_camera_ids)} cameras in the database.")
        return inserted_camera_ids

    except Exception as e:
        print(f"Error initializing sample cameras: {e}")
        return []

def insert_map_camera(connection, map_id, camera_id):
    """
    Inserts a new map-camera relationship into the "map_cameras" table in the PostgreSQL database.

    Args:
        connection: The database connection to commit the transaction.
        map_id (int): The ID of the map (foreign key to maps table).
        camera_id (int): The ID of the camera (foreign key to cameras table).

    Returns:
        int: The ID of the inserted map_camera relationship, or None if the insertion failed.
    """
    try:
        cursor = connection.cursor()
        insert_query = """
        INSERT INTO public.map_cameras (map_id, camera_id)
        VALUES (%s, %s)
        RETURNING id;
        """

        cursor.execute(insert_query, (map_id, camera_id))
        map_camera_id = cursor.fetchone()[0]
        connection.commit()
        print(f"Map-Camera relationship inserted successfully with ID {map_camera_id} (Map: {map_id}, Camera: {camera_id}).")
        return map_camera_id

    except Exception as e:
        print(f"Error inserting map-camera relationship: {e}")
        return None

def delete_all_map_cameras(connection):
    """
    Deletes all records from the "map_cameras" table in the PostgreSQL database.

    Args:
        connection: The database connection to commit the transaction.

    Returns:
        bool: True if the deletion was successful, False otherwise.
    """
    try:
        cursor = connection.cursor()
        delete_query = "DELETE FROM public.map_cameras;"
        cursor.execute(delete_query)
        connection.commit()
        print("All map-camera relationships deleted successfully.")
        return True
    except Exception as e:
        print(f"Error deleting map-camera relationships: {e}")
        return False

def fetch_all_map_cameras(connection):
    """
    Fetch all rows from the "map_cameras" table in the PostgreSQL database.
    Returns a list of dictionaries representing the rows, or an empty list if an error occurs.
    """
    try:
        cursor = connection.cursor()
        query = """
        SELECT id, map_id, camera_id
        FROM public.map_cameras
        """
        cursor.execute(query)

        # Fetch all rows and convert to a list of dictionaries
        columns = [desc[0] for desc in cursor.description]
        rows = cursor.fetchall()
        result = [dict(zip(columns, row)) for row in rows]

        cursor.close()
        return result

    except Exception as e:
        print(f"Error fetching map-camera relationships: {e}")
        return []

def fetch_cameras_by_map_id(connection, map_id):
    """
    Fetch all cameras associated with a specific map.

    Args:
        connection: The database connection.
        map_id (int): The ID of the map.

    Returns:
        list: List of camera records associated with the map.
    """
    try:
        cursor = connection.cursor()
        query = """
        SELECT c.id, c.camera_id, c.name, c.ip, c.port, c.protocol,
               c.is_active, c.connection_status, c.physical_location, c.position_mapping
        FROM public.cameras c
        INNER JOIN public.map_cameras mc ON c.id = mc.camera_id
        WHERE mc.map_id = %s
        """
        cursor.execute(query, (map_id,))

        # Fetch all rows and convert to a list of dictionaries
        columns = [desc[0] for desc in cursor.description]
        rows = cursor.fetchall()
        result = [dict(zip(columns, row)) for row in rows]

        cursor.close()
        return result

    except Exception as e:
        print(f"Error fetching cameras for map {map_id}: {e}")
        return []

def initialize_map_camera_relationships(connection, map_id, camera_ids):
    """
    Initialize relationships between a map and multiple cameras.

    Args:
        connection: The database connection to commit the transaction.
        map_id (int): The ID of the map.
        camera_ids (list): List of camera IDs to associate with the map.

    Returns:
        list: List of map_camera IDs that were successfully inserted.
    """
    try:
        # Delete existing relationships for this map
        cursor = connection.cursor()
        delete_query = "DELETE FROM public.map_cameras WHERE map_id = %s;"
        cursor.execute(delete_query, (map_id,))

        inserted_ids = []

        for camera_id in camera_ids:
            map_camera_id = insert_map_camera(connection, map_id, camera_id)
            if map_camera_id:
                inserted_ids.append(map_camera_id)

        print(f"Successfully initialized {len(inserted_ids)} map-camera relationships for map {map_id}.")
        return inserted_ids

    except Exception as e:
        print(f"Error initializing map-camera relationships: {e}")
        return []

def insert_robot_state(connection, robot_id, x=None, y=None, status=None, battery_level=None):
    """
    Inserts a new robot state into the "robot_states" table in the PostgreSQL database.

    Args:
        connection: The database connection to commit the transaction.
        robot_id (int): The ID of the robot (foreign key to robots table, required).
        x (float, optional): X coordinate position (numeric 10,2).
        y (float, optional): Y coordinate position (numeric 10,2).
        status (str, optional): Robot status (max 50 chars).
        battery_level (int, optional): Battery level percentage.

    Returns:
        int: The ID of the inserted robot state, or None if the insertion failed.
    """
    try:
        cursor = connection.cursor()
        insert_query = """
        INSERT INTO public.robot_states (x, y, status, battery_level, robot_id, updated_at)
        VALUES (%s, %s, %s, %s, %s, NOW())
        RETURNING id;
        """

        cursor.execute(insert_query, (x, y, status, battery_level, robot_id))
        robot_state_id = cursor.fetchone()[0]
        connection.commit()
        print(f"Robot state inserted successfully with ID {robot_state_id} for robot {robot_id}.")
        return robot_state_id

    except Exception as e:
        print(f"Error inserting robot state: {e}")
        return None

def delete_all_robot_states(connection):
    """
    Deletes all records from the "robot_states" table in the PostgreSQL database.

    Args:
        connection: The database connection to commit the transaction.

    Returns:
        bool: True if the deletion was successful, False otherwise.
    """
    try:
        cursor = connection.cursor()
        delete_query = "DELETE FROM public.robot_states;"
        cursor.execute(delete_query)
        connection.commit()
        print("All robot states deleted successfully.")
        return True
    except Exception as e:
        print(f"Error deleting robot states: {e}")
        return False

def fetch_all_robot_states(connection):
    """
    Fetch all rows from the "robot_states" table in the PostgreSQL database.
    Returns a list of dictionaries representing the rows, or an empty list if an error occurs.
    """
    try:
        cursor = connection.cursor()
        query = """
        SELECT id, x, y, status, battery_level, robot_id, updated_at
        FROM public.robot_states
        ORDER BY updated_at DESC
        """
        cursor.execute(query)

        # Fetch all rows and convert to a list of dictionaries
        columns = [desc[0] for desc in cursor.description]
        rows = cursor.fetchall()
        result = [dict(zip(columns, row)) for row in rows]

        cursor.close()
        return result

    except Exception as e:
        print(f"Error fetching robot states: {e}")
        return []

def fetch_robot_states_by_robot_id(connection, robot_id, limit=10):
    """
    Fetch robot states for a specific robot, ordered by most recent first.

    Args:
        connection: The database connection.
        robot_id (int): The ID of the robot.
        limit (int, optional): Maximum number of states to return. Defaults to 10.

    Returns:
        list: List of robot state records for the specified robot.
    """
    try:
        cursor = connection.cursor()
        query = """
        SELECT id, x, y, status, battery_level, robot_id, updated_at
        FROM public.robot_states
        WHERE robot_id = %s
        ORDER BY updated_at DESC
        LIMIT %s
        """
        cursor.execute(query, (robot_id, limit))

        # Fetch all rows and convert to a list of dictionaries
        columns = [desc[0] for desc in cursor.description]
        rows = cursor.fetchall()
        result = [dict(zip(columns, row)) for row in rows]

        cursor.close()
        return result

    except Exception as e:
        print(f"Error fetching robot states for robot {robot_id}: {e}")
        return []

def get_latest_robot_state(connection, robot_id):
    """
    Get the most recent state for a specific robot.

    Args:
        connection: The database connection.
        robot_id (int): The ID of the robot.

    Returns:
        dict: The latest robot state record, or None if not found.
    """
    try:
        cursor = connection.cursor()
        query = """
        SELECT id, x, y, status, battery_level, robot_id, updated_at
        FROM public.robot_states
        WHERE robot_id = %s
        ORDER BY updated_at DESC
        LIMIT 1
        """
        cursor.execute(query, (robot_id,))
        row = cursor.fetchone()

        cursor.close()

        if row:
            columns = [desc[0] for desc in cursor.description]
            return dict(zip(columns, row))
        return None

    except Exception as e:
        print(f"Error fetching latest robot state for robot {robot_id}: {e}")
        return None

def bulk_insert_robot_states(connection, robot_states):
    """
    Insert multiple robot states in a single transaction for better performance.

    Args:
        connection: The database connection to commit the transaction.
        robot_states (list): List of robot state dictionaries with keys:
                           ['robot_id', 'x', 'y', 'status', 'battery_level']

    Returns:
        int: Number of robot states successfully inserted, or None if failed.
    """
    try:
        cursor = connection.cursor()
        insert_query = """
        INSERT INTO public.robot_states (x, y, status, battery_level, robot_id, updated_at)
        VALUES (%s, %s, %s, %s, %s, NOW())
        """

        # Prepare data for bulk insert
        data_to_insert = []
        for state in robot_states:
            data_to_insert.append((
                state.get('x'),
                state.get('y'),
                state.get('status'),
                state.get('battery_level'),
                state.get('robot_id')
            ))

        # Execute bulk insert
        cursor.executemany(insert_query, data_to_insert)
        connection.commit()

        inserted_count = len(data_to_insert)
        print(f"Successfully bulk inserted {inserted_count} robot states.")
        return inserted_count

    except Exception as e:
        print(f"Error bulk inserting robot states: {e}")
        return None

def update_robot_state(connection, robot_id, x=None, y=None, status=None, battery_level=None):
    """
    Update the latest robot state or insert a new one if none exists.

    Args:
        connection: The database connection to commit the transaction.
        robot_id (int): The ID of the robot.
        x (float, optional): X coordinate position.
        y (float, optional): Y coordinate position.
        status (str, optional): Robot status.
        battery_level (int, optional): Battery level percentage.

    Returns:
        int: The ID of the updated/inserted robot state, or None if failed.
    """
    try:
        # Always insert a new state record (keeping history)
        return insert_robot_state(connection, robot_id, x, y, status, battery_level)

    except Exception as e:
        print(f"Error updating robot state for robot {robot_id}: {e}")
        return None

def cleanup_old_robot_states(connection, days_to_keep=30):
    """
    Delete robot state records older than the specified number of days.

    Args:
        connection: The database connection to commit the transaction.
        days_to_keep (int): Number of days of history to keep. Defaults to 30.

    Returns:
        int: Number of records deleted, or None if failed.
    """
    try:
        cursor = connection.cursor()
        delete_query = """
        DELETE FROM public.robot_states
        """
        cursor.execute(delete_query, (days_to_keep,))
        deleted_count = cursor.rowcount
        connection.commit()

        print(f"Cleaned up {deleted_count} old robot state records (older than {days_to_keep} days).")
        return deleted_count

    except Exception as e:
        print(f"Error cleaning up old robot states: {e}")
        return None

def initialize_default_robot_states(connection):
    """
    Initialize default robot states for all robots in the database.
    Inserts initial states with x=null, y=null, status='IDLE', battery_level=100.

    Args:
        connection: The database connection to commit the transaction.

    Returns:
        list: List of robot state IDs that were successfully inserted.
    """
    try:
        # Delete all existing robot states first
        delete_all_robot_states(connection)

        # Fetch all robots from database
        robots = fetch_all_robots(connection)

        if not robots:
            print("No robots found in database to initialize states for.")
            return []

        inserted_state_ids = []

        for robot in robots:
            robot_id = robot['id']
            robot_name = robot.get('name', f'Robot-{robot_id}')

            # Insert default state for each robot
            state_id = insert_robot_state(
                connection=connection,
                robot_id=robot_id,
                x=None,  # null
                y=None,  # null
                status='IDLE',
                battery_level=100
            )

            if state_id:
                inserted_state_ids.append(state_id)
                print(f"Initialized default state for robot '{robot_name}' (ID: {robot_id}) with state ID {state_id}")
            else:
                print(f"Failed to initialize state for robot '{robot_name}' (ID: {robot_id})")

        print(f"Successfully initialized default states for {len(inserted_state_ids)} robots.")
        return inserted_state_ids

    except Exception as e:
        print(f"Error initializing default robot states: {e}")
        return []

def bulk_initialize_robot_states_for_all(connection):
    """
    Bulk initialize default robot states for all robots for better performance.

    Args:
        connection: The database connection to commit the transaction.

    Returns:
        int: Number of robot states successfully inserted, or None if failed.
    """
    try:
        # Delete all existing robot states first
        delete_all_robot_states(connection)

        # Fetch all robots from database
        robots = fetch_all_robots(connection)

        if not robots:
            print("No robots found in database to initialize states for.")
            return 0

        # Prepare bulk data
        robot_states = []
        for robot in robots:
            robot_states.append({
                'robot_id': robot['id'],
                'x': None,
                'y': None,
                'status': 'IDLE',
                'battery_level': 100
            })

        # Bulk insert all robot states
        inserted_count = bulk_insert_robot_states(connection, robot_states)
        #
        # if inserted_count:
        #     print(f"Successfully bulk initialized default states for {inserted_count} robots.")
        #     for robot in robots:
        #         print(f"  - Robot '{robot.get('name', f'Robot-{robot['id']}')}' (ID: {robot['id']}) -> IDLE, 100%")

        return inserted_count

    except Exception as e:
        print(f"Error bulk initializing robot states: {e}")
        return None

def init_data():
# Kết nối tới cơ sở dữ liệu PostgreSQL
    connection = create_postgres_connection()
    robot_map_ids = {}

    if connection:
        try:
            delete_all_map_cameras(connection)
            delete_all_allowed_zone(connection)
            delete_all_map_objects(connection)
            delete_all_object_types(connection)
            delete_all_maps(connection)  # Xóa tất cả bản ghi hiện có
            maps = fetch_all_maps()
            map_count = len(maps)

            map_id = None  # Initialize map_id variable
            if map_count == 0 :
                # Nếu chưa có bản ghi nào, chèn một bản ghi mới
                factory_map = get_cached_map()
                # print(factory_map)
                if factory_map:
                    map_id = insert_map(
                        connection,
                        created_by="system",
                        name=factory_map.get("name", "Default Map"),
                        width=factory_map.get("width", 100),
                        height=factory_map.get("height", 100),
                        description="This is the default map."
                    )
                    if map_id:
                        print("Inserted a default map into the database.")
                    else:
                        print("Failed to insert the default map.")
                    obstacle_type_id = insert_object_type(connection, created_by="system", name="obstacle")
                    if obstacle_type_id:
                        print("Inserted a obstacle type into the database.")
                    else:
                        print("Failed to insert the obstacle type.")
                    insert_map_objects_from_cache(
                        connection, created_by="system", map_id=map_id, object_type_id=obstacle_type_id, cached_map=factory_map)
                else:
                    print("Failed to retrieve the factory map.")
            else:
                print(f"Database already contains {map_count} map(s). No insertion needed.")
                # Get the first existing map ID for camera relationships
                existing_maps = fetch_all_maps()
                if existing_maps:
                    map_id = existing_maps[0]['id']

            # Delete all existing robot types first
            cleanup_old_robot_states(connection)
            delete_all_robots(connection)
            delete_all_robot_types(connection)

            # Initialize robot type based on .env configuration
            delete_all_robot_types(connection)
            robot_type_id = insert_robot_type(
                connection=connection,
                created_by="system",
                name="Pioneer 3-AT",
                capacity=100,  # kg capacity
                max_speed=2.5,  # m/s max speed
                safe_distance=int(0.1 * 100),  # SAFETY_DISTANCE from .env (0.1m = 10cm)
                min_battery_capacity=20,  # percentage
                robot_type_id="PIONEER_3AT",
                model="Pioneer 3-AT Mobile Robot",
                description="Autonomous mobile robot for warehouse operations with safety distance 0.1m and tolerance 0.1m",
                width=int(0.6 * 100),   # ROBOT_WIDTH from .env (0.6m = 60cm)
                height=40,  # cm
                length=int(0.6 * 100)   # ROBOT_LENGTH from .env (0.6m = 60cm)
            )

            if robot_type_id:
                print(f"Configuration: Width={0.6}m, Length={0.6}m, Safety Distance={0.1}m")

                # Delete all existing robots first

                # Initialize sample robots using the created robot type
                robots_config = [
                    {
                        "id": 0,
                        "name": "Pioneer-Robot-01",
                        "serial_number": "PION-3AT-001",
                        "ua_opc_endpoint": "opc.tcp://0.0.0.0:4840/freeopcua/server/"
                    },
                    {
                        "id": 1,
                        "name": "Pioneer-Robot-02",
                        "serial_number": "PION-3AT-002",
                        "ua_opc_endpoint": "opc.tcp://0.0.0.0:4841/freeopcua/server/"
                    },
                    {
                        "id": 2,
                        "name": "Pioneer-Robot-03",
                        "serial_number": "PION-3AT-003",
                        "ua_opc_endpoint": "opc.tcp://0.0.0.0:4842/freeopcua/server/"
                    },
                    {
                        "id": 3,
                        "name": "Pioneer-Robot-04",
                        "serial_number": "PION-3AT-004",
                        "ua_opc_endpoint": "opc.tcp://0.0.0.0:4843/freeopcua/server/"
                    }
                ]
                for i, robot_config in enumerate(robots_config):
                    robot_id = insert_robot(
                        connection=connection,
                        created_by="system",
                        name=robot_config["name"],
                        serial_number=robot_config["serial_number"],
                        ua_opc_endpoint=robot_config["ua_opc_endpoint"],
                        robot_type_id=robot_type_id,
                        capacity=100,  # kg capacity
                        max_speed=2.5,  # m/s max speed
                        safe_distance=int(0.1 * 100),  # SAFETY_DISTANCE from .env (10cm)
                        min_battery_capacity=20,  # percentage
                        firmware_version="1.0.0",
                        username="",
                        password="",
                        status="CONNECTED"
                    )
                    robot_map_ids[f"{robot_config['id']}"] = robot_id
                    if not robot_id:
                        print(f"Failed to insert robot '{robot_config['name']}'.")

                print(f"Successfully initialized {len(robots_config)} robots in the database.")

                # Initialize default robot states after robots are created
                print("Initializing default robot states...")
                robot_state_count = bulk_initialize_robot_states_for_all(connection)
                if robot_state_count and robot_state_count > 0:
                    print(f"Successfully initialized default states for {robot_state_count} robots.")
                else:
                    print("Failed to initialize robot states.")

            else:
                print("Failed to insert robot type.")

            # Initialize sample cameras after robots are set up
            print("Initializing sample cameras...")
            camera_ids = initialize_sample_cameras(connection)
            if camera_ids:
                print(f"Successfully initialized {len(camera_ids)} cameras in the database.")

                # Initialize map-camera relationships
                print("Creating map-camera relationships...")
                map_camera_ids = initialize_map_camera_relationships(connection, map_id, camera_ids)
                if map_camera_ids:
                    print(f"Successfully created {len(map_camera_ids)} map-camera relationships.")
                else:
                    print("Failed to create map-camera relationships.")
            else:
                print("Failed to initialize cameras.")

        except Exception as e:
            print(f"Error during database initialization: {e}")
        finally:
            connection.close()

def execute_robot_task_queries():
    """
    Hàm đơn giản để chạy lần lượt 3 câu SQL:
    1. INSERT vào bảng steps
    2. INSERT vào bảng runtime_tasks
    3. INSERT vào bảng runtime_tasks_steps

    Returns:
        dict: {
            'success': bool,
            'step_ids': list,  # IDs từ câu SQL 1
            'task_ids': list   # IDs từ câu SQL 2
        }
    """
    connection = create_postgres_connection()
    if not connection:
        print("❌ Không thể kết nối đến database")
        return {'success': False, 'step_ids': [], 'task_ids': []}

    try:
        cursor = connection.cursor()

        # Câu SQL thứ 1: INSERT vào bảng steps với RETURNING id
        sql_1 = """
	        INSERT INTO public.steps (created_at, created_by, updated_at, updated_by, "action", description, name, from_map_object_id, to_map_object_id,
	                                  speed_suggestion, to_map_object_x, to_map_object_y, to_map_object_theta, robot_type_id, accuracy_tolerance, deleted_at,
	                                  deleted_by)
	        VALUES ('2025-12-10 17:37:20.602567', 'robot_194_operator', '2025-12-10 17:37:20.602567', 'robot_194_operator', 'MOVE',
	                'Di chuyển robot 194 đến điểm (5.05, 5.39)', 'Move_to_Target_Task_Robot_194_20251210_173720', NULL, 2151, 50, 505, 539, 0, 55, 10, NULL, NULL),
	               ('2025-12-10 17:37:25.281722', 'robot_195_operator', '2025-12-10 17:37:25.281722', 'robot_195_operator', 'MOVE',
	                'Di chuyển robot 195 đến điểm (2.47, 12.97)', 'Move_to_Target_Task_Robot_195_20251210_173725', NULL, 2152, 50, 247, 1297, 0, 55, 10, NULL, NULL),
	               ('2025-12-10 17:37:29.399994', 'robot_196_operator', '2025-12-10 17:37:29.399994', 'robot_196_operator', 'MOVE',
	                'Di chuyển robot 196 đến điểm (24.990000000000002, 6.23)', 'Move_to_Target_Task_Robot_196_20251210_173729', NULL, 2153, 50, 2499, 623, 0, 55, 10,
	                NULL, NULL),
	               ('2025-12-10 17:37:33.363689', 'robot_197_operator', '2025-12-10 17:37:33.363689', 'robot_197_operator', 'MOVE',
	                'Di chuyển robot 197 đến điểm (23.89, 12.71)', 'Move_to_Target_Task_Robot_197_20251210_173733', NULL, 2154, 50, 2389, 1271, 0, 55, 10, NULL, NULL)
	        RETURNING id;
                """

        print("🔄 Thực hiện câu SQL 1: INSERT vào bảng steps...")
        cursor.execute(sql_1)
        step_ids = [row[0] for row in cursor.fetchall()]
        print(f"✅ Câu SQL 1 hoàn thành. Step IDs: {step_ids}")

        # Câu SQL thứ 2: INSERT vào bảng runtime_tasks với RETURNING id
        sql_2 = """
	        INSERT INTO public.runtime_tasks (created_at, created_by, updated_at, updated_by, payload_info, payload_weight, priority, task_name, task_type, robot_id,
	                                          task_template_id, current_runtime_task_step_id, planned_start_time, planned_end_time, actual_start_time,
	                                          actual_end_time, status, user_id, description, action_reason, assigned_robots, safety_notes, execution_mode, is_repeat,
	                                          repeat_interval, deleted_at, parent_id)
	        VALUES ('2025-12-10 17:37:20.613368', 'robot_194_operator', '2025-12-10 17:37:20.613368', 'robot_194_operator', NULL, NULL, NULL,
	                'Task_Robot_194_20251210_173720', NULL, 194, NULL, NULL, NULL, NULL, NULL, NULL, 'PENDING', NULL, NULL, NULL, NULL, NULL, 'manual', false, NULL, NULL,
	                NULL),
	               ('2025-12-10 17:37:25.29005', 'robot_195_operator', '2025-12-10 17:37:25.29005', 'robot_195_operator', NULL, NULL, NULL,
	                'Task_Robot_195_20251210_173725', NULL, 195, NULL, NULL, NULL, NULL, NULL, NULL, 'PENDING', NULL, NULL, NULL, NULL, NULL, 'manual', false, NULL, NULL,
	                NULL),
	               ('2025-12-10 17:37:29.411668', 'robot_196_operator', '2025-12-10 17:37:29.411668', 'robot_196_operator', NULL, NULL, NULL,
	                'Task_Robot_196_20251210_173729', NULL, 196, NULL, NULL, NULL, NULL, NULL, NULL, 'PENDING', NULL, NULL, NULL, NULL, NULL, 'manual', false, NULL, NULL,
	                NULL),
	               ('2025-12-10 17:37:33.374233', 'robot_197_operator', '2025-12-10 17:37:33.374233', 'robot_197_operator', NULL, NULL, NULL,
	                'Task_Robot_197_20251210_173733', NULL, 197, NULL, NULL, NULL, NULL, NULL, NULL, 'PENDING', NULL, NULL, NULL, NULL, NULL, 'manual', false, NULL, NULL,
	                NULL)
	        RETURNING id;
                """

        print("🔄 Thực hiện câu SQL 2: INSERT vào bảng runtime_tasks...")
        cursor.execute(sql_2)
        task_ids = [row[0] for row in cursor.fetchall()]
        print(f"✅ Câu SQL 2 hoàn thành. Task IDs: {task_ids}")

        # Câu SQL thứ 3: INSERT vào bảng runtime_tasks_steps sử dụng step_ids và task_ids đã lấy được
        # Tạo câu SQL động dựa trên các IDs đã lấy được
        if len(step_ids) == len(task_ids):
            sql_3_values = []
            for i, (step_id, task_id) in enumerate(zip(step_ids, task_ids)):
                sql_3_values.append(f"({step_id}, 0, {task_id}, 'PENDING', NULL, NULL, NULL, NULL, NULL, NULL, NULL)")

            sql_3 = f"""
            INSERT INTO public.runtime_tasks_steps (step_id, step_number, runtime_task_id, status, started_at, completed_at, duration, note, result_code, operator_id,
                                                    execution_mode)
            VALUES {', '.join(sql_3_values)};
            """

            print("🔄 Thực hiện câu SQL 3: INSERT vào bảng runtime_tasks_steps...")
            cursor.execute(sql_3)
            print(f"✅ Câu SQL 3 hoàn thành với {len(step_ids)} bản ghi")
        else:
            print(f"❌ Lỗi: Số lượng step_ids ({len(step_ids)}) và task_ids ({len(task_ids)}) không khớp!")
            connection.rollback()
            connection.close()
            return {'success': False, 'step_ids': step_ids, 'task_ids': task_ids}

        # Commit tất cả thay đổi
        connection.commit()
        cursor.close()
        connection.close()

        print("✅ Hoàn thành tất cả 3 câu SQL thành công!")
        return {'success': True, 'step_ids': step_ids, 'task_ids': task_ids}

    except Exception as e:
        print(f"❌ Lỗi khi thực hiện SQL: {e}")
        if connection:
            connection.rollback()
            connection.close()
        return {'success': False, 'step_ids': [], 'task_ids': []}

def insert_tasks():
    """
    Lấy danh sách ID của robot theo thứ tự tăng dần từ database.
    Returns: List of robot IDs sorted in ascending order, or empty list if error occurs.
    """
    try:
        connection = create_postgres_connection()
        if connection is None:
            print("Failed to establish database connection.")
            return []

        cursor = connection.cursor()

        # 1. Xóa runtime_tasks_steps trước (vì nó reference đến runtime_tasks và steps)
        cursor.execute("DELETE FROM public.runtime_tasks_steps;")
        deleted_steps = cursor.rowcount
        connection.commit()
        print(f"   ✅ Deleted {deleted_steps} records from runtime_tasks_steps")

        # 2. Xóa runtime_tasks
        cursor.execute("DELETE FROM public.runtime_tasks;")
        deleted_tasks = cursor.rowcount
        connection.commit()
        print(f"   ✅ Deleted {deleted_tasks} records from runtime_tasks")

        # 3. Xóa steps cuối cùng
        cursor.execute("DELETE FROM public.steps;")
        deleted_step_definitions = cursor.rowcount
        connection.commit()
        print(f"   ✅ Deleted {deleted_step_definitions} records from steps")

        execute_robot_task_queries()

        cursor.close()
        connection.close()

    except Exception as e:
        print(f"Error fetching robot IDs: {e}")
        return []
    finally:
        if 'connection' in locals() and connection:
            connection.close()

def get_robot_ids_ascending():
    """
    Lấy danh sách ID của robot theo thứ tự tăng dần từ database.
    Returns: List of robot IDs sorted in ascending order, or empty list if error occurs.
    """
    try:
        connection = create_postgres_connection()
        if connection is None:
            print("Failed to establish database connection.")
            return []

        cursor = connection.cursor()
        query = """
        SELECT id 
        FROM public.robots 
        WHERE deleted_at IS NULL 
        ORDER BY id ASC
        """
        cursor.execute(query)

        # Fetch all robot IDs and convert to a simple list
        robot_ids = [row[0] for row in cursor.fetchall()]

        cursor.close()
        connection.close()
        return robot_ids

    except Exception as e:
        print(f"Error fetching robot IDs: {e}")
        return []
    finally:
        if 'connection' in locals() and connection:
            connection.close()

# --- Main Logic ---
if __name__ == "__main__":
    import sys

    print("Webots Python:", sys.executable)
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
    insert_tasks()
    robot_ids = get_robot_ids_ascending()
    print(f"Robot IDs: {robot_ids}")
    robot_map_ids = {
        "0": robot_ids[0],
        # "1": robot_ids[1],
        # "2": robot_ids[2],
        # "3": robot_ids[3],
    }

    publisher_thread = None
    while root_supervisor.step(timestep) != -1:
        # compute current time at start of iteration
        now = time.time()

        rotate_near_manhole_objects(
            supervisor=root_supervisor,
            robot_def="Pioneer_3-AT",
            threshold=0.5,
            angle_range=(5, 10)
        )
        if not publisher_thread or not publisher_thread.is_alive():
            publisher_thread = threading.Thread(
                target=publish_robot_and_obstacles,
                args=(robot_map_ids, root_supervisor),
                daemon=True,
            )
            publisher_thread.start()
        loop_count += 1

    print("Webots simulation ended")

