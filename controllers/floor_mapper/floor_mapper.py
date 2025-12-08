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
from psycopg2 import sql
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
    host='192.168.0.71',
    port=26379,
    db=0
)

redis_client = redis.Redis(host='192.168.0.71', port=26379, db=0, connection_pool=pool)

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
        robot_1 = fetch_current_robot_api(1)
        robot_1['object_id'] = map_robot_ids["1"]
        robot_2 = fetch_current_robot_api(2)
        robot_2['object_id'] = map_robot_ids["2"]
        robot_3  = fetch_current_robot_api(3)
        robot_3['object_id'] = map_robot_ids["3"]

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
            settings = json.dumps({"objectType": "obstacle"})
        insert_query = """
        INSERT INTO public.object_types (created_at, created_by, updated_at, updated_by, name, settings)
        VALUES (NOW(), %s, NOW(), %s, %s, %s) RETURNING id;
        """
        cursor.execute(insert_query, (created_by, created_by, name, json.dumps(settings) if settings else None))
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

    # Kết nối tới cơ sở dữ liệu PostgreSQL
    connection = create_postgres_connection()
    robot_map_ids = {}

    if connection:
        try:
            cursor = connection.cursor()

            delete_all_allowed_zone(connection)
            delete_all_map_objects(connection)
            delete_all_object_types(connection)
            delete_all_maps(connection)  # Xóa tất cả bản ghi hiện có
            maps = fetch_all_maps()
            map_count = len(maps)

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

            # Delete all existing robot types first

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
                        status="active"
                    )
                    robot_map_ids[f"{robot_config['id']}"] = robot_id
                    if not robot_id:
                        print(f"Failed to insert robot '{robot_config['name']}'.")

                print(f"Successfully initialized {len(robots_config)} robots in the database.")
            else:
                print("Failed to insert robot type.")

        except Exception as e:
            print(f"Error during database initialization: {e}")
        finally:
            connection.close()

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
