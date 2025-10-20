from controller import Robot
from flask import Flask, request
import threading

from controllers.floor_mapper.floor_mapper import get_current_robot_pose

# Tạo robot object
robot = Robot()
timestep = int(robot.getBasicTimeStep())
gps = robot.getDevice('gps')
gps.enable(timestep)
# Lấy 2 motor
left_motor = robot.getDevice('left wheel motor')
right_motor = robot.getDevice('right wheel motor')
left_motor.setPosition(float('inf'))
right_motor.setPosition(float('inf'))


# Biến lưu tốc độ hiện tại
current_command = 'stop'

# Hàm điều khiển robot theo lệnh
def set_motion(command):
    if command == 'forward-slow':
        left_motor.setVelocity(5.0)
        right_motor.setVelocity(5.0)
    if command == 'forward-fast':
        left_motor.setVelocity(20.0)
        right_motor.setVelocity(20.0)
    elif command == 'left':
        left_motor.setVelocity(1.0)
        right_motor.setVelocity(3.0)
    elif command == 'right':
        left_motor.setVelocity(3.0)
        right_motor.setVelocity(1.0)
    elif command == 'back':
        left_motor.setVelocity(-3.0)
        right_motor.setVelocity(-3.0)
    elif command == 'stop':
        left_motor.setVelocity(0.0)
        right_motor.setVelocity(0.0)

# Flask app để nhận lệnh điều khiển
app = Flask(__name__)


@app.route('/control', methods=['POST'])
def control():
    global current_command
    data = request.json
    command = data.get('command', '')
    if command in ['forward-slow', 'forward-fast', 'left', 'right', 'back', 'stop']:
        current_command = command
        return {'status': 'ok', 'command': current_command}
    else:
        return {'status': 'error', 'message': 'Invalid command'}, 400

@app.route('/current-robot-pose', methods=['GET'])
def current_robot_pose():
    x, y = get_current_robot_pose()
    return {"status": "ok", "x": x, "y": y}


# Thread để chạy Flask server
def run_flask():
    app.run(host='0.0.0.0', port=6000, debug=False, use_reloader=False)

# Bắt đầu thread Flask
threading.Thread(target=run_flask, daemon=True).start()

print('Controller started with API server')

# Vòng lặp chính của robot
while robot.step(timestep) != -1:
    gps_values = gps.getValues()
    # print(f"[GPS] Position: x={gps_values[0]:.2f}, y={gps_values[1]:.2f}, z={gps_values[2]:.2f}")
    set_motion(current_command)

