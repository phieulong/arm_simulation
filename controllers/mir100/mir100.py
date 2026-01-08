# Copyright 1996-2024 Cyberbotics Ltd.
# Licensed under the Apache License, Version 2.0

import math
from controller import Robot, Keyboard

# ===== Constants (giữ nguyên như C) =====
WHEEL_RADIUS = 0.0625
SPEED_MAX = 0.95
SPEED_MIN = -0.3
ANGULAR_SPEED_MAX = 0.3
ANGULAR_SPEED_MIN = -0.3
SPEED_INCREMENT = 0.05
ANGULAR_SPEED_INCREMENT = 0.05
DISTANCE_TO_CENTER = 0.2226

# ===== Robot init =====
robot = Robot()
time_step = int(robot.getBasicTimeStep())

# ===== Motors =====
motor_front_left_joint = robot.getDevice("front_left_wheel_joint")
motor_front_right_joint = robot.getDevice("front_right_wheel_joint")
motor_back_left_joint = robot.getDevice("back_left_wheel_joint")
motor_back_right_joint = robot.getDevice("back_right_wheel_joint")

motor_front_left_joint.setPosition(float("inf"))
motor_front_right_joint.setPosition(float("inf"))
motor_back_left_joint.setPosition(float("inf"))
motor_back_right_joint.setPosition(float("inf"))

motor_front_left_joint.setVelocity(0.0)
motor_front_right_joint.setVelocity(0.0)
motor_back_left_joint.setVelocity(0.0)
motor_back_right_joint.setVelocity(0.0)

motor_front_left = robot.getDevice("front_left_wheel")
motor_front_right = robot.getDevice("front_right_wheel")
motor_back_left = robot.getDevice("back_left_wheel")
motor_back_right = robot.getDevice("back_right_wheel")

motor_front_left.setPosition(float("inf"))
motor_front_right.setPosition(float("inf"))
motor_back_left.setPosition(float("inf"))
motor_back_right.setPosition(float("inf"))

motor_front_left.setVelocity(0.0)
motor_front_right.setVelocity(0.0)
motor_back_left.setVelocity(0.0)
motor_back_right.setVelocity(0.0)

# ===== Sensors =====
depth_camera = robot.getDevice("depth_camera")
depth_camera.enable(time_step)

keyboard = Keyboard()
keyboard.enable(time_step)

print(
    "To move the Mir100 with your keyboard, click inside the simulation window and press:\n"
    " vx   : ↑/↓\n"
    " ω    : ←/→\n"
    " Reset: Space bar\n"
)

# ===== Control variables =====
target_speed = 0.0   # [m/s]
target_omega = 0.0   # [rad/s]

# ===== Main loop =====
while robot.step(time_step) != -1:
    key = keyboard.getKey()
    is_key_valid = True

    if key == Keyboard.UP:
        target_speed = 12

    elif key == Keyboard.DOWN:
        target_speed = -6

    elif key == Keyboard.LEFT:
        target_omega = -1

    elif key == Keyboard.RIGHT:
        target_omega = 1

    elif key == ord(' '):
        target_speed = 0.0
        target_omega = 0.0

    else:
        is_key_valid = False

    if is_key_valid:
        print(f"vx:{target_speed:.2f}[m/s] ω:{target_omega:.2f}[rad/s]")

        # ===== Wheel speed computation =====
        # left_speed = (target_speed - target_omega * DISTANCE_TO_CENTER) / WHEEL_RADIUS
        # right_speed = (target_speed + target_omega * DISTANCE_TO_CENTER) / WHEEL_RADIUS

        motor_front_left_joint.setVelocity(target_omega)
        motor_front_right_joint.setVelocity(target_omega)
        motor_back_left_joint.setVelocity(target_omega)
        motor_back_right_joint.setVelocity(target_omega)

        motor_back_left.setVelocity(target_speed)
        motor_back_right.setVelocity(target_speed)
        motor_front_left.setVelocity(target_speed)
        motor_front_right.setVelocity(target_speed)


