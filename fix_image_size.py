from PIL import Image
import numpy as np

# ===== Thông số =====
dpi = 300

# A0 size (mm)
a0_width_mm = 841
a0_height_mm = 1189

# Chessboard size (mm)
board_mm = 800  # 80 cm

# ===== Chuyển mm -> pixel =====
mm_to_inch = 1 / 25.4
a0_w_px = int(a0_width_mm * mm_to_inch * dpi)
a0_h_px = int(a0_height_mm * mm_to_inch * dpi)
board_px = int(board_mm * mm_to_inch * dpi)

# ===== Load ảnh bàn cờ đã có =====
board = Image.open("chessboard_8x8_10cm.png").convert("L")

# (Optional) resize để chắc chắn đúng 800mm
board = board.resize((board_px, board_px), Image.NEAREST)

# ===== Tạo canvas A0 trắng =====
canvas = Image.new("L", (a0_w_px, a0_h_px), 255)

# ===== Căn giữa =====
x = (a0_w_px - board_px) // 2
y = (a0_h_px - board_px) // 2

canvas.paste(board, (x, y))

# ===== Lưu PDF A0 =====
canvas.save("chessboard_80cm_on_A0.pdf", dpi=(dpi, dpi))

print("Saved chessboard_80cm_on_A0.pdf")
